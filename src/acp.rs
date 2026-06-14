use agent_client_protocol as acp;
use anyhow::Result;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::session_log::{SessionLog, TranscriptDirection};
use crate::types::AgentEvent;

pub type SharedStderrTail = Arc<Mutex<VecDeque<String>>>;
pub type AgentConnection = sacp::ConnectionTo<sacp::Agent>;

const STDERR_TAIL_MAX_LINES: usize = 50;

pub struct SessionBootstrap {
    pub session_id: acp::SessionId,
    pub modes: Option<acp::SessionModeState>,
    pub config_options: Vec<acp::SessionConfigOption>,
}

pub async fn spawn_agent(
    agent_cmd: &str,
    project_path: &Path,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    session_log: Arc<SessionLog>,
    session_loading_in_progress: Arc<AtomicBool>,
) -> Result<(AgentConnection, tokio::process::Child, SharedStderrTail)> {
    // Execute via shell so user-configured commands support shell expansion
    // (e.g. `~`, quoted args, and env interpolation) consistently with manual runs.
    let mut child = Command::new("bash")
        .arg("-lc")
        .arg(agent_cmd)
        .current_dir(project_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stdin = child.stdin.take().unwrap().compat_write();
    let stdout = child.stdout.take().unwrap().compat();
    let stderr = child.stderr.take().unwrap();

    let stderr_tail = spawn_stderr_drain(stderr, session_log.clone());
    let transport = sacp::ByteStreams::new(stdin, stdout);
    let (conn_tx, conn_rx) = oneshot::channel();
    let io_event_tx = event_tx.clone();
    let notification_session_log = session_log.clone();
    let notification_loading = session_loading_in_progress.clone();

    tokio::spawn(async move {
        let connect_result = sacp::Client
            .builder()
            .name("telegram-acp")
            .on_receive_notification(
                async move |args: acp::SessionNotification, _connection| {
                    handle_session_notification(
                        args,
                        &notification_session_log,
                        &event_tx,
                        &notification_loading,
                    );
                    Ok(())
                },
                sacp::on_receive_notification!(),
            )
            .on_receive_request(
                async move |args: acp::RequestPermissionRequest, responder, _connection| {
                    responder.respond(auto_approve_permission(args))
                },
                sacp::on_receive_request!(),
            )
            .connect_with(transport, |connection: AgentConnection| async move {
                let _ = conn_tx.send(connection);
                std::future::pending::<Result<(), sacp::Error>>().await
            })
            .await;

        if let Err(err) = connect_result {
            tracing::error!("ACP IO error: {err}");
            let _ = io_event_tx.send(AgentEvent::Error(format!("Agent connection error: {err}")));
        }
    });

    let conn = conn_rx
        .await
        .map_err(|_| anyhow::anyhow!("ACP connection task exited before startup"))?;
    Ok((conn, child, stderr_tail))
}

fn auto_approve_permission(args: acp::RequestPermissionRequest) -> acp::RequestPermissionResponse {
    let option_id = args
        .options
        .iter()
        .find(|o| {
            matches!(
                o.kind,
                acp::PermissionOptionKind::AllowAlways | acp::PermissionOptionKind::AllowOnce
            )
        })
        .or(args.options.first())
        .map(|o| o.option_id.clone());

    match option_id {
        Some(option_id) => acp::RequestPermissionResponse::new(
            acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(option_id)),
        ),
        None => acp::RequestPermissionResponse::new(acp::RequestPermissionOutcome::Cancelled),
    }
}

fn handle_session_notification(
    args: acp::SessionNotification,
    session_log: &SessionLog,
    event_tx: &mpsc::UnboundedSender<AgentEvent>,
    session_loading_in_progress: &AtomicBool,
) {
    if session_loading_in_progress.load(Ordering::Relaxed) {
        return;
    }

    let session_id = args.session_id;
    let update = args.update;
    if let Err(err) = session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({
            "type": "session_notification",
            "session_id": &session_id,
            "update": &update,
        }),
    ) {
        tracing::warn!("Failed to record ACP notification: {err}");
    }

    match update {
        acp::SessionUpdate::AgentMessageChunk(_)
        | acp::SessionUpdate::AgentThoughtChunk(_)
        | acp::SessionUpdate::ToolCall(_)
        | acp::SessionUpdate::ToolCallUpdate(_)
        | acp::SessionUpdate::Plan(_)
        | acp::SessionUpdate::AvailableCommandsUpdate(_)
        | acp::SessionUpdate::UsageUpdate(_) => {
            let _ = event_tx.send(AgentEvent::Update(update));
        }
        _ => {
            // Ignore other notification types (UserMessageChunk, mode/config updates, etc.)
        }
    }
}

fn spawn_stderr_drain(
    stderr: tokio::process::ChildStderr,
    session_log: Arc<SessionLog>,
) -> SharedStderrTail {
    let stderr_tail: SharedStderrTail = Arc::new(Mutex::new(VecDeque::new()));
    let stderr_tail_for_task = Arc::clone(&stderr_tail);

    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if let Err(err) = session_log.write_agent_stderr_line(&line) {
                        tracing::warn!("Failed writing agent stderr line: {err}");
                    }
                    push_stderr_line(&stderr_tail_for_task, line);
                }
                Ok(None) => break,
                Err(err) => {
                    let message = format!("Failed reading agent stderr: {err}");
                    if let Err(write_err) = session_log.write_agent_stderr_line(&message) {
                        tracing::warn!("Failed writing agent stderr error: {write_err}");
                    }
                    break;
                }
            }
        }
    });

    stderr_tail
}

fn push_stderr_line(stderr_tail: &SharedStderrTail, line: String) {
    let mut tail = match stderr_tail.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if tail.len() >= STDERR_TAIL_MAX_LINES {
        tail.pop_front();
    }
    tail.push_back(line);
}

pub fn format_stderr_tail(stderr_tail: &SharedStderrTail) -> String {
    let lines: Vec<String> = match stderr_tail.lock() {
        Ok(guard) => guard.iter().cloned().collect(),
        Err(poisoned) => poisoned.into_inner().iter().cloned().collect(),
    };
    if lines.is_empty() {
        String::new()
    } else {
        format!(
            "\nAgent stderr (last {} lines):\n{}",
            lines.len(),
            lines.join("\n")
        )
    }
}

pub async fn init_session(
    conn: &AgentConnection,
    project_path: &Path,
    mcp_servers: Vec<acp::McpServer>,
    session_log: &SessionLog,
) -> Result<SessionBootstrap> {
    let init_request = acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
        acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION")).title("Telegram ACP"),
    );
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "initialize", "params": &init_request }),
    )?;
    let init_response = conn.send_request(init_request).block_task().await?;
    session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({ "method": "initialize", "result": &init_response }),
    )?;

    let new_session_request = acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
    )?;
    let session_resp = conn.send_request(new_session_request).block_task().await?;
    session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({ "method": "new_session", "result": &session_resp }),
    )?;

    Ok(SessionBootstrap {
        session_id: session_resp.session_id,
        modes: session_resp.modes,
        config_options: session_resp.config_options.unwrap_or_default(),
    })
}

pub async fn resume_session(
    conn: &AgentConnection,
    project_path: &Path,
    old_acp_session_id: String,
    mcp_servers: Vec<acp::McpServer>,
    session_log: &SessionLog,
) -> Result<SessionBootstrap> {
    let init_request = acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
        acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION")).title("Telegram ACP"),
    );
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "initialize", "params": &init_request }),
    )?;
    let init_resp = conn.send_request(init_request).block_task().await?;
    session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({ "method": "initialize", "result": &init_resp }),
    )?;

    if init_resp.agent_capabilities.load_session {
        tracing::info!(
            "Agent supports load_session, resuming session {}",
            old_acp_session_id
        );
        let session_id = acp::SessionId::new(old_acp_session_id.clone());
        let load_request = acp::LoadSessionRequest::new(old_acp_session_id, project_path)
            .mcp_servers(mcp_servers.clone());
        session_log.log_acp_payload(
            TranscriptDirection::ToAgent,
            &serde_json::json!({ "method": "load_session", "params": &load_request }),
        )?;
        match conn.send_request(load_request).block_task().await {
            Ok(load_resp) => {
                session_log.log_acp_payload(
                    TranscriptDirection::FromAgent,
                    &serde_json::json!({ "method": "load_session", "result": &load_resp }),
                )?;
                Ok(SessionBootstrap {
                    session_id,
                    modes: load_resp.modes,
                    config_options: load_resp.config_options.unwrap_or_default(),
                })
            }
            Err(err) => {
                tracing::warn!(
                    "load_session failed for {}, falling back to new_session: {err}",
                    session_id
                );
                let new_session_request =
                    acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
                session_log.log_acp_payload(
                    TranscriptDirection::ToAgent,
                    &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
                )?;
                let session_resp = conn.send_request(new_session_request).block_task().await?;
                session_log.log_acp_payload(
                    TranscriptDirection::FromAgent,
                    &serde_json::json!({ "method": "new_session", "result": &session_resp }),
                )?;
                Ok(SessionBootstrap {
                    session_id: session_resp.session_id,
                    modes: session_resp.modes,
                    config_options: session_resp.config_options.unwrap_or_default(),
                })
            }
        }
    } else {
        tracing::info!("Agent does not support load_session, creating new session");
        let new_session_request =
            acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
        session_log.log_acp_payload(
            TranscriptDirection::ToAgent,
            &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
        )?;
        let session_resp = conn.send_request(new_session_request).block_task().await?;
        session_log.log_acp_payload(
            TranscriptDirection::FromAgent,
            &serde_json::json!({ "method": "new_session", "result": &session_resp }),
        )?;
        Ok(SessionBootstrap {
            session_id: session_resp.session_id,
            modes: session_resp.modes,
            config_options: session_resp.config_options.unwrap_or_default(),
        })
    }
}
