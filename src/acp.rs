use acp::Agent;
use agent_client_protocol as acp;
use anyhow::Result;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::session_log::{SessionLog, TranscriptDirection};
use crate::types::AgentEvent;

pub type SharedStderrTail = Arc<Mutex<VecDeque<String>>>;
const STDERR_TAIL_MAX_LINES: usize = 50;

pub struct SessionBootstrap {
    pub session_id: acp::SessionId,
    pub modes: Option<acp::SessionModeState>,
    pub config_options: Vec<acp::SessionConfigOption>,
}

/// Our ACP Client implementation that forwards agent notifications as AgentEvents.
pub struct TelegramClient {
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    session_log: Arc<SessionLog>,
    /// When true, session_notification is a no-op (suppresses replay during load).
    pub session_loading_in_progress: Arc<AtomicBool>,
}

impl TelegramClient {
    pub fn new(
        event_tx: mpsc::UnboundedSender<AgentEvent>,
        session_log: Arc<SessionLog>,
        session_loading_in_progress: Arc<AtomicBool>,
    ) -> Self {
        Self {
            event_tx,
            session_log,
            session_loading_in_progress,
        }
    }

    fn send_event(&self, event: AgentEvent) {
        let _ = self.event_tx.send(event);
    }
}

#[async_trait::async_trait(?Send)]
impl acp::Client for TelegramClient {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        // Auto-approve: pick the first "allow" option, or first option if none are allow
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
            .map(|o| o.option_id.clone())
            .unwrap_or_else(|| acp::PermissionOptionId::new("allow_always"));

        Ok(acp::RequestPermissionResponse::new(
            acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(option_id)),
        ))
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        if self.session_loading_in_progress.load(Ordering::Relaxed) {
            return Ok(());
        }

        let session_id = args.session_id;
        let update = args.update;
        if let Err(err) = self.session_log.log_acp_payload(
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
            | acp::SessionUpdate::UsageUpdate(_) => self.send_event(AgentEvent::Update(update)),
            _ => {
                // Ignore other notification types (UserMessageChunk, mode/config updates, etc.)
            }
        }
        Ok(())
    }
}

/// Spawn an ACP agent subprocess and return the connection + child handle.
/// Must be called within a tokio LocalSet.
pub fn spawn_agent(
    agent_cmd: &str,
    project_path: &Path,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    session_log: Arc<SessionLog>,
    session_loading_in_progress: Arc<AtomicBool>,
) -> Result<(
    acp::ClientSideConnection,
    tokio::process::Child,
    SharedStderrTail,
    impl std::future::Future<Output = acp::Result<()>>,
)> {
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

    let client = TelegramClient::new(event_tx, session_log, session_loading_in_progress);

    let (conn, handle_io) = acp::ClientSideConnection::new(client, stdin, stdout, |fut| {
        tokio::task::spawn_local(fut);
    });

    Ok((conn, child, stderr_tail, handle_io))
}

fn spawn_stderr_drain(
    stderr: tokio::process::ChildStderr,
    session_log: Arc<SessionLog>,
) -> SharedStderrTail {
    let stderr_tail: SharedStderrTail = Arc::new(Mutex::new(VecDeque::new()));
    let stderr_tail_for_task = Arc::clone(&stderr_tail);

    tokio::task::spawn_local(async move {
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

/// Initialize an ACP connection: call initialize + new_session, return session_id.
pub async fn init_session(
    conn: &acp::ClientSideConnection,
    project_path: &Path,
    mcp_servers: Vec<acp::McpServer>,
    session_log: &SessionLog,
) -> Result<SessionBootstrap> {
    let init_request = acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
            acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION"))
                .title("Telegram ACP"),
        );
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "initialize", "params": &init_request }),
    )?;
    let init_response = conn.initialize(init_request).await?;
    session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({ "method": "initialize", "result": &init_response }),
    )?;

    let new_session_request = acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
    )?;
    let session_resp = conn.new_session(new_session_request).await?;
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

/// Resume a previous ACP session using load_session if supported, otherwise fall back to new_session.
pub async fn resume_session(
    conn: &acp::ClientSideConnection,
    project_path: &Path,
    old_acp_session_id: String,
    mcp_servers: Vec<acp::McpServer>,
    session_log: &SessionLog,
) -> Result<SessionBootstrap> {
    let init_request = acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
        acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION"))
            .title("Telegram ACP"),
    );
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "initialize", "params": &init_request }),
    )?;
    let init_resp = conn.initialize(init_request).await?;
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
        match conn.load_session(load_request).await {
            Ok(load_resp) => {
                session_log.log_acp_payload(
                    TranscriptDirection::FromAgent,
                    &serde_json::json!({ "method": "load_session", "result": &load_resp }),
                )?;
                // load_session succeeded — reuse the same session ID
                Ok(SessionBootstrap {
                    session_id,
                    modes: load_resp.modes,
                    config_options: load_resp.config_options.unwrap_or_default(),
                })
            }
            Err(e) => {
                tracing::warn!(
                    "load_session failed for {}, falling back to new_session: {e}",
                    session_id
                );
                let new_session_request =
                    acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
                session_log.log_acp_payload(
                    TranscriptDirection::ToAgent,
                    &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
                )?;
                let session_resp = conn.new_session(new_session_request).await?;
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
        let new_session_request = acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
        session_log.log_acp_payload(
            TranscriptDirection::ToAgent,
            &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
        )?;
        let session_resp = conn.new_session(new_session_request).await?;
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
