use acp::Agent;
use agent_client_protocol as acp;
use anyhow::Result;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::types::AgentEvent;

pub struct SessionBootstrap {
    pub session_id: acp::SessionId,
    pub modes: Option<acp::SessionModeState>,
    pub config_options: Vec<acp::SessionConfigOption>,
}

/// Our ACP Client implementation that forwards agent notifications as AgentEvents.
pub struct TelegramClient {
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    /// When true, session_notification is a no-op (suppresses replay during load).
    pub is_loading: Arc<AtomicBool>,
}

impl TelegramClient {
    pub fn new(event_tx: mpsc::UnboundedSender<AgentEvent>, is_loading: Arc<AtomicBool>) -> Self {
        Self {
            event_tx,
            is_loading,
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
            .unwrap_or_else(|| acp::PermissionOptionId::new("allow"));

        Ok(acp::RequestPermissionResponse::new(
            acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(option_id)),
        ))
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        if self.is_loading.load(Ordering::Relaxed) {
            return Ok(());
        }

        let update = args.update;
        match update {
            acp::SessionUpdate::AgentMessageChunk(_)
            | acp::SessionUpdate::AgentThoughtChunk(_)
            | acp::SessionUpdate::ToolCall(_)
            | acp::SessionUpdate::ToolCallUpdate(_)
            | acp::SessionUpdate::Plan(_)
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
    is_loading: Arc<AtomicBool>,
) -> Result<(
    acp::ClientSideConnection,
    tokio::process::Child,
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
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;

    let stdin = child.stdin.take().unwrap().compat_write();
    let stdout = child.stdout.take().unwrap().compat();

    let client = TelegramClient::new(event_tx, is_loading);

    let (conn, handle_io) = acp::ClientSideConnection::new(client, stdin, stdout, |fut| {
        tokio::task::spawn_local(fut);
    });

    Ok((conn, child, handle_io))
}

/// Initialize an ACP connection: call initialize + new_session, return session_id.
pub async fn init_session(
    conn: &acp::ClientSideConnection,
    project_path: &Path,
) -> Result<SessionBootstrap> {
    conn.initialize(
        acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
            acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION"))
                .title("Telegram ACP"),
        ),
    )
    .await?;

    let session_resp = conn
        .new_session(acp::NewSessionRequest::new(project_path))
        .await?;

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
) -> Result<SessionBootstrap> {
    let init_resp = conn
        .initialize(
            acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
                acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION"))
                    .title("Telegram ACP"),
            ),
        )
        .await?;

    if init_resp.agent_capabilities.load_session {
        tracing::info!(
            "Agent supports load_session, resuming session {}",
            old_acp_session_id
        );
        let session_id = acp::SessionId::new(old_acp_session_id.clone());
        match conn
            .load_session(acp::LoadSessionRequest::new(
                old_acp_session_id,
                project_path,
            ))
            .await
        {
            Ok(load_resp) => {
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
                let session_resp = conn
                    .new_session(acp::NewSessionRequest::new(project_path))
                    .await?;
                Ok(SessionBootstrap {
                    session_id: session_resp.session_id,
                    modes: session_resp.modes,
                    config_options: session_resp.config_options.unwrap_or_default(),
                })
            }
        }
    } else {
        tracing::info!("Agent does not support load_session, creating new session");
        let session_resp = conn
            .new_session(acp::NewSessionRequest::new(project_path))
            .await?;
        Ok(SessionBootstrap {
            session_id: session_resp.session_id,
            modes: session_resp.modes,
            config_options: session_resp.config_options.unwrap_or_default(),
        })
    }
}
