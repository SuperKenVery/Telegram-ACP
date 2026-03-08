use agent_client_protocol as acp;
use acp::Agent;
use anyhow::Result;
use std::path::Path;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::types::AgentEvent;

/// Our ACP Client implementation that forwards agent notifications as AgentEvents.
pub struct TelegramClient {
    event_tx: mpsc::UnboundedSender<AgentEvent>,
}

impl TelegramClient {
    pub fn new(event_tx: mpsc::UnboundedSender<AgentEvent>) -> Self {
        Self { event_tx }
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
            .find(|o| matches!(o.kind, acp::PermissionOptionKind::AllowAlways | acp::PermissionOptionKind::AllowOnce))
            .or(args.options.first())
            .map(|o| o.option_id.clone())
            .unwrap_or_else(|| acp::PermissionOptionId::new("allow"));

        Ok(acp::RequestPermissionResponse::new(
            acp::RequestPermissionOutcome::Selected(
                acp::SelectedPermissionOutcome::new(option_id),
            ),
        ))
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        match args.update {
            acp::SessionUpdate::AgentMessageChunk(chunk) => {
                let text = extract_text(&chunk.content);
                if !text.is_empty() {
                    self.send_event(AgentEvent::TextMessage(text));
                }
            }
            acp::SessionUpdate::ToolCall(tool_call) => {
                self.send_event(AgentEvent::ToolCall {
                    id: tool_call.tool_call_id.to_string(),
                    name: tool_call.title.clone(),
                });
            }
            acp::SessionUpdate::ToolCallUpdate(update) => {
                let output = update.fields.content.as_ref().and_then(|contents| {
                    let texts: Vec<String> = contents
                        .iter()
                        .filter_map(|c| match c {
                            acp::ToolCallContent::Content(content) => {
                                Some(extract_text(&content.content))
                            }
                            _ => None,
                        })
                        .collect();
                    if texts.is_empty() {
                        None
                    } else {
                        Some(texts.join(""))
                    }
                });
                let title = update.fields.title.unwrap_or_default();
                self.send_event(AgentEvent::ToolCallUpdate {
                    id: update.tool_call_id.to_string(),
                    name: title,
                    output,
                });
            }
            _ => {
                // Ignore other notification types (Plan, UserMessageChunk, etc.)
            }
        }
        Ok(())
    }
}

fn extract_text(content: &acp::ContentBlock) -> String {
    match content {
        acp::ContentBlock::Text(tc) => tc.text.clone(),
        _ => String::new(),
    }
}

/// Spawn an ACP agent subprocess and return the connection + child handle.
/// Must be called within a tokio LocalSet.
pub fn spawn_agent(
    agent_cmd: &str,
    project_path: &Path,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
) -> Result<(acp::ClientSideConnection, tokio::process::Child, impl std::future::Future<Output = acp::Result<()>>)> {
    let parts: Vec<&str> = agent_cmd.split_whitespace().collect();
    let (program, args) = parts.split_first().unwrap_or((&"claude-agent-acp", &[]));

    let mut child = Command::new(program)
        .args(args)
        .current_dir(project_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;

    let stdin = child.stdin.take().unwrap().compat_write();
    let stdout = child.stdout.take().unwrap().compat();

    let client = TelegramClient::new(event_tx);

    let (conn, handle_io) = acp::ClientSideConnection::new(client, stdin, stdout, |fut| {
        tokio::task::spawn_local(fut);
    });

    Ok((conn, child, handle_io))
}

/// Initialize an ACP connection: call initialize + new_session, return session_id.
pub async fn init_session(
    conn: &acp::ClientSideConnection,
    project_path: &Path,
) -> Result<acp::SessionId> {
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

    Ok(session_resp.session_id)
}
