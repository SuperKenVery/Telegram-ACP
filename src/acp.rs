use acp::Agent;
use agent_client_protocol as acp;
use anyhow::Result;
use serde_json::Value;
use similar::TextDiff;
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

        match args.update {
            acp::SessionUpdate::AgentMessageChunk(chunk) => {
                let text = extract_text(&chunk.content);
                if !text.is_empty() {
                    self.send_event(AgentEvent::TextMessage(text));
                }
            }
            acp::SessionUpdate::ToolCall(tool_call) => {
                let details = extract_tool_details(
                    &tool_call.title,
                    tool_call.raw_input.as_ref(),
                    &tool_call.content,
                );
                self.send_event(AgentEvent::ToolCall {
                    id: tool_call.tool_call_id.to_string(),
                    name: tool_call.title.clone(),
                    details,
                });
            }
            acp::SessionUpdate::ToolCallUpdate(update) => {
                let fields = update.fields;
                let output = fields
                    .content
                    .as_ref()
                    .and_then(|contents| extract_tool_output(contents));
                let details = extract_tool_details(
                    fields.title.as_deref().unwrap_or(""),
                    fields.raw_input.as_ref(),
                    fields.content.as_deref().unwrap_or(&[]),
                );
                let title = fields.title.unwrap_or_default();
                self.send_event(AgentEvent::ToolCallUpdate {
                    id: update.tool_call_id.to_string(),
                    name: title,
                    output,
                    details,
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

fn extract_tool_output(contents: &[acp::ToolCallContent]) -> Option<String> {
    let mut parts = Vec::new();
    for content in contents {
        match content {
            acp::ToolCallContent::Content(content) => {
                let text = extract_text(&content.content);
                if !text.trim().is_empty() {
                    parts.push(text);
                }
            }
            acp::ToolCallContent::Diff(diff) => {
                parts.push(format_unified_diff(
                    Some(diff.path.display().to_string()),
                    diff.old_text.as_deref(),
                    &diff.new_text,
                ));
            }
            _ => {}
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

fn extract_tool_details(
    title: &str,
    raw_input: Option<&Value>,
    contents: &[acp::ToolCallContent],
) -> Option<String> {
    if let Some(diff_text) = extract_diff_details(contents) {
        return Some(diff_text);
    }

    if !looks_like_edit_tool(title) {
        return None;
    }

    let input = raw_input?;
    let new_text = find_string_value(
        input,
        &[
            "newText",
            "new_text",
            "newString",
            "new_string",
            "content",
            "patch",
        ],
    )?;
    let old_text = find_string_value(input, &["oldText", "old_text", "oldString", "old_string"]);
    let path = find_string_value(
        input,
        &[
            "path",
            "filePath",
            "file_path",
            "targetFile",
            "target_file",
            "filename",
        ],
    );

    Some(format_unified_diff(path, old_text.as_deref(), &new_text))
}

fn extract_diff_details(contents: &[acp::ToolCallContent]) -> Option<String> {
    let diffs: Vec<String> = contents
        .iter()
        .filter_map(|content| match content {
            acp::ToolCallContent::Diff(diff) => Some(format_unified_diff(
                Some(diff.path.display().to_string()),
                diff.old_text.as_deref(),
                &diff.new_text,
            )),
            _ => None,
        })
        .collect();

    if diffs.is_empty() {
        None
    } else {
        Some(diffs.join("\n\n"))
    }
}

fn format_unified_diff(path: Option<String>, old_text: Option<&str>, new_text: &str) -> String {
    let old = old_text.unwrap_or("");
    let path = path.unwrap_or_else(|| "file".to_string());
    let old_header = format!("a/{path}");
    let new_header = format!("b/{path}");
    let unified = TextDiff::from_lines(old, new_text)
        .unified_diff()
        .context_radius(2)
        .header(&old_header, &new_header)
        .to_string();

    if unified.trim().is_empty() {
        format!("--- {old_header}\n+++ {new_header}\n(no changes)")
    } else {
        unified
    }
}

fn looks_like_edit_tool(title: &str) -> bool {
    let lower = title.to_ascii_lowercase();
    lower.contains("edit")
        || lower.contains("write")
        || lower.contains("replace")
        || lower.contains("apply_patch")
}

fn find_string_value(value: &Value, keys: &[&str]) -> Option<String> {
    let targets: Vec<String> = keys.iter().map(|k| normalize_key(k)).collect();
    find_string_value_inner(value, &targets)
}

fn find_string_value_inner(value: &Value, keys: &[String]) -> Option<String> {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if keys.iter().any(|target| *target == normalize_key(k)) {
                    if let Value::String(s) = v {
                        if !s.trim().is_empty() {
                            return Some(s.clone());
                        }
                    }
                }
            }

            for v in map.values() {
                if let Some(found) = find_string_value_inner(v, keys) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => {
            for item in items {
                if let Some(found) = find_string_value_inner(item, keys) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

fn normalize_key(key: &str) -> String {
    key.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| ch.to_lowercase())
        .collect()
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
