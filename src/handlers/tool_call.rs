use std::collections::HashMap;
use std::sync::Arc;

use agent_client_protocol as acp;
use similar::TextDiff;
use teloxide::types::MessageId;
use tokio::sync::Mutex;

use super::{EventContext, EventHandler};
use crate::formatting;
use crate::types::AgentEvent;

struct ToolCallMessageState {
    last_sent: Option<(MessageId, String)>,
    name: String,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    display_mode: crate::types::VerboseMode,
    details: Option<String>,
}

pub struct ToolCallHandler {
    messages: HashMap<String, ToolCallMessageState>,
    verbose: Arc<Mutex<crate::types::VerboseMode>>,
}

impl ToolCallHandler {
    pub fn new(verbose: Arc<Mutex<crate::types::VerboseMode>>) -> Self {
        Self {
            messages: HashMap::new(),
            verbose,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl EventHandler for ToolCallHandler {
    async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool {
        match event {
            AgentEvent::Update(acp::SessionUpdate::ToolCall(tool_call)) => {
                self.handle_tool_call(tool_call, ctx).await;
                true
            }
            AgentEvent::Update(acp::SessionUpdate::ToolCallUpdate(update)) => {
                self.handle_tool_call_update(update, ctx).await;
                true
            }
            _ => false,
        }
    }

    async fn reset(&mut self, _ctx: &mut EventContext) {
        self.messages.clear();
    }
}

impl ToolCallHandler {
    async fn handle_tool_call(&mut self, tool_call: &acp::ToolCall, ctx: &mut EventContext) {
        let id = tool_call.tool_call_id.to_string();
        let name = tool_call.title.clone();
        let kind = tool_call.kind;
        let status = tool_call.status;
        let details = extract_tool_diff(&tool_call.content);
        let verbose = *self.verbose.lock().await;
        let mut state = ToolCallMessageState {
            last_sent: None,
            name,
            kind,
            status,
            display_mode: verbose,
            details,
        };

        if verbose != crate::types::VerboseMode::Off {
            let text = Self::format_initial_state(verbose, &state);
            if let Some(message) = ctx.send_markdown(&text, true).await {
                state.last_sent = Some((message.id, text));
            }
        }
        self.messages.insert(id, state);
    }

    async fn handle_tool_call_update(
        &mut self,
        update: &acp::ToolCallUpdate,
        ctx: &mut EventContext,
    ) {
        let id = update.tool_call_id.to_string();
        let fields = &update.fields;
        let name = fields.title.clone().unwrap_or_default();
        let kind = fields.kind;
        let status = fields.status;
        let output = fields
            .content
            .as_ref()
            .and_then(|contents| extract_tool_result_text(contents));
        let details = extract_tool_diff(fields.content.as_deref().unwrap_or(&[]));
        let verbose = *self.verbose.lock().await;

        let resolved_name = if !name.is_empty() {
            name.clone()
        } else {
            self.messages
                .get(&id)
                .map(|s| s.name.clone())
                .unwrap_or_default()
        };
        let resolved_details = if details.is_some() {
            details.clone()
        } else {
            self.messages.get(&id).and_then(|s| s.details.clone())
        };
        let resolved_kind = kind
            .or_else(|| self.messages.get(&id).map(|s| s.kind))
            .unwrap_or(acp::ToolKind::Other);
        let resolved_status = status
            .or_else(|| self.messages.get(&id).map(|s| s.status))
            .unwrap_or(acp::ToolCallStatus::Pending);
        if let Some(state) = self.messages.get_mut(&id) {
            state.name = resolved_name.clone();
            state.kind = resolved_kind;
            state.status = resolved_status;
            if details.is_some() {
                state.details = details;
            }

            if let Some((msg_id, last_text)) = state.last_sent.as_ref() {
                let text = Self::format_state(state.display_mode, state, output.as_deref());
                let msg_id = *msg_id;
                if last_text != &text && ctx.edit_markdown(msg_id, &text).await {
                    state.last_sent = Some((msg_id, text));
                }
            } else if state.display_mode != crate::types::VerboseMode::Off {
                let text = Self::format_state(state.display_mode, state, output.as_deref());
                if let Some(sent) = ctx.send_markdown(&text, true).await {
                    state.last_sent = Some((sent.id, text));
                }
            }
            return;
        }

        let mut state = ToolCallMessageState {
            last_sent: None,
            name: resolved_name,
            kind: resolved_kind,
            status: resolved_status,
            display_mode: verbose,
            details: resolved_details,
        };
        if verbose != crate::types::VerboseMode::Off {
            let text = Self::format_state(verbose, &state, output.as_deref());
            if let Some(message) = ctx.send_markdown(&text, true).await {
                state.last_sent = Some((message.id, text));
            }
        }
        self.messages.insert(id, state);
    }

    fn format_state(
        mode: crate::types::VerboseMode,
        state: &ToolCallMessageState,
        output: Option<&str>,
    ) -> String {
        match mode {
            crate::types::VerboseMode::Off => String::new(),
            crate::types::VerboseMode::Compact => {
                formatting::format_tool_compact(&state.name, state.kind, state.status)
            }
            crate::types::VerboseMode::On => formatting::format_tool_result(
                &state.name,
                state.kind,
                state.status,
                output,
                state.details.as_deref(),
            ),
        }
    }

    fn format_initial_state(
        mode: crate::types::VerboseMode,
        state: &ToolCallMessageState,
    ) -> String {
        match mode {
            crate::types::VerboseMode::Off => String::new(),
            crate::types::VerboseMode::Compact => {
                formatting::format_tool_compact(&state.name, state.kind, state.status)
            }
            crate::types::VerboseMode::On => formatting::format_tool_call(
                &state.name,
                state.kind,
                state.status,
                state.details.as_deref(),
            ),
        }
    }
}

fn extract_tool_result_text(contents: &[acp::ToolCallContent]) -> Option<String> {
    let mut parts = Vec::new();
    for content in contents {
        match content {
            acp::ToolCallContent::Content(content) => {
                let text = match &content.content {
                    acp::ContentBlock::Text(tc) => tc.text.clone(),
                    _ => String::new(),
                };
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

fn extract_tool_diff(contents: &[acp::ToolCallContent]) -> Option<String> {
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
