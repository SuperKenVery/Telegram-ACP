use std::collections::HashMap;

use agent_client_protocol as acp;
use similar::TextDiff;
use teloxide::types::MessageId;

use super::{EventContext, EventHandler};
use crate::formatting;
use crate::types::AgentEvent;

struct ToolCallMessageState {
    msg_id: MessageId,
    name: String,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    details: Option<String>,
}

pub struct ToolCallHandler {
    messages: HashMap<String, ToolCallMessageState>,
}

impl ToolCallHandler {
    pub fn new() -> Self {
        Self {
            messages: HashMap::new(),
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
        if let Some(sent) = ctx
            .send_html(
                &formatting::format_tool_call(&name, kind, status, details.as_deref()),
                true,
            )
            .await
        {
            self.messages.insert(
                id,
                ToolCallMessageState {
                    msg_id: sent.id,
                    name,
                    kind,
                    status,
                    details,
                },
            );
        }
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
        let text = formatting::format_tool_result(
            &resolved_name,
            resolved_kind,
            resolved_status,
            output.as_deref(),
            resolved_details.as_deref(),
        );

        if let Some(state) = self.messages.get_mut(&id) {
            ctx.edit_html(state.msg_id, &text).await;
            if !name.is_empty() {
                state.name = name;
            }
            if let Some(k) = kind {
                state.kind = k;
            }
            if let Some(s) = status {
                state.status = s;
            }
            if details.is_some() {
                state.details = details;
            }
            return;
        }

        if let Some(sent) = ctx.send_html(&text, true).await {
            self.messages.insert(
                id,
                ToolCallMessageState {
                    msg_id: sent.id,
                    name: resolved_name,
                    kind: resolved_kind,
                    status: resolved_status,
                    details: resolved_details,
                },
            );
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

