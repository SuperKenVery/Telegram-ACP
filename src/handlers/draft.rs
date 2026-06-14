use super::EventContext;
use crate::sess_warn;
use crate::types::AgentEvent;
use crate::{formatting, telegram_rich};
use agent_client_protocol as acp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DraftKind {
    AgentMessage,
    AgentThought,
}

struct DraftState {
    draft_id: i64,
    text: String,
    kind: DraftKind,
}

pub struct DraftHandler {
    draft: Option<DraftState>,
}

impl DraftHandler {
    pub fn new() -> Self {
        Self { draft: None }
    }

    /// Returns true if the event was a text chunk (consumed).
    pub async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool {
        match event {
            AgentEvent::Update(acp::SessionUpdate::AgentMessageChunk(chunk)) => {
                let t = extract_text(&chunk.content);
                if !t.is_empty() {
                    self.accumulate(&t, DraftKind::AgentMessage, ctx).await;
                }
                true
            }
            AgentEvent::Update(acp::SessionUpdate::AgentThoughtChunk(chunk)) => {
                let t = extract_text(&chunk.content);
                if !t.is_empty() {
                    self.accumulate(&t, DraftKind::AgentThought, ctx).await;
                }
                true
            }
            _ => false,
        }
    }

    /// Flush accumulated draft text as a finalized sendMessage.
    pub async fn flush(&mut self, ctx: &mut EventContext) {
        if let Some(d) = self.draft.take() {
            if d.text.is_empty() {
                return;
            }
            let finalized_text = match d.kind {
                DraftKind::AgentMessage => formatting::format_text_message(&d.text),
                DraftKind::AgentThought => formatting::format_thought_message(&d.text),
            };
            ctx.send_markdown_chunks(&finalized_text, true).await;
        }
    }

    async fn accumulate(&mut self, text: &str, kind: DraftKind, ctx: &mut EventContext) {
        // Flush if draft kind changed (e.g. message → thought)
        if matches!(self.draft.as_ref().map(|d| d.kind), Some(k) if k != kind) {
            self.flush(ctx).await;
        }
        let d = self.draft.get_or_insert_with(|| DraftState {
            draft_id: rand_draft_id(),
            text: String::new(),
            kind,
        });
        d.text.push_str(text);
        if let Err(e) = send_streaming_draft(ctx, d.draft_id, &d.text).await {
            sess_warn!(
                "Draft message update failed ({} bytes): {}",
                d.text.len(),
                e
            );
        }
    }
}

/// Send a streaming draft update via the raw Telegram Bot API (sendRichMessageDraft).
/// Non-blocking: skips if within the throttle window.
async fn send_streaming_draft(
    ctx: &mut EventContext,
    draft_id: i64,
    text: &str,
) -> anyhow::Result<()> {
    if !ctx.throttle.try_turn() {
        return Ok(());
    }

    telegram_rich::send_rich_markdown_draft(&ctx.bot, ctx.chat_id, ctx.thread_id, draft_id, text)
        .await
}

fn extract_text(content: &acp::ContentBlock) -> String {
    match content {
        acp::ContentBlock::Text(tc) => tc.text.clone(),
        _ => String::new(),
    }
}

fn rand_draft_id() -> i64 {
    use std::sync::atomic::{AtomicI64, Ordering};
    static COUNTER: AtomicI64 = AtomicI64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
