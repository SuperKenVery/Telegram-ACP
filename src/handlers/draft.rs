use agent_client_protocol as acp;
use teloxide::types::ParseMode;

use super::EventContext;
use crate::formatting;
use crate::types::AgentEvent;

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
            let (finalized_text, parse_mode) = match d.kind {
                DraftKind::AgentMessage => {
                    let formatted = formatting::format_text_message(&d.text);
                    (
                        formatting::markdown_to_telegram_md_v2(&formatted),
                        ParseMode::MarkdownV2,
                    )
                }
                DraftKind::AgentThought => {
                    (formatting::format_thought_message(&d.text), ParseMode::Html)
                }
            };
            ctx.send_chunks(&finalized_text, parse_mode, true).await;
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
            tracing::warn!(
                chat_id = ctx.chat_id.0,
                thread_id = ctx.thread_id,
                text_len = d.text.len(),
                "Draft message update failed: {e}"
            );
        }
    }
}

/// Send a streaming draft update via the raw Telegram Bot API (sendMessageDraft).
/// Non-blocking: skips if within the throttle window.
async fn send_streaming_draft(
    ctx: &mut EventContext,
    draft_id: i64,
    text: &str,
) -> anyhow::Result<()> {
    if !ctx.throttle.try_turn() {
        return Ok(());
    }
    let client = ctx.bot.client();
    let token = ctx.bot.token();
    let url = format!("https://api.telegram.org/bot{token}/sendMessageDraft");
    let telegram_text = formatting::markdown_to_telegram_md_v2(text);
    let draft_text = formatting::truncate_message(&telegram_text, 4096);

    let mut body = serde_json::json!({
        "chat_id": ctx.chat_id.0,
        "draft_id": draft_id,
        "text": draft_text,
        "parse_mode": "MarkdownV2",
    });

    if ctx.thread_id != 0 {
        body["message_thread_id"] = serde_json::json!(ctx.thread_id);
    }

    let resp = client.post(&url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        anyhow::bail!("sendMessageDraft failed ({status}): {body_text}");
    }
    Ok(())
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