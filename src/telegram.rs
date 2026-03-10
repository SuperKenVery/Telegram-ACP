use std::collections::HashMap;
use std::sync::Arc;

use agent_client_protocol as acp;
use similar::TextDiff;
use teloxide::prelude::*;
use teloxide::types::{
    BotCommandScope, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, MessageId,
    ParseMode, Recipient, ThreadId,
};
use tokio::sync::mpsc;
use tokio::time::{sleep_until, Duration, Instant};

use crate::commands;
use crate::daemon::DaemonHandle;
use crate::formatting;
use crate::types::AgentEvent;

struct OutboundThrottle {
    min_interval: Duration,
    next_allowed_at: Instant,
}

impl OutboundThrottle {
    fn per_second(count: u64) -> Self {
        let min_interval = Duration::from_secs_f64(1.0 / count as f64);
        Self {
            min_interval,
            next_allowed_at: Instant::now(),
        }
    }

    async fn wait_turn(&mut self) {
        let now = Instant::now();
        if self.next_allowed_at > now {
            sleep_until(self.next_allowed_at).await;
        }
        self.next_allowed_at = Instant::now() + self.min_interval;
    }

    fn try_turn(&mut self) -> bool {
        let now = Instant::now();
        if self.next_allowed_at > now {
            return false;
        }
        self.next_allowed_at = now + self.min_interval;
        true
    }
}

/// Send a message draft (streaming partial text) via the raw Telegram Bot API.
/// Uses `sendMessageDraft` (Bot API 9.3+). Returns Ok(()) on success.
async fn send_message_draft(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    draft_id: i64,
    text: &str,
    throttle: &mut OutboundThrottle,
) -> anyhow::Result<()> {
    // Coalesce frequent streaming updates: if we are inside the 1s window,
    // skip this send and let the next allowed update publish full draft text.
    if !throttle.try_turn() {
        return Ok(());
    }
    let client = bot.client();
    let token = bot.token();
    let url = format!("https://api.telegram.org/bot{token}/sendMessageDraft");

    let mut body = serde_json::json!({
        "chat_id": chat_id.0,
        "draft_id": draft_id,
        "text": text,
        "parse_mode": "MarkdownV2",
    });

    if thread_id != 0 {
        body["message_thread_id"] = serde_json::json!(thread_id);
    }

    let resp = client.post(&url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        anyhow::bail!("sendMessageDraft failed ({status}): {body_text}");
    }
    Ok(())
}

fn fix_md_for_telegram(text: &str) -> String {
    match telegram_markdown_v2::convert(text) {
        Ok(converted) => converted.trim_end_matches('\n').to_string(),
        Err(e) => {
            tracing::warn!("telegram_markdown_v2 conversion failed, using escaped fallback: {e}");
            formatting::escape_markdown_v2(text)
        }
    }
}

/// Start the Telegram bot dispatcher. Runs until cancelled.
pub async fn run_bot(bot: Bot, daemon: Arc<DaemonHandle>) {
    use teloxide::dispatching::{Dispatcher, UpdateFilterExt};
    use teloxide::dptree;
    use teloxide::types::Update;

    if let Err(e) = register_bot_commands(&bot, daemon.config.chat_id).await {
        tracing::warn!("Failed to register Telegram slash commands: {e}");
    }

    let handler = dptree::entry()
        .branch(Update::filter_message().endpoint(handle_message))
        .branch(Update::filter_callback_query().endpoint(handle_callback_query));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![daemon])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn register_bot_commands(bot: &Bot, chat_id: i64) -> anyhow::Result<()> {
    bot.set_my_commands(commands::telegram_menu_commands())
        .scope(BotCommandScope::Chat {
            chat_id: Recipient::Id(ChatId(chat_id)),
        })
        .await?;
    Ok(())
}

/// Handle an incoming Telegram message.
async fn handle_message(bot: Bot, msg: Message, daemon: Arc<DaemonHandle>) -> anyhow::Result<()> {
    // Only process messages in our target chat
    if msg.chat.id != ChatId(daemon.config.chat_id) {
        return Ok(());
    }

    // Handle known slash commands
    if commands::execute_slash_command(&bot, &msg, &daemon).await? {
        return Ok(());
    }

    // Handle messages in forum topics -> route to session
    if let Some(thread_id) = msg.thread_id {
        if let Some(text) = msg.text() {
            handle_topic_message(&bot, text, thread_id, &daemon).await?;
        }
    }

    Ok(())
}

/// Handle a message in a forum topic, routing it to the corresponding agent session.
async fn handle_topic_message(
    bot: &Bot,
    text: &str,
    thread_id: ThreadId,
    daemon: &DaemonHandle,
) -> anyhow::Result<()> {
    let thread = thread_id.0 .0;
    if daemon.get_session_command_tx_by_thread(thread).is_none() {
        return Ok(());
    }

    if let Some(queued_prompts) = daemon.enqueue_user_prompt(thread, text.to_string()).await? {
        send_queued_notice(bot, daemon.config.chat_id, thread, &queued_prompts).await;
    }
    Ok(())
}

fn build_interrupt_callback_data(thread_id: i32) -> String {
    format!("cancelq:{thread_id}")
}

async fn send_queued_notice(bot: &Bot, chat_id: i64, thread_id: i32, queued_prompts: &[String]) {
    let mut lines = Vec::with_capacity(queued_prompts.len() + 2);
    lines.push("Agent is currently working.".to_string());
    lines.push("Your message was queued. Pending queue:".to_string());
    for (idx, prompt) in queued_prompts.iter().enumerate() {
        lines.push(format!("{}. {}", idx + 1, prompt));
    }
    let text = lines.join("\n");
    let chunks = formatting::split_message(&text, 4096);
    let callback_data = build_interrupt_callback_data(thread_id);
    let keyboard = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
        "Interrupt and run queued now",
        callback_data,
    )]]);

    let mut iter = chunks.into_iter().peekable();
    while let Some(chunk) = iter.next() {
        let mut request = bot
            .send_message(ChatId(chat_id), chunk)
            .message_thread_id(ThreadId(MessageId(thread_id)));
        if iter.peek().is_none() {
            request = request.reply_markup(keyboard.clone());
        }
        if let Err(e) = request.await {
            tracing::warn!(
                chat_id = chat_id,
                thread_id = thread_id,
                "Failed to send queued notice: {e}"
            );
            break;
        }
    }
}

async fn handle_callback_query(
    bot: Bot,
    query: CallbackQuery,
    daemon: Arc<DaemonHandle>,
) -> anyhow::Result<()> {
    commands::handle_callback_query(bot, query, daemon).await
}

/// State for an in-progress text draft being streamed.
struct DraftState {
    draft_id: i64,
    text: String,
}

/// Tracks which Telegram message corresponds to a tool call id.
struct ToolCallMessageState {
    msg_id: MessageId,
    name: String,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    details: Option<String>,
}

/// Flush the accumulated draft text as a finalized `sendMessage`.
/// Clears the draft state. Returns Ok(()) even if sending fails (logs error).
async fn flush_draft(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    draft: &mut Option<DraftState>,
    disable_notification: bool,
    throttle: &mut OutboundThrottle,
) {
    if let Some(d) = draft.take() {
        if d.text.is_empty() {
            return;
        }
        let formatted = formatting::format_text_message(&d.text);
        // Keep finalize behavior aligned with streaming drafts:
        // normalize/escape agent markdown before sending via MarkdownV2.
        let finalized_text = fix_md_for_telegram(&formatted);
        let chunks = formatting::split_message(&finalized_text, 4096);
        for chunk in chunks {
            throttle.wait_turn().await;
            let send_result = bot
                .send_message(chat_id, chunk.clone())
                .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                .parse_mode(ParseMode::MarkdownV2)
                .disable_notification(disable_notification)
                .await;

            // If agent-provided MarkdownV2 is invalid, retry with escaped plain text
            // so content is never dropped when the draft is finalized.
            if let Err(e) = send_result {
                tracing::warn!(
                    chat_id = chat_id.0,
                    thread_id = thread_id,
                    chunk_len = chunk.len(),
                    "Failed to send finalized draft chunk as MarkdownV2: {e}"
                );
                let safe_text = formatting::escape_markdown_v2(&chunk);
                let safe_chunks = formatting::split_message(&safe_text, 4096);
                for safe_chunk in safe_chunks {
                    throttle.wait_turn().await;
                    if let Err(e) = bot
                        .send_message(chat_id, safe_chunk)
                        .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                        .parse_mode(ParseMode::MarkdownV2)
                        .disable_notification(disable_notification)
                        .await
                    {
                        tracing::warn!(
                            chat_id = chat_id.0,
                            thread_id = thread_id,
                            "Failed to send escaped finalized draft chunk fallback: {e}"
                        );
                    }
                }
                break;
            }
        }
    }
}

/// Delete the "Working on it..." indicator message if one exists.
async fn delete_working_msg(
    bot: &Bot,
    chat_id: ChatId,
    working_msg_id: &mut Option<teloxide::types::MessageId>,
    throttle: &mut OutboundThrottle,
) {
    if let Some(msg_id) = working_msg_id.take() {
        throttle.wait_turn().await;
        let _ = bot.delete_message(chat_id, msg_id).await;
    }
}

/// Consume AgentEvents and send them as Telegram messages in the forum topic.
/// Consecutive text chunks are streamed via `sendMessageDraft` and finalized
/// with `sendMessage` when a non-text event arrives or the stream ends.
/// Runs until the event channel is closed (i.e., the session ends).
pub async fn run_event_consumer(
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    mut event_rx: mpsc::UnboundedReceiver<AgentEvent>,
) {
    let mut message_count = 0u32;
    let mut draft: Option<DraftState> = None;
    let mut tool_call_messages: HashMap<String, ToolCallMessageState> = HashMap::new();
    let mut throttle = OutboundThrottle::per_second(1);
    // Message ID of the "Working on it..." indicator, to delete when a real event arrives.
    let mut working_msg_id: Option<teloxide::types::MessageId> = None;

    while let Some(event) = event_rx.recv().await {
        match &event {
            AgentEvent::Update(acp::SessionUpdate::AgentMessageChunk(chunk)) => {
                let t = extract_text(&chunk.content);
                if t.is_empty() {
                    continue;
                }
                // Delete the "working" indicator on first real content
                delete_working_msg(&bot, chat_id, &mut working_msg_id, &mut throttle).await;

                // Accumulate text into the current draft, or start a new one
                let d = draft.get_or_insert_with(|| DraftState {
                    draft_id: rand_draft_id(),
                    text: String::new(),
                });
                d.text.push_str(&t);

                // Stream the partial text via sendMessageDraft (best-effort)
                let escaped_draft_text = fix_md_for_telegram(&d.text);
                if let Err(e) =
                    send_message_draft(
                        &bot,
                        chat_id,
                        thread_id,
                        d.draft_id,
                        &escaped_draft_text,
                        &mut throttle,
                    )
                    .await
                {
                    tracing::warn!(
                        chat_id = chat_id.0,
                        thread_id = thread_id,
                        draft_id = d.draft_id,
                        text_len = d.text.len(),
                        "sendMessageDraft failed: {e}"
                    );
                }
                continue;
            }
            _ => {
                // Non-text event: flush any accumulated draft first
                let disable_notification = message_count > 0;
                flush_draft(
                    &bot,
                    chat_id,
                    thread_id,
                    &mut draft,
                    disable_notification,
                    &mut throttle,
                )
                .await;
                if message_count == 0 {
                    message_count += 1;
                }
            }
        }

        // Delete the "working" indicator before sending any non-Working event
        if !matches!(event, AgentEvent::Working) {
            delete_working_msg(&bot, chat_id, &mut working_msg_id, &mut throttle).await;
        }

        match event {
            AgentEvent::Working => {
                let text = "⏳ _Working on it\\.\\.\\._".to_string();
                let disable_notification = message_count > 0;
                message_count += 1;
                throttle.wait_turn().await;
                let result = bot
                    .send_message(chat_id, &text)
                    .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                    .parse_mode(ParseMode::MarkdownV2)
                    .disable_notification(disable_notification)
                    .await;
                if let Ok(sent) = result {
                    working_msg_id = Some(sent.id);
                }
            }
            AgentEvent::Update(acp::SessionUpdate::ToolCall(tool_call)) => {
                let id = tool_call.tool_call_id.to_string();
                let name = tool_call.title;
                let kind = tool_call.kind;
                let status = tool_call.status;
                let details = extract_tool_details(&tool_call.content);
                let text = formatting::format_tool_call(&name, kind, status, details.as_deref());
                let disable_notification = message_count > 0;
                message_count += 1;
                throttle.wait_turn().await;
                if let Ok(sent) = bot
                    .send_message(chat_id, &text)
                    .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                    .parse_mode(ParseMode::MarkdownV2)
                    .disable_notification(disable_notification)
                    .await
                {
                    tool_call_messages.insert(
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
            AgentEvent::Update(acp::SessionUpdate::ToolCallUpdate(update)) => {
                let id = update.tool_call_id.to_string();
                let fields = update.fields;
                let name = fields.title.unwrap_or_default();
                let kind = fields.kind;
                let status = fields.status;
                let output = fields
                    .content
                    .as_ref()
                    .and_then(|contents| extract_tool_output(contents));
                let details = extract_tool_details(fields.content.as_deref().unwrap_or(&[]));

                let resolved_name = if !name.is_empty() {
                    name.clone()
                } else {
                    tool_call_messages
                        .get(&id)
                        .map(|s| s.name.clone())
                        .unwrap_or_default()
                };
                let resolved_details = if details.is_some() {
                    details.clone()
                } else {
                    tool_call_messages.get(&id).and_then(|s| s.details.clone())
                };
                let resolved_kind = kind
                    .or_else(|| tool_call_messages.get(&id).map(|s| s.kind))
                    .unwrap_or(acp::ToolKind::Other);
                let resolved_status = status
                    .or_else(|| tool_call_messages.get(&id).map(|s| s.status))
                    .unwrap_or(acp::ToolCallStatus::Pending);
                let text = formatting::format_tool_result(
                    &resolved_name,
                    resolved_kind,
                    resolved_status,
                    output.as_deref(),
                    resolved_details.as_deref(),
                );

                if let Some(state) = tool_call_messages.get_mut(&id) {
                    throttle.wait_turn().await;
                    if bot
                        .edit_message_text(chat_id, state.msg_id, text.clone())
                        .parse_mode(ParseMode::MarkdownV2)
                        .await
                        .is_ok()
                    {
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
                        continue;
                    }
                }

                // Fallback: if edit fails or we don't have prior tool-call message, send a new one.
                let disable_notification = message_count > 0;
                message_count += 1;
                throttle.wait_turn().await;
                if let Ok(sent) = bot
                    .send_message(chat_id, &text)
                    .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                    .parse_mode(ParseMode::MarkdownV2)
                    .disable_notification(disable_notification)
                    .await
                {
                    tool_call_messages.insert(
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
            AgentEvent::Update(acp::SessionUpdate::UsageUpdate(usage)) => {
                let text = formatting::format_text_message(&format_usage_update(&usage));
                let chunks = formatting::split_message(&text, 4096);
                let disable_notification = message_count > 0;
                message_count += 1;
                for chunk in chunks {
                    throttle.wait_turn().await;
                    let _ = bot
                        .send_message(chat_id, &chunk)
                        .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                        .parse_mode(ParseMode::MarkdownV2)
                        .disable_notification(disable_notification)
                        .await;
                }
            }
            AgentEvent::Update(_) => {}
            AgentEvent::Finished(reason) => {
                let text = formatting::format_completion(&reason, None);
                let chunks = formatting::split_message(&text, 4096);
                let disable_notification = false;
                for chunk in chunks {
                    throttle.wait_turn().await;
                    let _ = bot
                        .send_message(chat_id, &chunk)
                        .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                        .parse_mode(ParseMode::MarkdownV2)
                        .disable_notification(disable_notification)
                        .await;
                }
                message_count = 0;
                tool_call_messages.clear();
            }
            AgentEvent::Error(e) => {
                let text = formatting::format_error(&e);
                let chunks = formatting::split_message(&text, 4096);
                let disable_notification = false;
                for chunk in chunks {
                    throttle.wait_turn().await;
                    let _ = bot
                        .send_message(chat_id, &chunk)
                        .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                        .parse_mode(ParseMode::MarkdownV2)
                        .disable_notification(disable_notification)
                        .await;
                }
                message_count = 0;
                tool_call_messages.clear();
            }
        }
    }

    // Channel closed — session is done. Flush any remaining draft and close the topic.
    flush_draft(&bot, chat_id, thread_id, &mut draft, true, &mut throttle).await;
    throttle.wait_turn().await;
    let _ = bot
        .close_forum_topic(chat_id, ThreadId(teloxide::types::MessageId(thread_id)))
        .await;
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

fn extract_tool_details(contents: &[acp::ToolCallContent]) -> Option<String> {
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

fn format_usage_update(usage: &acp::UsageUpdate) -> String {
    let percent = if usage.size == 0 {
        0.0
    } else {
        (usage.used as f64 / usage.size as f64) * 100.0
    };

    let cost = usage
        .cost
        .as_ref()
        .map(|c| format!(", cost {:.4} {}", c.amount, c.currency))
        .unwrap_or_default();

    format!(
        "Usage update: {}/{} tokens ({percent:.1}%){}",
        usage.used, usage.size, cost
    )
}

/// Generate a random i64 to use as a draft_id.
fn rand_draft_id() -> i64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let s = RandomState::new();
    let mut h = s.build_hasher();
    h.write_u8(0);
    h.finish() as i64
}
