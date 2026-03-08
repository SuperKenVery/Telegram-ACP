use std::collections::HashMap;
use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::types::{MessageId, ParseMode, ThreadId};
use tokio::sync::mpsc;

use crate::daemon::DaemonHandle;
use crate::formatting;
use crate::types::AgentEvent;

/// Send a message draft (streaming partial text) via the raw Telegram Bot API.
/// Uses `sendMessageDraft` (Bot API 9.3+). Returns Ok(()) on success.
async fn send_message_draft(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    draft_id: i64,
    text: &str,
) -> anyhow::Result<()> {
    let client = bot.client();
    let token = bot.token();
    let url = format!("https://api.telegram.org/bot{token}/sendMessageDraft");

    let mut body = serde_json::json!({
        "chat_id": chat_id.0,
        "draft_id": draft_id,
        "text": text,
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

/// Start the Telegram bot dispatcher. Runs until cancelled.
pub async fn run_bot(bot: Bot, daemon: Arc<DaemonHandle>) {
    use teloxide::dispatching::{Dispatcher, UpdateFilterExt};
    use teloxide::dptree;
    use teloxide::types::Update;

    let handler = dptree::entry().branch(Update::filter_message().endpoint(handle_message));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![daemon])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

/// Handle an incoming Telegram message.
async fn handle_message(bot: Bot, msg: Message, daemon: Arc<DaemonHandle>) -> anyhow::Result<()> {
    let text = match msg.text() {
        Some(t) => t,
        None => return Ok(()),
    };

    // Only process messages in our target chat
    if msg.chat.id != ChatId(daemon.config.chat_id) {
        return Ok(());
    }

    // Handle /new command
    if text.starts_with("/new") {
        handle_new_command(&bot, &msg, text, &daemon).await?;
        return Ok(());
    }

    // Handle messages in forum topics -> route to session
    if let Some(thread_id) = msg.thread_id {
        handle_topic_message(&bot, &msg, text, thread_id, &daemon).await?;
    }

    Ok(())
}

/// Handle the /new command to spawn a new agent session.
async fn handle_new_command(
    bot: &Bot,
    msg: &Message,
    text: &str,
    daemon: &DaemonHandle,
) -> anyhow::Result<()> {
    let args: Vec<&str> = text.splitn(3, ' ').collect();
    let path = args.get(1).unwrap_or(&"/tmp");
    let prompt = args.get(2).map(|s| s.to_string());

    match daemon
        .spawn_session(path.to_string(), prompt.clone(), None)
        .await
    {
        Ok((acp_session_id, thread_id)) => {
            let reply = format!(
                "Session <code>{}</code> created in topic.",
                formatting::escape_html(&acp_session_id)
            );
            bot.send_message(msg.chat.id, reply)
                .parse_mode(ParseMode::Html)
                .await?;

            // If there's an initial prompt, send it to the session
            if let Some(prompt_text) = prompt {
                if let Some(user_tx) = daemon.get_session_tx_by_thread(thread_id) {
                    let _ = user_tx.send(prompt_text);
                }
            }
        }
        Err(e) => {
            bot.send_message(msg.chat.id, format!("Failed to create session: {e}"))
                .await?;
        }
    }

    Ok(())
}

/// Handle a message in a forum topic, routing it to the corresponding agent session.
async fn handle_topic_message(
    _bot: &Bot,
    _msg: &Message,
    text: &str,
    thread_id: ThreadId,
    daemon: &DaemonHandle,
) -> anyhow::Result<()> {
    if let Some(user_tx) = daemon.get_session_tx_by_thread(thread_id.0 .0) {
        let _ = user_tx.send(text.to_string());
    }
    Ok(())
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
}

/// Flush the accumulated draft text as a finalized `sendMessage`.
/// Clears the draft state. Returns Ok(()) even if sending fails (logs error).
async fn flush_draft(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    draft: &mut Option<DraftState>,
    disable_notification: bool,
) {
    if let Some(d) = draft.take() {
        if d.text.is_empty() {
            return;
        }
        let formatted = formatting::format_text_message(&d.text);
        let chunks = formatting::split_message(&formatted, 4096);
        for chunk in chunks {
            let _ = bot
                .send_message(chat_id, chunk)
                .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                .parse_mode(ParseMode::Html)
                .disable_notification(disable_notification)
                .await;
        }
    }
}

/// Delete the "Working on it..." indicator message if one exists.
async fn delete_working_msg(
    bot: &Bot,
    chat_id: ChatId,
    working_msg_id: &mut Option<teloxide::types::MessageId>,
) {
    if let Some(msg_id) = working_msg_id.take() {
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
    // Message ID of the "Working on it..." indicator, to delete when a real event arrives.
    let mut working_msg_id: Option<teloxide::types::MessageId> = None;

    while let Some(event) = event_rx.recv().await {
        match &event {
            AgentEvent::TextMessage(t) => {
                // Delete the "working" indicator on first real content
                delete_working_msg(&bot, chat_id, &mut working_msg_id).await;

                // Accumulate text into the current draft, or start a new one
                let d = draft.get_or_insert_with(|| DraftState {
                    draft_id: rand_draft_id(),
                    text: String::new(),
                });
                d.text.push_str(t);

                // Stream the partial text via sendMessageDraft (best-effort)
                let _ = send_message_draft(&bot, chat_id, thread_id, d.draft_id, &d.text).await;
                continue;
            }
            _ => {
                // Non-text event: flush any accumulated draft first
                let disable_notification = message_count > 0;
                flush_draft(&bot, chat_id, thread_id, &mut draft, disable_notification).await;
                if message_count == 0 {
                    message_count += 1;
                }
            }
        }

        // Delete the "working" indicator before sending any non-Working event
        if !matches!(event, AgentEvent::Working) {
            delete_working_msg(&bot, chat_id, &mut working_msg_id).await;
        }

        match &event {
            AgentEvent::Working => {
                let text = "⏳ <i>Working on it...</i>".to_string();
                let disable_notification = message_count > 0;
                message_count += 1;
                let result = bot
                    .send_message(chat_id, &text)
                    .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                    .parse_mode(ParseMode::Html)
                    .disable_notification(disable_notification)
                    .await;
                if let Ok(sent) = result {
                    working_msg_id = Some(sent.id);
                }
            }
            AgentEvent::TextMessage(_) => unreachable!(),
            AgentEvent::ToolCall { id, name } => {
                let text = formatting::format_tool_call(name);
                let disable_notification = message_count > 0;
                message_count += 1;
                if let Ok(sent) = bot
                    .send_message(chat_id, &text)
                    .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                    .parse_mode(ParseMode::Html)
                    .disable_notification(disable_notification)
                    .await
                {
                    tool_call_messages.insert(
                        id.clone(),
                        ToolCallMessageState {
                            msg_id: sent.id,
                            name: name.clone(),
                        },
                    );
                }
            }
            AgentEvent::ToolCallUpdate { id, name, output } => {
                let resolved_name = if !name.is_empty() {
                    name.clone()
                } else {
                    tool_call_messages
                        .get(id)
                        .map(|s| s.name.clone())
                        .unwrap_or_default()
                };
                let text = formatting::format_tool_result(&resolved_name, output.as_deref());

                if let Some(state) = tool_call_messages.get_mut(id) {
                    if bot
                        .edit_message_text(chat_id, state.msg_id, text.clone())
                        .parse_mode(ParseMode::Html)
                        .await
                        .is_ok()
                    {
                        if !name.is_empty() {
                            state.name = name.clone();
                        }
                        continue;
                    }
                }

                // Fallback: if edit fails or we don't have prior tool-call message, send a new one.
                let disable_notification = message_count > 0;
                message_count += 1;
                if let Ok(sent) = bot
                    .send_message(chat_id, &text)
                    .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                    .parse_mode(ParseMode::Html)
                    .disable_notification(disable_notification)
                    .await
                {
                    tool_call_messages.insert(
                        id.clone(),
                        ToolCallMessageState {
                            msg_id: sent.id,
                            name: resolved_name,
                        },
                    );
                }
            }
            AgentEvent::Finished(reason) => {
                let text = formatting::format_completion(reason, None);
                let chunks = formatting::split_message(&text, 4096);
                let disable_notification = false;
                for chunk in chunks {
                    let _ = bot
                        .send_message(chat_id, &chunk)
                        .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                        .parse_mode(ParseMode::Html)
                        .disable_notification(disable_notification)
                        .await;
                }
                message_count = 0;
                tool_call_messages.clear();
            }
            AgentEvent::Error(e) => {
                let text = formatting::format_error(e);
                let chunks = formatting::split_message(&text, 4096);
                let disable_notification = false;
                for chunk in chunks {
                    let _ = bot
                        .send_message(chat_id, &chunk)
                        .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                        .parse_mode(ParseMode::Html)
                        .disable_notification(disable_notification)
                        .await;
                }
                message_count = 0;
                tool_call_messages.clear();
            }
        }
    }

    // Channel closed — session is done. Flush any remaining draft and close the topic.
    flush_draft(&bot, chat_id, thread_id, &mut draft, true).await;
    let _ = bot
        .close_forum_topic(chat_id, ThreadId(teloxide::types::MessageId(thread_id)))
        .await;
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
