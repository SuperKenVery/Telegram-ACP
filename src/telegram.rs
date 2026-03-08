use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::types::{ParseMode, ThreadId};
use tokio::sync::mpsc;

use crate::daemon::DaemonHandle;
use crate::formatting;
use crate::types::AgentEvent;

/// Start the Telegram bot dispatcher. Runs until cancelled.
pub async fn run_bot(bot: Bot, daemon: Arc<DaemonHandle>) {
    use teloxide::dispatching::{Dispatcher, UpdateFilterExt};
    use teloxide::dptree;
    use teloxide::types::Update;

    let handler = dptree::entry()
        .branch(Update::filter_message().endpoint(handle_message));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![daemon])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

/// Handle an incoming Telegram message.
async fn handle_message(
    bot: Bot,
    msg: Message,
    daemon: Arc<DaemonHandle>,
) -> anyhow::Result<()> {
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
        Ok((session_id, _thread_id)) => {
            let reply = format!(
                "Session <code>{}</code> created in topic.",
                formatting::escape_html(&session_id)
            );
            bot.send_message(msg.chat.id, reply)
                .parse_mode(ParseMode::Html)
                .await?;

            // If there's an initial prompt, send it to the session
            if let Some(prompt_text) = prompt {
                if let Some(user_tx) = daemon.get_session_tx(&session_id) {
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
    if let Some(session_id) = daemon.thread_to_session.get(&thread_id.0 .0) {
        let session_id = session_id.value().clone();
        if let Some(user_tx) = daemon.get_session_tx(&session_id) {
            let _ = user_tx.send(text.to_string());
        }
    }
    Ok(())
}

/// Consume AgentEvents and send them as Telegram messages in the forum topic.
pub async fn run_event_consumer(
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    mut event_rx: mpsc::UnboundedReceiver<AgentEvent>,
) {
    let mut message_count = 0u32;

    while let Some(event) = event_rx.recv().await {
        let (text, is_final) = match &event {
            AgentEvent::Working => ("⏳ <i>Working on it...</i>".to_string(), false),
            AgentEvent::TextMessage(t) => (formatting::format_text_message(t), false),
            AgentEvent::ToolCall { name, .. } => (formatting::format_tool_call(name), false),
            AgentEvent::ToolCallUpdate { name, output, .. } => {
                (formatting::format_tool_result(name, output.as_deref()), false)
            }
            AgentEvent::Finished(reason) => {
                (formatting::format_completion(reason, None), true)
            }
            AgentEvent::Error(e) => (formatting::format_error(e), true),
        };

        // Notification logic: first message and final message notify, others silent
        let disable_notification = message_count > 0 && !is_final;
        message_count += 1;

        // Split long messages
        let chunks = formatting::split_message(&text, 4096);
        for chunk in chunks {
            let _ = bot
                .send_message(chat_id, chunk)
                .message_thread_id(ThreadId(teloxide::types::MessageId(thread_id)))
                .parse_mode(ParseMode::Html)
                .disable_notification(disable_notification)
                .await;
        }

        if is_final {
            // Close the forum topic
            let _ = bot
                .close_forum_topic(chat_id, ThreadId(teloxide::types::MessageId(thread_id)))
                .await;
            break;
        }
    }
}
