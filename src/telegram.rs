use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::types::{BotCommandScope, CallbackQuery, MessageKind, Recipient};

use crate::commands;
use crate::daemon::DaemonHandle;
use crate::session_control::SessionCommand;

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
    if msg.chat.id != ChatId(daemon.config.chat_id) {
        return Ok(());
    }

    if commands::execute_slash_command(&bot, &msg, &daemon).await? {
        return Ok(());
    }

    if let Some(thread_id) = msg.thread_id {
        if msg.from().map(|user| user.is_bot).unwrap_or(false) {
            return Ok(());
        }

        if let Err(e) = bot
            .pin_chat_message(msg.chat.id, msg.id)
            .disable_notification(true)
            .await
        {
            tracing::warn!(
                chat_id = msg.chat.id.0,
                thread_id = thread_id.0 .0,
                message_id = msg.id.0,
                "Failed to pin user message: {e}"
            );
        }

        if let Some(text) = msg.text() {
            let prompt = build_prompt_with_quote(&msg, text);
            handle_topic_message(&prompt, thread_id, &daemon).await?;
        }
    }

    Ok(())
}

/// Handle a message in a forum topic, routing it to the corresponding agent session.
async fn handle_topic_message(
    text: &str,
    thread_id: teloxide::types::ThreadId,
    daemon: &DaemonHandle,
) -> anyhow::Result<()> {
    let thread = thread_id.0 .0;
    let Some(command_tx) = daemon.get_session_command_tx_by_thread(thread) else {
        return Ok(());
    };

    command_tx
        .send(SessionCommand::Prompt(text.to_string()))
        .map_err(|_| anyhow::anyhow!("Session command channel closed"))?;

    Ok(())
}

async fn handle_callback_query(
    bot: Bot,
    query: CallbackQuery,
    daemon: Arc<DaemonHandle>,
) -> anyhow::Result<()> {
    commands::handle_callback_query(bot, query, daemon).await
}

fn build_prompt_with_quote(msg: &Message, raw_text: &str) -> String {
    let Some(quote_text) = extract_quote_text(msg) else {
        return raw_text.to_string();
    };

    format!("{}\n\n{}", format_blockquote(&quote_text), raw_text)
}

fn extract_quote_text(msg: &Message) -> Option<String> {
    let MessageKind::Common(common) = &msg.kind else {
        return None;
    };

    if let Some(quote) = &common.quote {
        if !quote.text.trim().is_empty() {
            return Some(quote.text.clone());
        }
    }

    let reply = common.reply_to_message.as_deref()?;
    let text = reply.text().or_else(|| reply.caption())?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn format_blockquote(text: &str) -> String {
    text.lines()
        .map(|line| format!("> {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
