use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::types::{BotCommandScope, CallbackQuery, Recipient};

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
        if let Some(text) = msg.text() {
            handle_topic_message(text, thread_id, &daemon).await?;
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
