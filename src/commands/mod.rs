use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{BotCommand, CallbackQuery, Message};
use tokio::sync::oneshot;

use crate::daemon::DaemonHandle;
use crate::session_control::{SessionCommand, SessionControlState};

mod cancel;
mod model;
mod new;
mod permission;
mod rename;
mod remove;

use cancel::CancelCommand;
use model::ModelCommand;
use new::NewCommand;
use permission::PermissionCommand;
use rename::RenameCommand;
use remove::RemoveCommand;

pub struct CommandContext<'a> {
    pub bot: &'a Bot,
    pub msg: &'a Message,
    pub daemon: &'a DaemonHandle,
    pub thread_id: i32,
    pub args: &'a str,
}

#[async_trait]
pub trait Command: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()>;
}

fn command_registry() -> Vec<Box<dyn Command>> {
    vec![
        Box::new(NewCommand),
        Box::new(CancelCommand),
        Box::new(ModelCommand),
        Box::new(PermissionCommand),
        Box::new(RenameCommand),
        Box::new(RemoveCommand),
    ]
}

pub fn telegram_menu_commands() -> Vec<BotCommand> {
    command_registry()
        .into_iter()
        .map(|command| BotCommand::new(command.name(), command.description()))
        .collect()
}

fn parse_command(text: &str) -> Option<(String, String)> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let token = parts.next()?;
    let args = parts.next().unwrap_or("").trim().to_string();

    let command = token
        .trim_start_matches('/')
        .split('@')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();

    if command.is_empty() {
        return None;
    }

    Some((command, args))
}

pub async fn execute_slash_command(
    bot: &Bot,
    msg: &Message,
    daemon: &DaemonHandle,
) -> Result<bool> {
    let text = match msg.text() {
        Some(value) => value,
        None => return Ok(false),
    };

    let (command_name, args) = match parse_command(text) {
        Some(parsed) => parsed,
        None => return Ok(false),
    };

    let thread_id = match msg.thread_id {
        Some(id) => id.0 .0,
        None => return Ok(false),
    };

    let command = command_registry()
        .into_iter()
        .find(|command| command.name() == command_name);

    let Some(command) = command else {
        return Ok(false);
    };

    let ctx = CommandContext {
        bot,
        msg,
        daemon,
        thread_id,
        args: &args,
    };

    command.execute(ctx).await?;
    Ok(true)
}

pub async fn handle_callback_query(
    bot: Bot,
    query: CallbackQuery,
    daemon: Arc<DaemonHandle>,
) -> Result<()> {
    let chat_id = match &query.message {
        Some(message) => message.chat().id,
        None => {
            bot.answer_callback_query(query.id)
                .text("Message is no longer available")
                .await?;
            return Ok(());
        }
    };

    if chat_id != ChatId(daemon.config.chat_id) {
        return Ok(());
    }

    if model::try_handle_callback(&bot, &query, &daemon).await? {
        return Ok(());
    }

    if permission::try_handle_callback(&bot, &query, &daemon).await? {
        return Ok(());
    }

    if cancel::try_handle_callback(&bot, &query, &daemon).await? {
        return Ok(());
    }

    bot.answer_callback_query(query.id)
        .text("Unsupported action")
        .await?;

    Ok(())
}

pub(super) async fn get_control_state(
    daemon: &DaemonHandle,
    thread_id: i32,
) -> Result<SessionControlState> {
    let command_tx = daemon
        .get_session_command_tx_by_thread(thread_id)
        .ok_or_else(|| anyhow!("No active session in this topic"))?;
    let (result_tx, result_rx) = oneshot::channel();

    command_tx
        .send(SessionCommand::GetControlState { result_tx })
        .context("Failed to query session controls")?;

    result_rx.await.context("Session control query dropped")?
}

pub(super) async fn set_permission_mode(
    daemon: &DaemonHandle,
    thread_id: i32,
    mode_id: &str,
) -> Result<SessionControlState> {
    let command_tx = daemon
        .get_session_command_tx_by_thread(thread_id)
        .ok_or_else(|| anyhow!("No active session in this topic"))?;
    let (result_tx, result_rx) = oneshot::channel();

    command_tx
        .send(SessionCommand::SetPermissionMode {
            mode_id: mode_id.to_string(),
            result_tx,
        })
        .context("Failed to send permission mode request")?;

    result_rx.await.context("Permission mode request dropped")?
}

pub(super) async fn set_config_option(
    daemon: &DaemonHandle,
    thread_id: i32,
    config_id: &str,
    value_id: &str,
) -> Result<SessionControlState> {
    let command_tx = daemon
        .get_session_command_tx_by_thread(thread_id)
        .ok_or_else(|| anyhow!("No active session in this topic"))?;
    let (result_tx, result_rx) = oneshot::channel();

    command_tx
        .send(SessionCommand::SetConfigOption {
            config_id: config_id.to_string(),
            value_id: value_id.to_string(),
            result_tx,
        })
        .context("Failed to send model selection request")?;

    result_rx.await.context("Model selection request dropped")?
}

pub(super) async fn cancel_prompt(daemon: &DaemonHandle, thread_id: i32) -> Result<()> {
    daemon.cancel_session(thread_id).await
}
