use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{BotCommand, CallbackQuery, Message};

use crate::daemon::DaemonHandle;
use crate::session_control::SessionControlState;

mod cancel;
mod command;
mod commands;
mod stop_daemon;
mod model;
mod new;
mod permission;
mod remove;
mod rename;
mod status;
mod switch;
mod timer;

use cancel::CancelCommand;
use command::CommandCommand;
use commands::CommandsCommand;
use stop_daemon::StopDaemonCommand;
use model::ModelCommand;
use new::NewCommand;
use permission::PermissionCommand;
use remove::RemoveCommand;
use rename::RenameCommand;
use status::StatusCommand;
use switch::SwitchCommand;
use timer::TimerCommand;

pub struct CommandContext<'a> {
    pub bot: &'a Bot,
    pub msg: &'a Message,
    pub daemon: &'a DaemonHandle,
    pub thread_id: Option<i32>,
    pub args: &'a str,
}

impl CommandContext<'_> {
    pub fn require_thread_id(&self) -> Result<i32> {
        self.thread_id
            .ok_or_else(|| anyhow!("This command must be used inside a topic"))
    }
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
        Box::new(CommandsCommand),
        Box::new(CommandCommand),
        Box::new(TimerCommand),
        Box::new(StatusCommand),
        Box::new(SwitchCommand),
        Box::new(StopDaemonCommand),
    ]
}

#[cfg(test)]
fn has_registered_command(name: &str) -> bool {
    command_registry()
        .into_iter()
        .any(|command| command.name() == name)
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

    let thread_id = msg.thread_id.map(|id| id.0 .0);

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

    if switch::try_handle_callback(&bot, &query, &daemon).await? {
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
    let entry = daemon
        .topics
        .get(&thread_id)
        .and_then(|t| t.active.as_ref().map(|s| s.control_state.clone()))
        .ok_or_else(|| anyhow!("No active session in this topic"))?;
    let state = entry.lock().await.clone();
    Ok(state)
}

pub(super) async fn set_permission_mode(
    daemon: &DaemonHandle,
    thread_id: i32,
    mode_id: &str,
) -> Result<SessionControlState> {
    let (command_tx, control_state) = daemon
        .topics
        .get(&thread_id)
        .and_then(|t| {
            t.active
                .as_ref()
                .map(|s| (s.command_tx.clone(), s.control_state.clone()))
        })
        .ok_or_else(|| anyhow!("No active session in this topic"))?;

    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    command_tx
        .send(crate::session_control::SessionCommand::SetPermissionMode {
            mode_id: mode_id.to_string(),
            result_tx,
        })
        .map_err(|_| anyhow!("Session command channel closed"))?;
    result_rx
        .await
        .map_err(|_| anyhow!("Permission mode request dropped"))??;

    let state = control_state.lock().await.clone();
    Ok(state)
}

pub(super) async fn set_config_option(
    daemon: &DaemonHandle,
    thread_id: i32,
    config_id: &str,
    value_id: &str,
) -> Result<SessionControlState> {
    let (command_tx, control_state) = daemon
        .topics
        .get(&thread_id)
        .and_then(|t| {
            t.active
                .as_ref()
                .map(|s| (s.command_tx.clone(), s.control_state.clone()))
        })
        .ok_or_else(|| anyhow!("No active session in this topic"))?;

    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    command_tx
        .send(crate::session_control::SessionCommand::SetConfigOption {
            config_id: config_id.to_string(),
            value_id: value_id.to_string(),
            result_tx,
        })
        .map_err(|_| anyhow!("Session command channel closed"))?;
    result_rx
        .await
        .map_err(|_| anyhow!("Config option request dropped"))??;

    let state = control_state.lock().await.clone();
    Ok(state)
}

pub(super) async fn cancel_prompt(daemon: &DaemonHandle, thread_id: i32) -> Result<()> {
    daemon.cancel_session(thread_id).await
}

#[cfg(test)]
mod tests {
    use super::{has_registered_command, parse_command};

    #[test]
    fn parse_slash_command_token_and_args() {
        assert_eq!(
            parse_command("/new /tmp/project"),
            Some(("new".to_string(), "/tmp/project".to_string()))
        );
    }

    #[test]
    fn parse_ignores_non_slash_text() {
        assert_eq!(parse_command("hello"), None);
    }

    #[test]
    fn registry_contains_new_agent_commands() {
        assert!(has_registered_command("commands"));
        assert!(has_registered_command("command"));
        assert!(has_registered_command("new"));
        assert!(has_registered_command("timer"));
        assert!(has_registered_command("status"));
        assert!(has_registered_command("switch"));
        assert!(!has_registered_command("missing"));
    }
}
