use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ThreadId,
};

use crate::daemon::DaemonHandle;

use super::{get_control_state, set_permission_mode, Command, CommandContext};

const CB_PREFIX: &str = "permission";

pub struct PermissionCommand;

#[async_trait]
impl Command for PermissionCommand {
    fn name(&self) -> &'static str {
        "permission"
    }

    fn description(&self) -> &'static str {
        "Choose the permission mode"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let state = get_control_state(ctx.daemon, ctx.thread_id).await?;
        if state.permission_modes.is_empty() {
            ctx.bot
                .send_message(
                    ctx.msg.chat.id,
                    "This agent session does not expose permission modes.",
                )
                .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                .await?;
            return Ok(());
        }

        let current = state.current_permission_mode_id.as_deref();
        let mut rows = Vec::new();
        for mode in &state.permission_modes {
            let selected = Some(mode.id.as_str()) == current;
            let label = if selected {
                format!("* {}", mode.name)
            } else {
                mode.name.clone()
            };

            if let Some(data) = encode_callback(ctx.thread_id, &mode.id) {
                rows.push(vec![InlineKeyboardButton::callback(label, data)]);
            }
        }

        if rows.is_empty() {
            ctx.bot
                .send_message(
                    ctx.msg.chat.id,
                    "Permission modes are available, but identifiers are too long for Telegram callback payloads.",
                )
                .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                .await?;
            return Ok(());
        }

        let keyboard = InlineKeyboardMarkup::new(rows);
        ctx.bot
            .send_message(ctx.msg.chat.id, "Select permission mode:")
            .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
            .reply_markup(keyboard)
            .await?;

        Ok(())
    }
}

fn encode_callback(thread_id: i32, mode_id: &str) -> Option<String> {
    let data = format!("{CB_PREFIX}:{thread_id}:{mode_id}");
    (data.len() <= 64).then_some(data)
}

fn parse_callback(data: &str) -> Option<(i32, String)> {
    let rest = data.strip_prefix(&format!("{CB_PREFIX}:"))?;
    let mut parts = rest.splitn(2, ':');
    let thread_id = parts.next()?.parse::<i32>().ok()?;
    let mode_id = parts.next()?.to_string();
    Some((thread_id, mode_id))
}

pub async fn try_handle_callback(
    bot: &Bot,
    query: &CallbackQuery,
    daemon: &Arc<DaemonHandle>,
) -> Result<bool> {
    let Some(data) = query.data.as_deref() else {
        return Ok(false);
    };

    let Some((thread_id, mode_id)) = parse_callback(data) else {
        return Ok(false);
    };

    match set_permission_mode(daemon, thread_id, &mode_id).await {
        Ok(updated) => {
            let current = updated.current_permission_mode_id.as_deref();
            let mode_name = updated
                .permission_modes
                .iter()
                .find(|mode| Some(mode.id.as_str()) == current)
                .map(|mode| mode.name.clone())
                .unwrap_or_else(|| "updated".to_string());

            bot.answer_callback_query(query.id.clone())
                .text(format!("Permission mode set to {mode_name}"))
                .await?;
        }
        Err(e) => {
            bot.answer_callback_query(query.id.clone())
                .text(format!("Failed: {e}"))
                .show_alert(true)
                .await?;
        }
    }

    Ok(true)
}
