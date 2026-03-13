use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ThreadId};

use crate::daemon::DaemonHandle;

use super::{Command, CommandContext};

const CB_PREFIX: &str = "switch";

pub struct SwitchCommand;

#[async_trait]
impl Command for SwitchCommand {
    fn name(&self) -> &'static str {
        "switch"
    }

    fn description(&self) -> &'static str {
        "Switch to a previous session in this topic"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;

        let topic = ctx.daemon.topics.get(&thread_id);
        let history = match &topic {
            Some(entry) => entry.history.clone(),
            None => {
                ctx.bot
                    .send_message(ctx.msg.chat.id, "No session history for this topic.")
                    .message_thread_id(ThreadId(MessageId(thread_id)))
                    .await?;
                return Ok(());
            }
        };

        if history.is_empty() {
            ctx.bot
                .send_message(ctx.msg.chat.id, "No session history for this topic.")
                .message_thread_id(ThreadId(MessageId(thread_id)))
                .await?;
            return Ok(());
        }

        let active_id = topic
            .as_ref()
            .and_then(|e| e.active.as_ref())
            .map(|s| s.acp_session_id.as_str());

        let mut rows = Vec::new();
        for (i, record) in history.iter().enumerate() {
            let is_active = Some(record.acp_session_id.as_str()) == active_id;
            let label = format!(
                "{}{} | {} | {}",
                if is_active { "* " } else { "" },
                record.agent_command,
                record.project_path.display(),
                record.created_at.format("%Y-%m-%d %H:%M"),
            );
            let data = format!("{CB_PREFIX}:{thread_id}:{i}");
            if data.len() <= 64 {
                rows.push(vec![InlineKeyboardButton::callback(label, data)]);
            }
        }

        if rows.is_empty() {
            ctx.bot
                .send_message(
                    ctx.msg.chat.id,
                    "Session identifiers are too long for Telegram callback payloads.",
                )
                .message_thread_id(ThreadId(MessageId(thread_id)))
                .await?;
            return Ok(());
        }

        let keyboard = InlineKeyboardMarkup::new(rows);
        ctx.bot
            .send_message(ctx.msg.chat.id, "Select session:")
            .message_thread_id(ThreadId(MessageId(thread_id)))
            .reply_markup(keyboard)
            .await?;

        Ok(())
    }
}

fn parse_callback(data: &str) -> Option<(i32, usize)> {
    let rest = data.strip_prefix(&format!("{CB_PREFIX}:"))?;
    let mut parts = rest.splitn(2, ':');
    let thread_id = parts.next()?.parse::<i32>().ok()?;
    let index = parts.next()?.parse::<usize>().ok()?;
    Some((thread_id, index))
}

pub async fn try_handle_callback(
    bot: &Bot,
    query: &CallbackQuery,
    daemon: &Arc<DaemonHandle>,
) -> Result<bool> {
    let Some(data) = query.data.as_deref() else {
        return Ok(false);
    };
    let Some((thread_id, index)) = parse_callback(data) else {
        return Ok(false);
    };

    // Look up the history record
    let record = {
        let Some(topic) = daemon.topics.get(&thread_id) else {
            bot.answer_callback_query(query.id.clone())
                .text("Topic no longer exists")
                .show_alert(true)
                .await?;
            return Ok(true);
        };

        let Some(record) = topic.history.get(index) else {
            bot.answer_callback_query(query.id.clone())
                .text("Session not found")
                .show_alert(true)
                .await?;
            return Ok(true);
        };

        // Check if already active
        if topic
            .active
            .as_ref()
            .map(|s| s.acp_session_id == record.acp_session_id)
            .unwrap_or(false)
        {
            bot.answer_callback_query(query.id.clone())
                .text("Already active")
                .await?;
            return Ok(true);
        }

        record.clone()
    };

    match daemon.switch_to_session(thread_id, &record).await {
        Ok(new_id) => {
            bot.answer_callback_query(query.id.clone())
                .text(format!("Switched to session {}", &new_id[..8.min(new_id.len())]))
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
