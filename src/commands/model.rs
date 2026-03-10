use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ThreadId,
};

use crate::daemon::DaemonHandle;

use super::{get_control_state, set_config_option, Command, CommandContext};

const CB_PREFIX: &str = "model";

pub struct ModelCommand;

#[async_trait]
impl Command for ModelCommand {
    fn name(&self) -> &'static str {
        "model"
    }

    fn description(&self) -> &'static str {
        "Choose the model for this session"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let state = get_control_state(ctx.daemon, ctx.thread_id).await?;
        let selector = match state.model_selector {
            Some(selector) => selector,
            None => {
                ctx.bot
                    .send_message(
                        ctx.msg.chat.id,
                        "This agent session does not expose a model selector.",
                    )
                    .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                    .await?;
                return Ok(());
            }
        };

        let mut rows = Vec::new();
        for option in &selector.options {
            let selected = option.id == selector.current_value_id;
            let label = if selected {
                format!("* {}", option.name)
            } else {
                option.name.clone()
            };

            if let Some(data) = encode_callback(ctx.thread_id, &selector.config_id, &option.id) {
                rows.push(vec![InlineKeyboardButton::callback(label, data)]);
            }
        }

        if rows.is_empty() {
            ctx.bot
                .send_message(
                    ctx.msg.chat.id,
                    "Model selector is available, but option identifiers are too long for Telegram callback payloads.",
                )
                .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                .await?;
            return Ok(());
        }

        let keyboard = InlineKeyboardMarkup::new(rows);
        ctx.bot
            .send_message(ctx.msg.chat.id, "Select model:")
            .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
            .reply_markup(keyboard)
            .await?;

        Ok(())
    }
}

fn encode_callback(thread_id: i32, config_id: &str, value_id: &str) -> Option<String> {
    let data = format!("{CB_PREFIX}:{thread_id}:{config_id}:{value_id}");
    (data.len() <= 64).then_some(data)
}

fn parse_callback(data: &str) -> Option<(i32, String, String)> {
    let rest = data.strip_prefix(&format!("{CB_PREFIX}:"))?;
    let mut parts = rest.splitn(3, ':');
    let thread_id = parts.next()?.parse::<i32>().ok()?;
    let config_id = parts.next()?.to_string();
    let value_id = parts.next()?.to_string();
    Some((thread_id, config_id, value_id))
}

pub async fn try_handle_callback(
    bot: &Bot,
    query: &CallbackQuery,
    daemon: &Arc<DaemonHandle>,
) -> Result<bool> {
    let Some(data) = query.data.as_deref() else {
        return Ok(false);
    };

    let Some((thread_id, config_id, value_id)) = parse_callback(data) else {
        return Ok(false);
    };

    match set_config_option(daemon, thread_id, &config_id, &value_id).await {
        Ok(updated) => {
            let model_name = updated
                .model_selector
                .as_ref()
                .and_then(|selector| {
                    selector
                        .options
                        .iter()
                        .find(|opt| opt.id == selector.current_value_id)
                })
                .map(|opt| opt.name.clone())
                .unwrap_or_else(|| "updated".to_string());

            bot.answer_callback_query(query.id.clone())
                .text(format!("Model set to {model_name}"))
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
