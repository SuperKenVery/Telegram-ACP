use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{CallbackQuery, MessageId, ThreadId};

use crate::daemon::DaemonHandle;

use super::{cancel_prompt, Command, CommandContext};

const CB_PREFIX: &str = "cancelq";

pub struct CancelCommand;

#[async_trait]
impl Command for CancelCommand {
    fn name(&self) -> &'static str {
        "cancel"
    }

    fn description(&self) -> &'static str {
        "Cancel the current running prompt"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;
        cancel_prompt(ctx.daemon, thread_id).await?;
        ctx.bot
            .send_message(
                ctx.msg.chat.id,
                "Interrupt requested. The current run is being cancelled.",
            )
            .message_thread_id(ThreadId(MessageId(thread_id)))
            .await?;
        Ok(())
    }
}

fn parse_callback(data: &str) -> Option<i32> {
    let thread = data.strip_prefix(&format!("{CB_PREFIX}:"))?;
    thread.parse::<i32>().ok()
}

pub async fn try_handle_callback(
    bot: &Bot,
    query: &CallbackQuery,
    daemon: &Arc<DaemonHandle>,
) -> Result<bool> {
    let Some(data) = query.data.as_deref() else {
        return Ok(false);
    };
    let Some(thread_id) = parse_callback(data) else {
        return Ok(false);
    };

    match cancel_prompt(daemon, thread_id).await {
        Ok(()) => {
            bot.answer_callback_query(query.id.clone())
                .text("Interrupt requested")
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
