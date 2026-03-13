use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ThreadId};

use super::{Command, CommandContext};

pub struct RemoveCommand;

#[async_trait]
impl Command for RemoveCommand {
    fn name(&self) -> &'static str {
        "remove"
    }

    fn description(&self) -> &'static str {
        "Delete this topic and remove its saved sessions"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;

        if !ctx.args.trim().is_empty() {
            ctx.bot
                .send_message(ctx.msg.chat.id, "Usage: /remove")
                .message_thread_id(ThreadId(MessageId(thread_id)))
                .await?;
            return Ok(());
        }

        ctx.bot
            .delete_forum_topic(ctx.msg.chat.id, ThreadId(MessageId(thread_id)))
            .await?;

        let removed = ctx.daemon.remove_topic(thread_id).await.is_some();

        let summary = if removed {
            "Topic deleted. All sessions removed."
        } else {
            "Topic deleted."
        };
        ctx.bot.send_message(ctx.msg.chat.id, summary).await?;

        Ok(())
    }
}
