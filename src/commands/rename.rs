use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ThreadId};

use super::{Command, CommandContext};

pub struct RenameCommand;

#[async_trait]
impl Command for RenameCommand {
    fn name(&self) -> &'static str {
        "rename"
    }

    fn description(&self) -> &'static str {
        "Rename this topic"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let new_name = ctx.args.trim();
        if new_name.is_empty() {
            ctx.bot
                .send_message(ctx.msg.chat.id, "Usage: /rename <new topic name>")
                .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                .await?;
            return Ok(());
        }

        ctx.bot
            .edit_forum_topic(ctx.msg.chat.id, ThreadId(MessageId(ctx.thread_id)))
            .name(new_name)
            .await?;

        ctx.bot
            .send_message(ctx.msg.chat.id, format!("Topic renamed to: {new_name}"))
            .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
            .await?;

        Ok(())
    }
}
