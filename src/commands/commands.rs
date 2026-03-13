use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ParseMode, ThreadId};

use crate::formatting;

use super::{Command, CommandContext};

pub struct CommandsCommand;

#[async_trait]
impl Command for CommandsCommand {
    fn name(&self) -> &'static str {
        "commands"
    }

    fn description(&self) -> &'static str {
        "List agent slash commands in this session"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;
        let Some(commands) = ctx
            .daemon
            .get_available_commands_by_thread(thread_id)
            .await
        else {
            ctx.bot
                .send_message(ctx.msg.chat.id, "No active session in this topic.")
                .message_thread_id(ThreadId(MessageId(thread_id)))
                .await?;
            return Ok(());
        };

        ctx.bot
            .send_message(
                ctx.msg.chat.id,
                formatting::format_available_commands(&commands),
            )
            .message_thread_id(ThreadId(MessageId(thread_id)))
            .parse_mode(ParseMode::MarkdownV2)
            .await?;

        Ok(())
    }
}
