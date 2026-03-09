use anyhow::{anyhow, Result};
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ParseMode, ThreadId};

use crate::formatting;

use super::{Command, CommandContext};

pub struct NewCommand;

#[async_trait]
impl Command for NewCommand {
    fn name(&self) -> &'static str {
        "new"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let project_path = ctx
            .daemon
            .get_session_project_path_by_thread(ctx.thread_id)
            .ok_or_else(|| anyhow!("No active session in this topic"))?;

        match ctx
            .daemon
            .spawn_session(project_path.to_string_lossy().to_string(), None, None)
            .await
        {
            Ok((acp_session_id, _new_thread_id)) => {
                let reply = format!(
                    "Session `{}` created in a new topic\\.",
                    formatting::escape_markdown_v2(&acp_session_id)
                );
                ctx.bot
                    .send_message(ctx.msg.chat.id, reply)
                    .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                    .parse_mode(ParseMode::MarkdownV2)
                    .await?;
            }
            Err(e) => {
                ctx.bot
                    .send_message(ctx.msg.chat.id, format!("Failed to create session: {e}"))
                    .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                    .await?;
            }
        }

        Ok(())
    }
}
