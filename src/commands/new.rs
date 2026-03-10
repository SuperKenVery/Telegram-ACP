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

    fn description(&self) -> &'static str {
        "Create a new session topic (optional agent)"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let project_path = ctx
            .daemon
            .get_session_project_path_by_thread(ctx.thread_id)
            .ok_or_else(|| anyhow!("No active session in this topic"))?;
        let agent = parse_agent_arg(ctx.args)?;

        match ctx
            .daemon
            .spawn_session(project_path.to_string_lossy().to_string(), None, agent)
            .await
        {
            Ok((acp_session_id, _thread_id)) => {
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

fn parse_agent_arg(args: &str) -> Result<Option<String>> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut parts = trimmed.split_whitespace();
    let agent = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        anyhow::bail!("Usage: /new [agent]");
    }

    Ok(Some(agent.to_string()))
}
