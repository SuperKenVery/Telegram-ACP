use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
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
        "Create new session: /new [agent] [project_path]"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let parsed = parse_new_args(ctx.args, &ctx.daemon.config.agents)?;
        let project_path = if let Some(path) = parsed.project_path {
            PathBuf::from(path)
        } else {
            ctx.daemon
                .get_session_project_path_by_thread(ctx.thread_id)
                .ok_or_else(|| anyhow!("No active session in this topic; provide a path: /new [agent] <project_path>"))?
        };

        match ctx
            .daemon
            .spawn_session(
                project_path.to_string_lossy().to_string(),
                None,
                parsed.agent,
            )
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

struct NewArgs {
    agent: Option<String>,
    project_path: Option<String>,
}

fn parse_new_args(args: &str, configured_agents: &HashMap<String, String>) -> Result<NewArgs> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return Ok(NewArgs {
            agent: None,
            project_path: None,
        });
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let first = parts.next().unwrap_or_default().trim();
    let second = parts.next().map(str::trim).filter(|s| !s.is_empty());

    if configured_agents.contains_key(first) {
        return Ok(NewArgs {
            agent: Some(first.to_string()),
            project_path: second.map(ToOwned::to_owned),
        });
    }

    if second.is_some() {
        anyhow::bail!(
            "Unknown agent '{}'. Usage: /new [agent] [project_path], or /new <project_path>",
            first
        );
    }

    Ok(NewArgs {
        agent: None,
        project_path: Some(trimmed.to_string()),
    })
}
