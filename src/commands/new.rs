use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::env;
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
        "Create or replace session: /new [agent] [project_path]"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let parsed = parse_new_args(ctx.args, &ctx.daemon.config.agents)?;

        match ctx.thread_id {
            Some(thread_id) => {
                // In a topic: replace current session, reuse topic
                let project_path = if let Some(raw_path) = parsed.project_path {
                    absolutize_project_path(PathBuf::from(raw_path))?
                } else {
                    let existing_path = ctx
                        .daemon
                        .get_session_project_path_by_thread(thread_id)
                        .ok_or_else(|| anyhow!("No active session in this topic; provide a path: /new [agent] <project_path>"))?;
                    absolutize_project_path(existing_path)?
                };

                match ctx
                    .daemon
                    .replace_session_in_thread(thread_id, project_path, parsed.agent)
                    .await
                {
                    Ok(acp_session_id) => {
                        let reply_markdown = format!("Session `{acp_session_id}` started in this topic.");
                        ctx.bot
                            .send_message(
                                ctx.msg.chat.id,
                                formatting::markdown_to_telegram_md_v2(&reply_markdown),
                            )
                            .message_thread_id(ThreadId(MessageId(thread_id)))
                            .parse_mode(ParseMode::MarkdownV2)
                            .await?;
                    }
                    Err(e) => {
                        ctx.bot
                            .send_message(
                                ctx.msg.chat.id,
                                format!("Failed to create session: {e}"),
                            )
                            .message_thread_id(ThreadId(MessageId(thread_id)))
                            .await?;
                    }
                }
            }
            None => {
                // No topic: create new topic (requires explicit path)
                let project_path = parsed
                    .project_path
                    .ok_or_else(|| anyhow!("Provide a project path: /new [agent] <project_path>"))?;
                let project_path = absolutize_project_path(PathBuf::from(project_path))?;

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
                        let reply_markdown = format!("Session `{acp_session_id}` created in a new topic.");
                        ctx.bot
                            .send_message(
                                ctx.msg.chat.id,
                                formatting::markdown_to_telegram_md_v2(&reply_markdown),
                            )
                            .parse_mode(ParseMode::MarkdownV2)
                            .await?;
                    }
                    Err(e) => {
                        ctx.bot
                            .send_message(
                                ctx.msg.chat.id,
                                format!("Failed to create session: {e}"),
                            )
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }
}

fn absolutize_project_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(env::current_dir()?.join(path))
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
