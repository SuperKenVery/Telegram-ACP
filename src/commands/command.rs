use anyhow::{Context, Result};
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ThreadId};

use crate::session_control::SessionCommand;

use super::{Command, CommandContext};

pub struct CommandCommand;

#[async_trait]
impl Command for CommandCommand {
    fn name(&self) -> &'static str {
        "command"
    }

    fn description(&self) -> &'static str {
        "Run agent slash command: /command <name> [args]"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let Some((command_name, args)) = parse_args(ctx.args) else {
            ctx.bot
                .send_message(ctx.msg.chat.id, "Usage: /command <name> [args]")
                .message_thread_id(ThreadId(MessageId(ctx.thread_id)))
                .await?;
            return Ok(());
        };

        let prompt = if args.is_empty() {
            format!("/{command_name}")
        } else {
            format!("/{command_name} {args}")
        };

        let command_tx = ctx
            .daemon
            .get_session_command_tx_by_thread(ctx.thread_id)
            .context("No active session in this topic")?;
        command_tx
            .send(SessionCommand::Prompt(prompt))
            .map_err(|_| anyhow::anyhow!("Session command channel closed"))?;

        Ok(())
    }
}

fn parse_args(args: &str) -> Option<(String, String)> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let raw_name = parts.next()?.trim();
    let rest = parts.next().unwrap_or("").trim().to_string();
    let command_name = raw_name
        .trim_start_matches('/')
        .split('@')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();

    if command_name.is_empty() {
        return None;
    }

    Some((command_name, rest))
}

#[cfg(test)]
mod tests {
    use super::parse_args;

    #[test]
    fn parse_args_supports_optional_leading_slash() {
        assert_eq!(
            parse_args("new path"),
            Some(("new".to_string(), "path".to_string()))
        );
        assert_eq!(
            parse_args("/new path"),
            Some(("new".to_string(), "path".to_string()))
        );
    }

    #[test]
    fn parse_args_handles_empty() {
        assert_eq!(parse_args(""), None);
        assert_eq!(parse_args("   "), None);
        assert_eq!(parse_args("/"), None);
    }
}
