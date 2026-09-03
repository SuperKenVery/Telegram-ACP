use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ParseMode, ThreadId};

use crate::types::VerboseMode;

use super::{Command, CommandContext};

pub struct VerboseCommand;

#[async_trait]
impl Command for VerboseCommand {
    fn name(&self) -> &'static str {
        "verbose"
    }

    fn description(&self) -> &'static str {
        "Set tool message verbosity: /verbose [off|compact|on]"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;
        let arg = ctx.args.trim();

        if arg.is_empty() {
            let mode = ctx
                .daemon
                .get_thread_verbose(thread_id)
                .await
                .ok_or_else(|| anyhow::anyhow!("No active session in this topic"))?;
            send_reply(ctx, &format!("Verbose mode: <code>{mode}</code>")).await?;
            return Ok(());
        }

        let Some(mode) = parse_mode(arg) else {
            send_reply(ctx, "Usage: /verbose [off|compact|on]").await?;
            return Ok(());
        };

        ctx.daemon.set_thread_verbose(thread_id, mode).await?;
        send_reply(ctx, &format!("Verbose mode set to <code>{mode}</code>.")).await?;
        Ok(())
    }
}

fn parse_mode(arg: &str) -> Option<VerboseMode> {
    match arg.to_ascii_lowercase().as_str() {
        "off" => Some(VerboseMode::Off),
        "compact" => Some(VerboseMode::Compact),
        "on" => Some(VerboseMode::On),
        _ => None,
    }
}

async fn send_reply(ctx: CommandContext<'_>, text: &str) -> Result<()> {
    let thread_id = ctx.require_thread_id()?;
    ctx.bot
        .send_message(ctx.msg.chat.id, text)
        .message_thread_id(ThreadId(MessageId(thread_id)))
        .parse_mode(ParseMode::Html)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_mode, VerboseMode};

    #[test]
    fn parses_supported_modes() {
        assert_eq!(parse_mode("off"), Some(VerboseMode::Off));
        assert_eq!(parse_mode("compact"), Some(VerboseMode::Compact));
        assert_eq!(parse_mode("ON"), Some(VerboseMode::On));
        assert_eq!(parse_mode("verbose"), None);
    }
}
