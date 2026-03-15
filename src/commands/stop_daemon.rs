use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;

use super::{Command, CommandContext};

pub struct StopDaemonCommand;

#[async_trait]
impl Command for StopDaemonCommand {
    fn name(&self) -> &'static str {
        "stop_daemon"
    }

    fn description(&self) -> &'static str {
        "Shut down the daemon"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        ctx.bot
            .send_message(ctx.msg.chat.id, "Shutting down daemon.")
            .await?;
        std::process::exit(0);
    }
}
