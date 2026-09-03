use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;

use super::{Command, CommandContext};

pub struct StatusCommand;

#[async_trait]
impl Command for StatusCommand {
    fn name(&self) -> &'static str {
        "status"
    }

    fn description(&self) -> &'static str {
        "Show current session status"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;

        let topic = ctx
            .daemon
            .topics
            .get(&thread_id)
            .ok_or_else(|| anyhow::anyhow!("No topic found for this thread"))?;

        let active = topic
            .active
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No active session in this topic"))?;

        let agent_name = active.agent_name.as_deref().unwrap_or("unknown");
        let agent_command = &active.agent_command;
        let working_dir = active.project_path.display();

        let log_dir = active.session_log.log_dir().display().to_string();
        let verbose = *topic.verbose.lock().await;

        let status_text = format!(
            "<b>Session Status</b>\n\n\
            <b>Agent:</b> {}\n\
            <b>Command:</b> <code>{}</code>\n\
            <b>Working Directory:</b> <code>{}</code>\n\
            <b>Log Directory:</b> <code>{}</code>\n\
            <b>Verbose:</b> <code>{}</code>",
            agent_name, agent_command, working_dir, log_dir, verbose
        );

        ctx.bot
            .send_message(ctx.msg.chat.id, status_text)
            .message_thread_id(teloxide::types::ThreadId(teloxide::types::MessageId(
                thread_id,
            )))
            .parse_mode(teloxide::types::ParseMode::Html)
            .await?;

        Ok(())
    }
}
