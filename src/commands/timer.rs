use anyhow::{anyhow, Result};
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ThreadId};
use tokio::time::{sleep, Duration};

use super::{Command, CommandContext};

const MAX_REPEAT: usize = 10;

pub struct TimerCommand;

#[async_trait]
impl Command for TimerCommand {
    fn name(&self) -> &'static str {
        "timer"
    }

    fn description(&self) -> &'static str {
        "Schedule a prompt: /timer <interval> <prompt> [--repeat [n]]"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;
        let args = parse_timer_args(ctx.args)?;
        let session = ctx
            .daemon
            .get_session_by_thread(thread_id)
            .ok_or_else(|| anyhow!("No active session in this topic"))?;

        let prompt = args.prompt.clone();
        let repeat = args.repeat;
        let interval = args.interval;

        tokio::task::spawn_local(async move {
            for _ in 0..repeat {
                sleep(interval).await;
                session.send_prompt(prompt.clone());
            }
        });

        let summary = if repeat == 1 {
            format!(
                "Timer scheduled: 1 prompt queued after {}.",
                args.interval_label
            )
        } else {
            format!(
                "Timer scheduled: {repeat} prompts queued every {}.",
                args.interval_label
            )
        };

        ctx.bot
            .send_message(ctx.msg.chat.id, summary)
            .message_thread_id(ThreadId(MessageId(thread_id)))
            .await?;

        Ok(())
    }
}

struct TimerArgs {
    interval: Duration,
    interval_label: String,
    prompt: String,
    repeat: usize,
}

fn parse_timer_args(args: &str) -> Result<TimerArgs> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        anyhow::bail!("Usage: /timer <interval> <prompt> [--repeat [n]]");
    }

    let mut parts = trimmed.split_whitespace();
    let interval_token = parts
        .next()
        .ok_or_else(|| anyhow!("Usage: /timer <interval> <prompt> [--repeat [n]]"))?;
    let interval = parse_interval(interval_token)?;
    let interval_label = interval_token.to_string();

    let rest = parts.collect::<Vec<_>>().join(" ");
    if rest.trim().is_empty() {
        anyhow::bail!("Usage: /timer <interval> <prompt> [--repeat [n]]");
    }

    let mut repeat: Option<usize> = None;
    let mut prompt_parts: Vec<&str> = Vec::new();
    let mut iter = rest.split_whitespace().peekable();

    while let Some(token) = iter.next() {
        if let Some(value) = token.strip_prefix("--repeat=") {
            if repeat.is_some() {
                anyhow::bail!("Repeat specified more than once");
            }
            let parsed = value
                .parse::<usize>()
                .map_err(|_| anyhow!("Repeat must be a number between 1 and {MAX_REPEAT}"))?;
            repeat = Some(parsed);
            continue;
        }

        if token == "--repeat" {
            if repeat.is_some() {
                anyhow::bail!("Repeat specified more than once");
            }
            if let Some(next) = iter.peek().copied() {
                if next.chars().all(|c| c.is_ascii_digit()) {
                    let parsed = next.parse::<usize>().map_err(|_| {
                        anyhow!("Repeat must be a number between 1 and {MAX_REPEAT}")
                    })?;
                    iter.next();
                    repeat = Some(parsed);
                    continue;
                }
            }
            repeat = Some(MAX_REPEAT);
            continue;
        }

        prompt_parts.push(token);
    }

    if prompt_parts.is_empty() {
        anyhow::bail!("Usage: /timer <interval> <prompt> [--repeat [n]]");
    }

    let repeat = repeat.unwrap_or(1);
    if repeat == 0 || repeat > MAX_REPEAT {
        anyhow::bail!("Repeat must be between 1 and {MAX_REPEAT}");
    }

    Ok(TimerArgs {
        interval,
        interval_label,
        prompt: prompt_parts.join(" "),
        repeat,
    })
}

fn parse_interval(raw: &str) -> Result<Duration> {
    if raw.is_empty() {
        anyhow::bail!("Interval must be like 10m, 2h, or 2h30m");
    }

    let mut total_minutes: u64 = 0;
    let mut number = String::new();

    for ch in raw.chars() {
        if ch.is_ascii_digit() {
            number.push(ch);
            continue;
        }

        if number.is_empty() {
            anyhow::bail!("Interval must be like 10m, 2h, or 2h30m");
        }

        let value = number
            .parse::<u64>()
            .map_err(|_| anyhow!("Interval must be like 10m, 2h, or 2h30m"))?;
        number.clear();

        match ch {
            'h' => total_minutes = total_minutes.saturating_add(value.saturating_mul(60)),
            'm' => total_minutes = total_minutes.saturating_add(value),
            _ => anyhow::bail!("Only 'h' and 'm' are supported in intervals"),
        }
    }

    if !number.is_empty() {
        anyhow::bail!("Interval must end with 'h' or 'm'");
    }

    if total_minutes == 0 {
        anyhow::bail!("Interval must be greater than zero");
    }

    Ok(Duration::from_secs(total_minutes.saturating_mul(60)))
}

#[cfg(test)]
mod tests {
    use super::parse_timer_args;

    #[test]
    fn parses_default_repeat() {
        let args = parse_timer_args("2h hello world").unwrap();
        assert_eq!(args.prompt, "hello world");
        assert_eq!(args.repeat, 1);
    }

    #[test]
    fn parses_repeat_with_value() {
        let args = parse_timer_args("30m hello --repeat 4").unwrap();
        assert_eq!(args.prompt, "hello");
        assert_eq!(args.repeat, 4);
    }

    #[test]
    fn parses_repeat_without_value_as_max() {
        let args = parse_timer_args("30m hello --repeat").unwrap();
        assert_eq!(args.repeat, 10);
    }

    #[test]
    fn parses_repeat_equals_syntax() {
        let args = parse_timer_args("1h30m hello --repeat=3").unwrap();
        assert_eq!(args.repeat, 3);
    }
}
