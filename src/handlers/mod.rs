//! Handlers for ACP events/Agent updates

pub mod draft;
pub mod plan;
pub mod tool_call;
pub mod working;

use std::future::Future;

use teloxide::prelude::*;
use teloxide::types::{MessageId, ThreadId};
use tokio::time::{sleep_until, Duration, Instant};

use crate::sess_warn;
use crate::types::AgentEvent;
use crate::{formatting, telegram_rich};

// --- OutboundThrottle (moved from session.rs) ---

pub struct OutboundThrottle {
    min_interval: Duration,
    next_allowed_at: Instant,
}

impl OutboundThrottle {
    pub fn with_interval(secs: f64) -> Self {
        let min_interval = Duration::from_secs_f64(secs);
        Self {
            min_interval,
            next_allowed_at: Instant::now(),
        }
    }

    pub async fn wait_turn(&mut self) {
        let now = Instant::now();
        if self.next_allowed_at > now {
            sleep_until(self.next_allowed_at).await;
        }
        self.next_allowed_at = Instant::now() + self.min_interval;
    }

    pub fn try_turn(&mut self) -> bool {
        let now = Instant::now();
        if self.next_allowed_at > now {
            return false;
        }
        self.next_allowed_at = now + self.min_interval;
        true
    }

    pub fn defer_for(&mut self, delay: Duration) {
        let retry_at = Instant::now() + delay;
        if retry_at > self.next_allowed_at {
            self.next_allowed_at = retry_at;
        }
    }
}

// --- EventContext ---

/// Shared context passed to all handlers for sending Telegram messages.
pub struct EventContext {
    pub bot: Bot,
    pub chat_id: ChatId,
    pub thread_id: i32,
    pub throttle: OutboundThrottle,
}

impl EventContext {
    const RETRY_AFTER_PADDING: Duration = Duration::from_secs(1);

    pub fn new(bot: Bot, chat_id: ChatId, thread_id: i32) -> Self {
        Self {
            bot,
            chat_id,
            thread_id,
            throttle: OutboundThrottle::with_interval(2.0),
        }
    }

    async fn request_with_throttle<T, F, Fut>(
        &mut self,
        label: &str,
        mut request: F,
    ) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        loop {
            self.throttle.wait_turn().await;
            match request().await {
                Ok(value) => return Ok(value),
                Err(err)
                    if err
                        .downcast_ref::<teloxide::RequestError>()
                        .is_some_and(|e| matches!(e, teloxide::RequestError::RetryAfter(_))) =>
                {
                    let teloxide::RequestError::RetryAfter(after) =
                        err.downcast_ref::<teloxide::RequestError>().unwrap()
                    else {
                        unreachable!()
                    };
                    let delay = after.duration() + Self::RETRY_AFTER_PADDING;
                    self.throttle.defer_for(delay);
                    sess_warn!(
                        "Telegram rate limit while {label}; backing off for {}s before retrying",
                        delay.as_secs()
                    );
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn send_markdown(&mut self, text: &str, silent: bool) -> Option<Message> {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        let thread_id = self.thread_id;
        let text = text.to_string();
        match self
            .request_with_throttle("sending rich message to Telegram", move || {
                let bot = bot.clone();
                let text = text.clone();
                async move {
                    telegram_rich::send_rich_markdown(&bot, chat_id, thread_id, &text, silent).await
                }
            })
            .await
        {
            Ok(message) => Some(message),
            Err(err) => {
                sess_warn!("Failed to send rich message to Telegram: {err}");
                None
            }
        }
    }

    pub async fn send_markdown_chunks(&mut self, text: &str, silent: bool) {
        for chunk in formatting::split_message(text, 32_768) {
            let _ = self.send_markdown(&chunk, silent).await;
        }
    }

    pub async fn edit_markdown(&mut self, msg_id: MessageId, text: &str) -> bool {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        let text = text.to_string();
        match self
            .request_with_throttle(
                &format!("editing Telegram rich message {}", msg_id.0),
                move || {
                    let bot = bot.clone();
                    let text = text.clone();
                    async move { telegram_rich::edit_rich_markdown(&bot, chat_id, msg_id, &text).await }
                },
            )
            .await
        {
            Ok(_) => true,
            Err(err) => {
                sess_warn!("Failed to edit Telegram rich message {}: {}", msg_id.0, err);
                false
            }
        }
    }

    pub async fn send_chunks(&mut self, text: &str, silent: bool) {
        self.send_markdown_chunks(text, silent).await;
    }

    pub async fn delete_msg(&mut self, msg_id: MessageId) {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        if let Err(err) = self
            .request_with_throttle(
                &format!("deleting Telegram message {}", msg_id.0),
                move || {
                    let bot = bot.clone();
                    async move {
                        bot.delete_message(chat_id, msg_id)
                            .send()
                            .await
                            .map_err(Into::into)
                    }
                },
            )
            .await
        {
            sess_warn!("Failed to delete Telegram message {}: {}", msg_id.0, err);
        }
    }

    pub async fn pin_msg(&mut self, msg_id: MessageId) {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        if let Err(e) = self
            .request_with_throttle(
                &format!("pinning Telegram message {}", msg_id.0),
                move || {
                    let bot = bot.clone();
                    async move {
                        bot.pin_chat_message(chat_id, msg_id)
                            .disable_notification(true)
                            .send()
                            .await
                            .map_err(Into::into)
                    }
                },
            )
            .await
        {
            sess_warn!("Failed to pin Telegram message {}: {}", msg_id.0, e);
        }
    }

    pub async fn close_topic(&mut self) {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        let thread_id = self.thread_id;
        let _ = self
            .request_with_throttle("closing Telegram topic", move || {
                let bot = bot.clone();
                async move {
                    bot.close_forum_topic(chat_id, ThreadId(MessageId(thread_id)))
                        .send()
                        .await
                        .map_err(Into::into)
                }
            })
            .await
            .map_err(|err| sess_warn!("Failed to close Telegram topic: {err}"));
    }
}

// --- EventHandler trait ---

#[async_trait::async_trait(?Send)]
pub trait EventHandler {
    /// Process an event. Return true if consumed.
    async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool;
    /// Called when the event stream ends.
    async fn finish(&mut self, _ctx: &mut EventContext) {}
    /// Called on turn boundaries (Finished/Error) to reset per-turn state.
    async fn reset(&mut self, _ctx: &mut EventContext) {}
}
