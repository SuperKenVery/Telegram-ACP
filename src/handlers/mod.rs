//! Handlers for ACP events/Agent updates

pub mod draft;
pub mod plan;
pub mod tool_call;
pub mod working;

use std::future::Future;

use teloxide::prelude::*;
use teloxide::types::{MessageId, ParseMode, ThreadId};
use teloxide::RequestError;
use tokio::time::{sleep_until, Duration, Instant};

use crate::formatting;
use crate::sess_warn;
use crate::types::AgentEvent;

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

    fn html_fallback_text(text: &str) -> Option<String> {
        if text.len() <= 4096 {
            None
        } else {
            Some(formatting::truncate_message(text, 3900))
        }
    }

    pub fn new(bot: Bot, chat_id: ChatId, thread_id: i32) -> Self {
        Self {
            bot,
            chat_id,
            thread_id,
            throttle: OutboundThrottle::with_interval(2.0),
        }
    }

    async fn request_with_throttle<T, F, Fut>(&mut self, label: &str, mut request: F) -> Result<T, RequestError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, RequestError>>,
    {
        loop {
            self.throttle.wait_turn().await;
            match request().await {
                Ok(value) => return Ok(value),
                Err(RequestError::RetryAfter(after)) => {
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

    async fn request_with_throttle_drop<T, F, Fut>(&mut self, label: &str, mut request: F) -> Result<Option<T>, RequestError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, RequestError>>,
    {
        if !self.throttle.try_turn() {
            return Ok(None);
        }
        match request().await {
            Ok(value) => Ok(Some(value)),
            Err(RequestError::RetryAfter(after)) => {
                let delay = after.duration() + Self::RETRY_AFTER_PADDING;
                self.throttle.defer_for(delay);
                sess_warn!(
                    "Telegram rate limit while {label}; backing off for {}s (dropping update)",
                    delay.as_secs()
                );
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn send_html(&mut self, text: &str, silent: bool) -> Option<Message> {
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            match self.request_with_throttle("sending plain-text fallback message to Telegram", move || {
                bot.send_message(chat_id, text.clone())
                    .message_thread_id(ThreadId(MessageId(thread_id)))
                    .disable_notification(silent)
                    .send()
            }).await {
                Ok(message) => Some(message),
                Err(err) => {
                    sess_warn!("Failed to send plain-text fallback message to Telegram: {err}");
                    None
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let text = text.to_string();
            match self.request_with_throttle("sending HTML message to Telegram", move || {
                bot.send_message(chat_id, text.clone())
                    .message_thread_id(ThreadId(MessageId(thread_id)))
                    .parse_mode(ParseMode::Html)
                    .disable_notification(silent)
                    .send()
            }).await {
                Ok(message) => Some(message),
                Err(err) => {
                    sess_warn!("Failed to send HTML message to Telegram: {err}");
                    None
                }
            }
        }
    }

    pub async fn send_html_drop(&mut self, text: &str, silent: bool) -> Option<Message> {
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            match self.request_with_throttle_drop("sending plain-text fallback message to Telegram", move || {
                bot.send_message(chat_id, text.clone())
                    .message_thread_id(ThreadId(MessageId(thread_id)))
                    .disable_notification(silent)
                    .send()
            }).await {
                Ok(Some(message)) => Some(message),
                Ok(None) => None,
                Err(err) => {
                    sess_warn!("Failed to send plain-text fallback message to Telegram: {err}");
                    None
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let text = text.to_string();
            match self.request_with_throttle_drop("sending HTML message to Telegram", move || {
                bot.send_message(chat_id, text.clone())
                    .message_thread_id(ThreadId(MessageId(thread_id)))
                    .parse_mode(ParseMode::Html)
                    .disable_notification(silent)
                    .send()
            }).await {
                Ok(Some(message)) => Some(message),
                Ok(None) => None,
                Err(err) => {
                    sess_warn!("Failed to send HTML message to Telegram: {err}");
                    None
                }
            }
        }
    }

    pub async fn send_html_chunks(&mut self, text: &str, silent: bool) {
        let chunks = formatting::split_message(text, 4096);
        for chunk in chunks {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let _ = self
                .request_with_throttle("sending HTML message chunk to Telegram", move || {
                    bot.send_message(chat_id, chunk.clone())
                        .message_thread_id(ThreadId(MessageId(thread_id)))
                        .parse_mode(ParseMode::Html)
                        .disable_notification(silent)
                        .send()
                })
                .await
                .map_err(|err| sess_warn!("Failed to send HTML message chunk to Telegram: {err}"));
        }
    }

    pub async fn send_chunks(&mut self, text: &str, parse_mode: ParseMode, silent: bool) {
        let chunks = formatting::split_message(text, 4096);
        for chunk in chunks {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let _ = self
                .request_with_throttle("sending Telegram message chunk", move || {
                    bot.send_message(chat_id, chunk.clone())
                        .message_thread_id(ThreadId(MessageId(thread_id)))
                        .parse_mode(parse_mode)
                        .disable_notification(silent)
                        .send()
                })
                .await
                .map_err(|err| sess_warn!("Failed to send Telegram message chunk: {err}"));
        }
    }

    pub async fn edit_html(&mut self, msg_id: MessageId, text: &str) -> bool {
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            match self
                .request_with_throttle(
                    &format!("editing Telegram message {} with plain-text fallback", msg_id.0),
                    move || bot.edit_message_text(chat_id, msg_id, text.clone()).send(),
                )
                .await
            {
                Ok(_) => true,
                Err(err) => {
                    sess_warn!(
                        "Failed to edit Telegram message {} with plain-text fallback: {}",
                        msg_id.0,
                        err
                    );
                    false
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let text = text.to_string();
            match self
                .request_with_throttle(&format!("editing Telegram message {}", msg_id.0), move || {
                    bot.edit_message_text(chat_id, msg_id, text.clone())
                        .parse_mode(ParseMode::Html)
                        .send()
                })
                .await
            {
                Ok(_) => true,
                Err(err) => {
                    sess_warn!("Failed to edit Telegram message {}: {}", msg_id.0, err);
                    false
                }
            }
        }
    }

    pub async fn edit_html_drop(&mut self, msg_id: MessageId, text: &str) -> bool {
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            match self
                .request_with_throttle_drop(
                    &format!("editing Telegram message {} with plain-text fallback", msg_id.0),
                    move || bot.edit_message_text(chat_id, msg_id, text.clone()).send(),
                )
                .await
            {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(err) => {
                    sess_warn!(
                        "Failed to edit Telegram message {} with plain-text fallback: {}",
                        msg_id.0,
                        err
                    );
                    false
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let text = text.to_string();
            match self
                .request_with_throttle_drop(&format!("editing Telegram message {}", msg_id.0), move || {
                    bot.edit_message_text(chat_id, msg_id, text.clone())
                        .parse_mode(ParseMode::Html)
                        .send()
                })
                .await
            {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(err) => {
                    sess_warn!("Failed to edit Telegram message {}: {}", msg_id.0, err);
                    false
                }
            }
        }
    }

    pub async fn delete_msg(&mut self, msg_id: MessageId) {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        if let Err(err) = self
            .request_with_throttle(&format!("deleting Telegram message {}", msg_id.0), move || {
                bot.delete_message(chat_id, msg_id).send()
            })
            .await
        {
            sess_warn!("Failed to delete Telegram message {}: {}", msg_id.0, err);
        }
    }

    pub async fn pin_msg(&mut self, msg_id: MessageId) {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        if let Err(e) = self
            .request_with_throttle(&format!("pinning Telegram message {}", msg_id.0), move || {
                bot.pin_chat_message(chat_id, msg_id)
                    .disable_notification(true)
                    .send()
            })
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
                bot.close_forum_topic(chat_id, ThreadId(MessageId(thread_id)))
                    .send()
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
