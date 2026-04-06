//! Handlers for ACP events/Agent updates

pub mod draft;
pub mod plan;
pub mod tool_call;
pub mod working;

use teloxide::prelude::*;
use teloxide::types::{MessageId, ParseMode, ThreadId};
use tokio::time::{sleep_until, Duration, Instant};

use crate::formatting;
use crate::types::AgentEvent;
use crate::sess_warn;

// --- OutboundThrottle (moved from session.rs) ---

pub struct OutboundThrottle {
    min_interval: Duration,
    next_allowed_at: Instant,
}

impl OutboundThrottle {
    pub fn per_second(count: u64) -> Self {
        let min_interval = Duration::from_secs_f64(1.0 / count as f64);
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
    pub fn new(bot: Bot, chat_id: ChatId, thread_id: i32) -> Self {
        Self {
            bot,
            chat_id,
            thread_id,
            throttle: OutboundThrottle::per_second(1),
        }
    }

    pub async fn send_html(&mut self, text: &str, silent: bool) -> Option<Message> {
        self.throttle.wait_turn().await;
        match self
            .bot
            .send_message(self.chat_id, text)
            .message_thread_id(ThreadId(MessageId(self.thread_id)))
            .parse_mode(ParseMode::Html)
            .disable_notification(silent)
            .await
        {
            Ok(message) => Some(message),
            Err(err) => {
                sess_warn!("Failed to send HTML message to Telegram: {err}");
                None
            }
        }
    }

    pub async fn send_html_chunks(&mut self, text: &str, silent: bool) {
        let chunks = formatting::split_message(text, 4096);
        for chunk in chunks {
            self.throttle.wait_turn().await;
            let _ = self
                .bot
                .send_message(self.chat_id, chunk)
                .message_thread_id(ThreadId(MessageId(self.thread_id)))
                .parse_mode(ParseMode::Html)
                .disable_notification(silent)
                .await
                .map_err(|err| sess_warn!("Failed to send HTML message chunk to Telegram: {err}"));
        }
    }

    pub async fn send_chunks(&mut self, text: &str, parse_mode: ParseMode, silent: bool) {
        let chunks = formatting::split_message(text, 4096);
        for chunk in chunks {
            self.throttle.wait_turn().await;
            let _ = self
                .bot
                .send_message(self.chat_id, chunk)
                .message_thread_id(ThreadId(MessageId(self.thread_id)))
                .parse_mode(parse_mode)
                .disable_notification(silent)
                .await
                .map_err(|err| sess_warn!("Failed to send Telegram message chunk: {err}"));
        }
    }

    pub async fn edit_html(&mut self, msg_id: MessageId, text: &str) -> bool {
        self.throttle.wait_turn().await;
        match self
            .bot
            .edit_message_text(self.chat_id, msg_id, text)
            .parse_mode(ParseMode::Html)
            .await
        {
            Ok(_) => true,
            Err(err) => {
                sess_warn!("Failed to edit Telegram message {}: {}", msg_id.0, err);
                false
            }
        }
    }

    pub async fn delete_msg(&mut self, msg_id: MessageId) {
        self.throttle.wait_turn().await;
        if let Err(err) = self.bot.delete_message(self.chat_id, msg_id).await {
            sess_warn!("Failed to delete Telegram message {}: {}", msg_id.0, err);
        }
    }

    pub async fn pin_msg(&mut self, msg_id: MessageId) {
        self.throttle.wait_turn().await;
        if let Err(e) = self
            .bot
            .pin_chat_message(self.chat_id, msg_id)
            .disable_notification(true)
            .await
        {
            sess_warn!("Failed to pin Telegram message {}: {}", msg_id.0, e);
        }
    }

    pub async fn close_topic(&mut self) {
        self.throttle.wait_turn().await;
        let _ = self
            .bot
            .close_forum_topic(self.chat_id, ThreadId(MessageId(self.thread_id)))
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
