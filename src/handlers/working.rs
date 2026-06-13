use teloxide::types::MessageId;

use super::EventContext;
use crate::types::AgentEvent;

pub struct WorkingHandler {
    msg_id: Option<MessageId>,
}

impl WorkingHandler {
    pub fn new() -> Self {
        Self { msg_id: None }
    }

    /// Handle the Working event by sending a "Working..." indicator.
    /// Returns true if consumed.
    pub async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool {
        if !matches!(event, AgentEvent::Working) {
            return false;
        }
        if let Some(sent) = ctx.send_markdown("⏳ _Working on it..._", true).await {
            self.msg_id = Some(sent.id);
        }
        true
    }

    /// Delete the working indicator message if present.
    pub async fn dismiss(&mut self, ctx: &mut EventContext) {
        if let Some(msg_id) = self.msg_id.take() {
            ctx.delete_msg(msg_id).await;
        }
    }
}
