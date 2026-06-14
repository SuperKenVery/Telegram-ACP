use agent_client_protocol as acp;
use teloxide::types::MessageId;

use super::{EventContext, EventHandler};
use crate::formatting;
use crate::types::AgentEvent;

pub struct PlanHandler {
    message_id: Option<MessageId>,
}

impl PlanHandler {
    pub fn new() -> Self {
        Self { message_id: None }
    }
}

#[async_trait::async_trait]
impl EventHandler for PlanHandler {
    async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool {
        let plan = match event {
            AgentEvent::Update(acp::SessionUpdate::Plan(plan)) => plan,
            _ => return false,
        };

        if is_plan_completed(plan) {
            if let Some(old_id) = self.message_id.take() {
                ctx.delete_msg(old_id).await;
            }
            ctx.send_markdown_chunks(&formatting::format_plan_completed(plan), true)
                .await;
            return true;
        }

        let formatted = formatting::format_plan(plan);

        // Try to edit existing plan message
        if let Some(existing_id) = self.message_id {
            if ctx.edit_markdown(existing_id, &formatted).await {
                return true;
            }
        }

        // Send new plan message and pin it
        if let Some(sent) = ctx.send_markdown(&formatted, true).await {
            self.message_id = Some(sent.id);
            ctx.pin_msg(sent.id).await;
        }
        true
    }
}

fn is_plan_completed(plan: &acp::Plan) -> bool {
    !plan.entries.is_empty()
        && plan
            .entries
            .iter()
            .all(|entry| matches!(entry.status, acp::PlanEntryStatus::Completed))
}
