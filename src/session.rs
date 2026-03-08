use agent_client_protocol as acp;
use acp::Agent;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::types::{AgentEvent, SessionStatus};

/// Run the prompt loop for a session. Listens for user messages and sends them to the agent.
/// Emits AgentEvents via the event_tx that was given to the TelegramClient.
pub async fn run_prompt_loop(
    conn: Arc<acp::ClientSideConnection>,
    acp_session_id: acp::SessionId,
    mut user_rx: mpsc::UnboundedReceiver<String>,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    status: Arc<Mutex<SessionStatus>>,
) {
    while let Some(user_text) = user_rx.recv().await {
        {
            let mut s = status.lock().await;
            *s = SessionStatus::Prompting;
        }

        // Send a "Working on it..." indicator immediately
        let _ = event_tx.send(AgentEvent::Working);

        let prompt_result = conn
            .prompt(acp::PromptRequest::new(
                acp_session_id.clone(),
                vec![user_text.into()],
            ))
            .await;

        match prompt_result {
            Ok(resp) => {
                let reason = format!("{:?}", resp.stop_reason);
                let _ = event_tx.send(AgentEvent::Finished(reason));
            }
            Err(e) => {
                let _ = event_tx.send(AgentEvent::Error(format!("Agent error: {e}")));
            }
        }

        {
            let mut s = status.lock().await;
            *s = SessionStatus::Idle;
        }
    }

    // Channel closed, session is done
    let mut s = status.lock().await;
    *s = SessionStatus::Finished;
}
