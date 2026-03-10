use acp::Agent;
use agent_client_protocol as acp;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::session_control::{build_control_state, SessionCommand};
use crate::types::{AgentEvent, SessionStatus};

/// Run the prompt loop for a session. Listens for user messages and sends them to the agent.
/// Emits AgentEvents via the event_tx that was given to the TelegramClient.
pub async fn run_prompt_loop(
    conn: Arc<acp::ClientSideConnection>,
    acp_session_id: acp::SessionId,
    mut command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    status: Arc<Mutex<SessionStatus>>,
    pending_prompts: Arc<Mutex<VecDeque<String>>>,
    mut mode_state: Option<acp::SessionModeState>,
    mut config_options: Vec<acp::SessionConfigOption>,
) {
    while let Some(cmd) = command_rx.recv().await {
        match cmd {
            SessionCommand::Prompt(user_text) => {
                // If this prompt came from the queued backlog, drop it from the in-memory queue.
                {
                    let mut pending = pending_prompts.lock().await;
                    if pending.front().is_some_and(|front| front == &user_text) {
                        pending.pop_front();
                    }
                }

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
            SessionCommand::GetControlState { result_tx } => {
                let _ = result_tx.send(Ok(build_control_state(&mode_state, &config_options)));
            }
            SessionCommand::SetPermissionMode { mode_id, result_tx } => {
                let result = conn
                    .set_session_mode(acp::SetSessionModeRequest::new(
                        acp_session_id.clone(),
                        mode_id.clone(),
                    ))
                    .await
                    .map(|_| {
                        if let Some(state) = &mut mode_state {
                            state.current_mode_id = acp::SessionModeId::new(mode_id.clone());
                        }
                        build_control_state(&mode_state, &config_options)
                    })
                    .map_err(|e| anyhow::anyhow!("Failed to set permission mode: {e}"));

                let _ = result_tx.send(result);
            }
            SessionCommand::SetConfigOption {
                config_id,
                value_id,
                result_tx,
            } => {
                let result = conn
                    .set_session_config_option(acp::SetSessionConfigOptionRequest::new(
                        acp_session_id.clone(),
                        config_id,
                        value_id,
                    ))
                    .await
                    .map(|resp| {
                        config_options = resp.config_options;
                        build_control_state(&mode_state, &config_options)
                    })
                    .map_err(|e| anyhow::anyhow!("Failed to set config option: {e}"));

                let _ = result_tx.send(result);
            }
        }
    }

    // Channel closed, session is done
    let mut s = status.lock().await;
    *s = SessionStatus::Finished;
}

pub async fn run_cancel_loop(
    conn: Arc<acp::ClientSideConnection>,
    acp_session_id: acp::SessionId,
    mut cancel_rx: mpsc::UnboundedReceiver<oneshot::Sender<anyhow::Result<()>>>,
) {
    while let Some(result_tx) = cancel_rx.recv().await {
        let result = conn
            .cancel(acp::CancelNotification::new(acp_session_id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to cancel session: {e}"));
        let _ = result_tx.send(result);
    }
}
