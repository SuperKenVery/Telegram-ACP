use acp::Agent;
use agent_client_protocol as acp;
use std::collections::VecDeque;
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ParseMode, ThreadId};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::formatting;
use crate::handlers::draft::DraftHandler;
use crate::handlers::plan::PlanHandler;
use crate::handlers::tool_call::ToolCallHandler;
use crate::handlers::working::WorkingHandler;
use crate::handlers::{EventContext, EventHandler};
use crate::session_log::{self, with_session_context, TranscriptDirection};
use crate::session_control::{build_control_state, SessionCommand};
use crate::types::{AgentEvent, SessionStatus};
use crate::{sess_error, sess_info, sess_warn};

enum PromptOutcome {
    Finished(String),
    Error(String),
}

pub async fn run_session_runtime(
    conn: Arc<acp::ClientSideConnection>,
    acp_session_id: acp::SessionId,
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    mut command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    mut cancel_rx: mpsc::UnboundedReceiver<oneshot::Sender<anyhow::Result<()>>>,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    status: Arc<Mutex<SessionStatus>>,
    control_state: Arc<Mutex<crate::session_control::SessionControlState>>,
    mut mode_state: Option<acp::SessionModeState>,
    mut config_options: Vec<acp::SessionConfigOption>,
) {
    let (prompt_done_tx, mut prompt_done_rx) = mpsc::unbounded_channel::<PromptOutcome>();
    let mut prompt_active = false;
    let mut command_closed = false;
    let mut cancel_closed = false;
    let mut pending_prompts = VecDeque::<String>::new();

    while !(command_closed && cancel_closed && !prompt_active && pending_prompts.is_empty()) {
        tokio::select! {
            maybe_cmd = command_rx.recv(), if !command_closed => {
                match maybe_cmd {
                    Some(SessionCommand::Prompt(user_text)) => {
                        if prompt_active {
                            sess_info!("Prompt queued while another prompt is active");
                            pending_prompts.push_back(user_text);
                            send_queued_notice(&bot, chat_id, thread_id, &pending_prompts).await;
                        } else {
                            start_prompt(
                                conn.clone(),
                                acp_session_id.clone(),
                                user_text,
                                event_tx.clone(),
                                status.clone(),
                                prompt_done_tx.clone(),
                            ).await;
                            prompt_active = true;
                        }
                    }
                    Some(SessionCommand::SetPermissionMode { mode_id, result_tx }) => {
                        sess_info!("Changing permission mode to {}", mode_id);
                        let request = acp::SetSessionModeRequest::new(
                            acp_session_id.clone(),
                            mode_id.clone(),
                        );
                        if let Some(ctx) = session_log::try_current_session_context() {
                            if let Err(err) = ctx.log().log_acp_payload(
                                TranscriptDirection::ToAgent,
                                &serde_json::json!({ "method": "set_session_mode", "params": &request }),
                            ) {
                                sess_warn!("Failed to record set_session_mode request: {err}");
                            }
                        }
                        let result = conn
                            .set_session_mode(request)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to set permission mode: {e}"));

                        if result.is_ok() {
                            if let Some(state) = &mut mode_state {
                                state.current_mode_id = acp::SessionModeId::new(mode_id.clone());
                            }
                            if let Some(ctx) = session_log::try_current_session_context() {
                                if let Err(err) = ctx.log().log_acp_payload(
                                    TranscriptDirection::FromAgent,
                                    &serde_json::json!({ "method": "set_session_mode", "result": &mode_state }),
                                ) {
                                    sess_warn!("Failed to record set_session_mode response: {err}");
                                }
                            }
                            *control_state.lock().await = build_control_state(&mode_state, &config_options);
                        }
                        let _ = result_tx.send(result.map(|_| ()));
                    }
                    Some(SessionCommand::SetConfigOption {
                        config_id,
                        value_id,
                        result_tx,
                    }) => {
                        sess_info!("Changing config option {} to {}", config_id, value_id);
                        let request = acp::SetSessionConfigOptionRequest::new(
                            acp_session_id.clone(),
                            config_id,
                            value_id,
                        );
                        if let Some(ctx) = session_log::try_current_session_context() {
                            if let Err(err) = ctx.log().log_acp_payload(
                                TranscriptDirection::ToAgent,
                                &serde_json::json!({ "method": "set_session_config_option", "params": &request }),
                            ) {
                                sess_warn!("Failed to record set_session_config_option request: {err}");
                            }
                        }
                        let result = conn
                            .set_session_config_option(request)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to set config option: {e}"));

                        match result {
                            Ok(resp) => {
                                if let Some(ctx) = session_log::try_current_session_context() {
                                    if let Err(err) = ctx.log().log_acp_payload(
                                        TranscriptDirection::FromAgent,
                                        &serde_json::json!({ "method": "set_session_config_option", "result": &resp }),
                                    ) {
                                        sess_warn!("Failed to record set_session_config_option response: {err}");
                                    }
                                }
                                config_options = resp.config_options;
                                *control_state.lock().await = build_control_state(&mode_state, &config_options);
                                let _ = result_tx.send(Ok(()));
                            }
                            Err(e) => {
                                let _ = result_tx.send(Err(e));
                            }
                        }
                    }
                    None => {
                        command_closed = true;
                    }
                }
            }
            maybe_cancel = cancel_rx.recv(), if !cancel_closed => {
                match maybe_cancel {
                    Some(result_tx) => {
                        sess_warn!("Session cancellation requested");
                        let request = acp::CancelNotification::new(acp_session_id.clone());
                        if let Some(ctx) = session_log::try_current_session_context() {
                            if let Err(err) = ctx.log().log_acp_payload(
                                TranscriptDirection::ToAgent,
                                &serde_json::json!({ "method": "cancel", "params": &request }),
                            ) {
                                sess_warn!("Failed to record cancel request: {err}");
                            }
                        }
                        let result = conn
                            .cancel(request)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to cancel session: {e}"));
                        let _ = result_tx.send(result);
                    }
                    None => {
                        cancel_closed = true;
                    }
                }
            }
            maybe_done = prompt_done_rx.recv(), if prompt_active => {
                match maybe_done {
                    Some(PromptOutcome::Finished(reason)) => {
                        sess_info!("Prompt finished: {}", reason);
                        let _ = event_tx.send(AgentEvent::Finished(reason));
                    }
                    Some(PromptOutcome::Error(err)) => {
                        sess_error!("Prompt failed: {}", err);
                        let _ = event_tx.send(AgentEvent::Error(err));
                    }
                    None => {
                        sess_error!("Prompt runner closed unexpectedly");
                        let _ = event_tx.send(AgentEvent::Error("Prompt runner closed unexpectedly".to_string()));
                    }
                }

                if let Some(next_prompt) = pending_prompts.pop_front() {
                    start_prompt(
                        conn.clone(),
                        acp_session_id.clone(),
                        next_prompt,
                        event_tx.clone(),
                        status.clone(),
                        prompt_done_tx.clone(),
                    ).await;
                    prompt_active = true;
                } else {
                    prompt_active = false;
                    let mut s = status.lock().await;
                    *s = SessionStatus::Idle;
                }
            }
        }
    }

    let mut s = status.lock().await;
    *s = SessionStatus::Finished;
    sess_info!("Session runtime marked as finished");
}

async fn start_prompt(
    conn: Arc<acp::ClientSideConnection>,
    acp_session_id: acp::SessionId,
    user_text: String,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    status: Arc<Mutex<SessionStatus>>,
    prompt_done_tx: mpsc::UnboundedSender<PromptOutcome>,
) {
    {
        let mut s = status.lock().await;
        *s = SessionStatus::Prompting;
    }

    sess_info!("Prompt started");
    let _ = event_tx.send(AgentEvent::Working);

    let current_ctx = session_log::try_current_session_context();
    let prompt_future = async move {
        let request = acp::PromptRequest::new(acp_session_id, vec![user_text.into()]);
        if let Some(ctx) = session_log::try_current_session_context() {
            if let Err(err) = ctx.log().log_acp_payload(
                TranscriptDirection::ToAgent,
                &serde_json::json!({ "method": "prompt", "params": &request }),
            ) {
                sess_warn!("Failed to record prompt request: {err}");
            }
        }
        let prompt_result = conn
            .prompt(request)
            .await;

        let outcome = match prompt_result {
            Ok(resp) => {
                if let Some(ctx) = session_log::try_current_session_context() {
                    if let Err(err) = ctx.log().log_acp_payload(
                        TranscriptDirection::FromAgent,
                        &serde_json::json!({ "method": "prompt", "result": &resp }),
                    ) {
                        sess_warn!("Failed to record prompt response: {err}");
                    }
                }
                PromptOutcome::Finished(format!("{:?}", resp.stop_reason))
            }
            Err(e) => PromptOutcome::Error(format!("Agent error: {e}")),
        };
        let _ = prompt_done_tx.send(outcome);
    };

    if let Some(ctx) = current_ctx {
        tokio::task::spawn_local(with_session_context(ctx, prompt_future));
    } else {
        tokio::task::spawn_local(prompt_future);
    }
}

fn build_interrupt_callback_data(thread_id: i32) -> String {
    format!("cancelq:{thread_id}")
}

async fn send_queued_notice(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    queued_prompts: &VecDeque<String>,
) {
    let mut lines = Vec::with_capacity(queued_prompts.len() + 2);
    lines.push("Agent is currently working.".to_string());
    lines.push("Your message was queued. Pending queue:".to_string());
    for (idx, prompt) in queued_prompts.iter().enumerate() {
        lines.push(format!("{}. {}", idx + 1, formatting::escape_html(prompt)));
    }

    let text = lines.join("\n");
    let chunks = formatting::split_message(&text, 4096);
    let callback_data = build_interrupt_callback_data(thread_id);
    let keyboard = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
        "Interrupt and run queued now",
        callback_data,
    )]]);

    let mut iter = chunks.into_iter().peekable();
    while let Some(chunk) = iter.next() {
        let mut request = bot
            .send_message(chat_id, chunk)
            .message_thread_id(ThreadId(MessageId(thread_id)))
            .parse_mode(ParseMode::Html);
        if iter.peek().is_none() {
            request = request.reply_markup(keyboard.clone());
        }
        if let Err(e) = request.await {
            sess_warn!("Failed to send queued notice: {e}");
            break;
        }
    }
}

/// Consume AgentEvents and send them as Telegram messages in the forum topic.
/// Dispatches events to specialized handlers, each tracking their own state.
pub async fn run_event_consumer(
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    mut event_rx: mpsc::UnboundedReceiver<AgentEvent>,
    available_commands_cache: Arc<Mutex<Vec<acp::AvailableCommand>>>,
) {
    sess_info!("Event consumer started");
    let mut ctx = EventContext::new(bot, chat_id, thread_id);
    let mut draft = DraftHandler::new();
    let mut working = WorkingHandler::new();
    let mut tool_call = ToolCallHandler::new();
    let mut plan = PlanHandler::new();

    while let Some(event) = event_rx.recv().await {
        // AvailableCommandsUpdate: just update cache
        if let AgentEvent::Update(acp::SessionUpdate::AvailableCommandsUpdate(update)) = &event {
            *available_commands_cache.lock().await = update.available_commands.clone();
            continue;
        }

        // Text chunks → draft handler (streaming)
        if draft.handle(&event, &mut ctx).await {
            working.dismiss(&mut ctx).await;
            continue;
        }

        // Non-text event: flush accumulated draft
        draft.flush(&mut ctx).await;

        // Dismiss working indicator for non-Working events
        if !matches!(event, AgentEvent::Working) {
            working.dismiss(&mut ctx).await;
        }

        // Dispatch to handlers
        if working.handle(&event, &mut ctx).await {
            continue;
        }
        if tool_call.handle(&event, &mut ctx).await {
            continue;
        }
        if plan.handle(&event, &mut ctx).await {
            continue;
        }

        // Inline: simple events
        match event {
            AgentEvent::Update(acp::SessionUpdate::UsageUpdate(usage)) => {
                let text = formatting::format_text_message(&format_usage_update(&usage));
                ctx.send_html_chunks(&text, true).await;
            }
            AgentEvent::Finished(reason) => {
                ctx.send_html_chunks(
                    &formatting::format_completion(&reason, None),
                    false,
                )
                .await;
                tool_call.reset(&mut ctx).await;
            }
            AgentEvent::Error(e) => {
                ctx.send_html_chunks(&formatting::format_error(&e), false)
                    .await;
                tool_call.reset(&mut ctx).await;
            }
            _ => {}
        }
    }

    draft.flush(&mut ctx).await;
    ctx.close_topic().await;
    sess_info!("Event consumer finished");
}

fn format_usage_update(usage: &acp::UsageUpdate) -> String {
    let percent = if usage.size == 0 {
        0.0
    } else {
        (usage.used as f64 / usage.size as f64) * 100.0
    };

    let cost = usage
        .cost
        .as_ref()
        .map(|c| format!(", cost {:.4} {}", c.amount, c.currency))
        .unwrap_or_default();

    format!(
        "Usage update: {}/{} tokens ({percent:.1}%){}",
        usage.used, usage.size, cost
    )
}
