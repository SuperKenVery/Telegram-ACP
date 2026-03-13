use acp::Agent;
use agent_client_protocol as acp;
use similar::TextDiff;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ParseMode, ThreadId};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{sleep_until, Duration, Instant};

use crate::formatting;
use crate::session_control::{build_control_state, SessionCommand};
use crate::types::{AgentEvent, SessionStatus};

enum PromptOutcome {
    Finished(String),
    Error(String),
}

struct OutboundThrottle {
    min_interval: Duration,
    next_allowed_at: Instant,
}

impl OutboundThrottle {
    fn per_second(count: u64) -> Self {
        let min_interval = Duration::from_secs_f64(1.0 / count as f64);
        Self {
            min_interval,
            next_allowed_at: Instant::now(),
        }
    }

    async fn wait_turn(&mut self) {
        let now = Instant::now();
        if self.next_allowed_at > now {
            sleep_until(self.next_allowed_at).await;
        }
        self.next_allowed_at = Instant::now() + self.min_interval;
    }

    fn try_turn(&mut self) -> bool {
        let now = Instant::now();
        if self.next_allowed_at > now {
            return false;
        }
        self.next_allowed_at = now + self.min_interval;
        true
    }
}

/// State for an in-progress text draft being streamed.
struct DraftState {
    draft_id: i64,
    text: String,
    kind: DraftKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DraftKind {
    AgentMessage,
    AgentThought,
}

/// Tracks which Telegram message corresponds to a tool call id.
struct ToolCallMessageState {
    msg_id: MessageId,
    name: String,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    details: Option<String>,
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
                        let result = conn
                            .set_session_mode(acp::SetSessionModeRequest::new(
                                acp_session_id.clone(),
                                mode_id.clone(),
                            ))
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to set permission mode: {e}"));

                        if result.is_ok() {
                            if let Some(state) = &mut mode_state {
                                state.current_mode_id = acp::SessionModeId::new(mode_id.clone());
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
                        let result = conn
                            .set_session_config_option(acp::SetSessionConfigOptionRequest::new(
                                acp_session_id.clone(),
                                config_id,
                                value_id,
                            ))
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to set config option: {e}"));

                        match result {
                            Ok(resp) => {
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
                        let result = conn
                            .cancel(acp::CancelNotification::new(acp_session_id.clone()))
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
                        let _ = event_tx.send(AgentEvent::Finished(reason));
                    }
                    Some(PromptOutcome::Error(err)) => {
                        let _ = event_tx.send(AgentEvent::Error(err));
                    }
                    None => {
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

    let _ = event_tx.send(AgentEvent::Working);

    tokio::task::spawn_local(async move {
        let prompt_result = conn
            .prompt(acp::PromptRequest::new(
                acp_session_id,
                vec![user_text.into()],
            ))
            .await;

        let outcome = match prompt_result {
            Ok(resp) => PromptOutcome::Finished(format!("{:?}", resp.stop_reason)),
            Err(e) => PromptOutcome::Error(format!("Agent error: {e}")),
        };
        let _ = prompt_done_tx.send(outcome);
    });
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
        lines.push(format!("{}. {}", idx + 1, prompt));
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
            .message_thread_id(ThreadId(MessageId(thread_id)));
        if iter.peek().is_none() {
            request = request.reply_markup(keyboard.clone());
        }
        if let Err(e) = request.await {
            tracing::warn!(
                chat_id = chat_id.0,
                thread_id = thread_id,
                "Failed to send queued notice: {e}"
            );
            break;
        }
    }
}

/// Send a message draft (streaming partial text) via the raw Telegram Bot API.
/// Uses `sendMessageDraft` (Bot API 9.3+). Returns Ok(()) on success.
async fn send_message_draft(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    draft_id: i64,
    text: &str,
    throttle: &mut OutboundThrottle,
) -> anyhow::Result<()> {
    // Coalesce frequent streaming updates: if we are inside the 1s window,
    // skip this send and let the next allowed update publish full draft text.
    if !throttle.try_turn() {
        return Ok(());
    }
    let client = bot.client();
    let token = bot.token();
    let url = format!("https://api.telegram.org/bot{token}/sendMessageDraft");
    let telegram_text = formatting::markdown_to_telegram_md_v2(text);

    let mut body = serde_json::json!({
        "chat_id": chat_id.0,
        "draft_id": draft_id,
        "text": telegram_text,
        "parse_mode": "MarkdownV2",
    });

    if thread_id != 0 {
        body["message_thread_id"] = serde_json::json!(thread_id);
    }

    let resp = client.post(&url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        anyhow::bail!("sendMessageDraft failed ({status}): {body_text}");
    }
    Ok(())
}

/// Flush the accumulated draft text as a finalized `sendMessage`.
/// Clears the draft state. Returns Ok(()) even if sending fails (logs error).
async fn flush_draft(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    draft: &mut Option<DraftState>,
    disable_notification: bool,
    throttle: &mut OutboundThrottle,
) {
    if let Some(d) = draft.take() {
        if d.text.is_empty() {
            return;
        }
        let formatted = match d.kind {
            DraftKind::AgentMessage => formatting::format_text_message(&d.text),
            DraftKind::AgentThought => formatting::format_thought_message(&d.text),
        };
        let finalized_text = formatting::markdown_to_telegram_md_v2(&formatted);
        let chunks = formatting::split_message(&finalized_text, 4096);
        for chunk in chunks {
            throttle.wait_turn().await;
            let send_result = bot
                .send_message(chat_id, chunk.clone())
                .message_thread_id(ThreadId(MessageId(thread_id)))
                .parse_mode(ParseMode::MarkdownV2)
                .disable_notification(disable_notification)
                .await;

            if let Err(e) = send_result {
                tracing::warn!(
                    chat_id = chat_id.0,
                    thread_id = thread_id,
                    chunk_len = chunk.len(),
                    "Failed to send finalized draft chunk as MarkdownV2: {e}"
                );
                break;
            }
        }
    }
}

async fn delete_working_msg(
    bot: &Bot,
    chat_id: ChatId,
    working_msg_id: &mut Option<teloxide::types::MessageId>,
    throttle: &mut OutboundThrottle,
) {
    if let Some(msg_id) = working_msg_id.take() {
        throttle.wait_turn().await;
        let _ = bot.delete_message(chat_id, msg_id).await;
    }
}

async fn send_markdown_message(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    text: &str,
    disable_notification: bool,
    throttle: &mut OutboundThrottle,
) -> Option<Message> {
    throttle.wait_turn().await;
    let telegram_text = formatting::markdown_to_telegram_md_v2(text);
    bot.send_message(chat_id, telegram_text)
        .message_thread_id(ThreadId(MessageId(thread_id)))
        .parse_mode(ParseMode::MarkdownV2)
        .disable_notification(disable_notification)
        .await
        .ok()
}

async fn send_markdown_chunks(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    text: &str,
    disable_notification: bool,
    throttle: &mut OutboundThrottle,
) {
    let telegram_text = formatting::markdown_to_telegram_md_v2(text);
    let chunks = formatting::split_message(&telegram_text, 4096);
    for chunk in chunks {
        throttle.wait_turn().await;
        let _ = bot
            .send_message(chat_id, chunk)
            .message_thread_id(ThreadId(MessageId(thread_id)))
            .parse_mode(ParseMode::MarkdownV2)
            .disable_notification(disable_notification)
            .await;
    }
}

/// Consume AgentEvents and send them as Telegram messages in the forum topic.
/// Consecutive text chunks are streamed via `sendMessageDraft` and finalized
/// with `sendMessage` when a non-text event arrives or the stream ends.
/// Runs until the event channel is closed (i.e., the session ends).
pub async fn run_event_consumer(
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    mut event_rx: mpsc::UnboundedReceiver<AgentEvent>,
    available_commands_cache: Arc<Mutex<Vec<acp::AvailableCommand>>>,
) {
    let mut message_count = 0u32;
    let mut draft: Option<DraftState> = None;
    let mut tool_call_messages: HashMap<String, ToolCallMessageState> = HashMap::new();
    let mut plan_message_id: Option<MessageId> = None;
    let mut throttle = OutboundThrottle::per_second(1);
    let mut working_msg_id: Option<teloxide::types::MessageId> = None;

    while let Some(event) = event_rx.recv().await {
        match &event {
            AgentEvent::Update(acp::SessionUpdate::AvailableCommandsUpdate(update)) => {
                let mut cache = available_commands_cache.lock().await;
                replace_available_commands(&mut cache, update);
                continue;
            }
            AgentEvent::Update(acp::SessionUpdate::AgentMessageChunk(chunk)) => {
                let t = extract_text(&chunk.content);
                if t.is_empty() {
                    continue;
                }
                delete_working_msg(&bot, chat_id, &mut working_msg_id, &mut throttle).await;

                if matches!(
                    draft.as_ref().map(|d| d.kind),
                    Some(DraftKind::AgentThought)
                ) {
                    flush_draft(
                        &bot,
                        chat_id,
                        thread_id,
                        &mut draft,
                        message_count > 0,
                        &mut throttle,
                    )
                    .await;
                    if message_count == 0 {
                        message_count += 1;
                    }
                }

                let d = draft.get_or_insert_with(|| DraftState {
                    draft_id: rand_draft_id(),
                    text: String::new(),
                    kind: DraftKind::AgentMessage,
                });
                d.text.push_str(&t);

                let draft_text = formatting::format_text_message(&d.text);
                if let Err(e) = send_message_draft(
                    &bot,
                    chat_id,
                    thread_id,
                    d.draft_id,
                    &draft_text,
                    &mut throttle,
                )
                .await
                {
                    tracing::warn!(
                        chat_id = chat_id.0,
                        thread_id = thread_id,
                        draft_id = d.draft_id,
                        text_len = d.text.len(),
                        "sendMessageDraft failed: {e}"
                    );
                }
                continue;
            }
            AgentEvent::Update(acp::SessionUpdate::AgentThoughtChunk(chunk)) => {
                let t = extract_text(&chunk.content);
                if t.is_empty() {
                    continue;
                }
                delete_working_msg(&bot, chat_id, &mut working_msg_id, &mut throttle).await;

                if matches!(
                    draft.as_ref().map(|d| d.kind),
                    Some(DraftKind::AgentMessage)
                ) {
                    flush_draft(
                        &bot,
                        chat_id,
                        thread_id,
                        &mut draft,
                        message_count > 0,
                        &mut throttle,
                    )
                    .await;
                    if message_count == 0 {
                        message_count += 1;
                    }
                }

                let d = draft.get_or_insert_with(|| DraftState {
                    draft_id: rand_draft_id(),
                    text: String::new(),
                    kind: DraftKind::AgentThought,
                });
                d.text.push_str(&t);

                let draft_text = formatting::format_thought_message(&d.text);
                if let Err(e) = send_message_draft(
                    &bot,
                    chat_id,
                    thread_id,
                    d.draft_id,
                    &draft_text,
                    &mut throttle,
                )
                .await
                {
                    tracing::warn!(
                        chat_id = chat_id.0,
                        thread_id = thread_id,
                        draft_id = d.draft_id,
                        text_len = d.text.len(),
                        "sendMessageDraft failed: {e}"
                    );
                }
                continue;
            }
            _ => {
                flush_draft(
                    &bot,
                    chat_id,
                    thread_id,
                    &mut draft,
                    message_count > 0,
                    &mut throttle,
                )
                .await;
                if message_count == 0 {
                    message_count += 1;
                }
            }
        }

        if !matches!(event, AgentEvent::Working) {
            delete_working_msg(&bot, chat_id, &mut working_msg_id, &mut throttle).await;
        }

        match event {
            AgentEvent::Working => {
                let disable_notification = message_count > 0;
                message_count += 1;
                if let Some(sent) = send_markdown_message(
                    &bot,
                    chat_id,
                    thread_id,
                    "⏳ _Working on it..._",
                    disable_notification,
                    &mut throttle,
                )
                .await
                {
                    working_msg_id = Some(sent.id);
                }
            }
            AgentEvent::Update(acp::SessionUpdate::ToolCall(tool_call)) => {
                let id = tool_call.tool_call_id.to_string();
                let name = tool_call.title;
                let kind = tool_call.kind;
                let status = tool_call.status;
                let details = extract_tool_details(&tool_call.content);
                let disable_notification = message_count > 0;
                message_count += 1;
                if let Some(sent) = send_markdown_message(
                    &bot,
                    chat_id,
                    thread_id,
                    &formatting::format_tool_call(&name, kind, status, details.as_deref()),
                    disable_notification,
                    &mut throttle,
                )
                .await
                {
                    tool_call_messages.insert(
                        id,
                        ToolCallMessageState {
                            msg_id: sent.id,
                            name,
                            kind,
                            status,
                            details,
                        },
                    );
                }
            }
            AgentEvent::Update(acp::SessionUpdate::ToolCallUpdate(update)) => {
                let id = update.tool_call_id.to_string();
                let fields = update.fields;
                let name = fields.title.unwrap_or_default();
                let kind = fields.kind;
                let status = fields.status;
                let output = fields
                    .content
                    .as_ref()
                    .and_then(|contents| extract_tool_output(contents));
                let details = extract_tool_details(fields.content.as_deref().unwrap_or(&[]));

                let resolved_name = if !name.is_empty() {
                    name.clone()
                } else {
                    tool_call_messages
                        .get(&id)
                        .map(|s| s.name.clone())
                        .unwrap_or_default()
                };
                let resolved_details = if details.is_some() {
                    details.clone()
                } else {
                    tool_call_messages.get(&id).and_then(|s| s.details.clone())
                };
                let resolved_kind = kind
                    .or_else(|| tool_call_messages.get(&id).map(|s| s.kind))
                    .unwrap_or(acp::ToolKind::Other);
                let resolved_status = status
                    .or_else(|| tool_call_messages.get(&id).map(|s| s.status))
                    .unwrap_or(acp::ToolCallStatus::Pending);
                let text = formatting::format_tool_result(
                    &resolved_name,
                    resolved_kind,
                    resolved_status,
                    output.as_deref(),
                    resolved_details.as_deref(),
                );

                if let Some(state) = tool_call_messages.get_mut(&id) {
                    let telegram_text = formatting::markdown_to_telegram_md_v2(&text);
                    throttle.wait_turn().await;
                    if bot
                        .edit_message_text(chat_id, state.msg_id, telegram_text)
                        .parse_mode(ParseMode::MarkdownV2)
                        .await
                        .is_ok()
                    {
                        if !name.is_empty() {
                            state.name = name;
                        }
                        if let Some(k) = kind {
                            state.kind = k;
                        }
                        if let Some(s) = status {
                            state.status = s;
                        }
                        if details.is_some() {
                            state.details = details;
                        }
                        continue;
                    }
                }

                let disable_notification = message_count > 0;
                message_count += 1;
                if let Some(sent) = send_markdown_message(
                    &bot,
                    chat_id,
                    thread_id,
                    &text,
                    disable_notification,
                    &mut throttle,
                )
                .await
                {
                    tool_call_messages.insert(
                        id,
                        ToolCallMessageState {
                            msg_id: sent.id,
                            name: resolved_name,
                            kind: resolved_kind,
                            status: resolved_status,
                            details: resolved_details,
                        },
                    );
                }
            }
            AgentEvent::Update(acp::SessionUpdate::UsageUpdate(usage)) => {
                let disable_notification = message_count > 0;
                message_count += 1;
                send_markdown_chunks(
                    &bot,
                    chat_id,
                    thread_id,
                    &formatting::format_text_message(&format_usage_update(&usage)),
                    disable_notification,
                    &mut throttle,
                )
                .await;
            }
            AgentEvent::Update(acp::SessionUpdate::Plan(plan)) => {
                if is_plan_completed(&plan) {
                    if let Some(old_plan_message_id) = plan_message_id.take() {
                        throttle.wait_turn().await;
                        let _ = bot.delete_message(chat_id, old_plan_message_id).await;
                    }

                    send_markdown_chunks(
                        &bot,
                        chat_id,
                        thread_id,
                        &formatting::format_plan_completed(&plan),
                        false,
                        &mut throttle,
                    )
                    .await;
                    continue;
                }

                let formatted = formatting::format_plan(&plan);
                if let Some(existing_plan_message_id) = plan_message_id {
                    let telegram_formatted = formatting::markdown_to_telegram_md_v2(&formatted);
                    throttle.wait_turn().await;
                    if bot
                        .edit_message_text(chat_id, existing_plan_message_id, telegram_formatted)
                        .parse_mode(ParseMode::MarkdownV2)
                        .await
                        .is_ok()
                    {
                        continue;
                    }
                }

                let disable_notification = message_count > 0;
                message_count += 1;
                if let Some(sent) = send_markdown_message(
                    &bot,
                    chat_id,
                    thread_id,
                    &formatted,
                    disable_notification,
                    &mut throttle,
                )
                .await
                {
                    plan_message_id = Some(sent.id);
                    throttle.wait_turn().await;
                    if let Err(e) = bot
                        .pin_chat_message(chat_id, sent.id)
                        .disable_notification(true)
                        .await
                    {
                        tracing::warn!(
                            chat_id = chat_id.0,
                            thread_id = thread_id,
                            message_id = sent.id.0,
                            "Failed to pin plan message: {e}"
                        );
                    }
                }
            }
            AgentEvent::Update(_) => {}
            AgentEvent::Finished(reason) => {
                send_markdown_chunks(
                    &bot,
                    chat_id,
                    thread_id,
                    &formatting::format_completion(&reason, None),
                    false,
                    &mut throttle,
                )
                .await;
                message_count = 0;
                tool_call_messages.clear();
            }
            AgentEvent::Error(e) => {
                send_markdown_chunks(
                    &bot,
                    chat_id,
                    thread_id,
                    &formatting::format_error(&e),
                    false,
                    &mut throttle,
                )
                .await;
                message_count = 0;
                tool_call_messages.clear();
            }
        }
    }

    flush_draft(&bot, chat_id, thread_id, &mut draft, true, &mut throttle).await;
    throttle.wait_turn().await;
    let _ = bot
        .close_forum_topic(chat_id, ThreadId(MessageId(thread_id)))
        .await;
}

fn extract_text(content: &acp::ContentBlock) -> String {
    match content {
        acp::ContentBlock::Text(tc) => tc.text.clone(),
        _ => String::new(),
    }
}

fn extract_tool_output(contents: &[acp::ToolCallContent]) -> Option<String> {
    let mut parts = Vec::new();
    for content in contents {
        match content {
            acp::ToolCallContent::Content(content) => {
                let text = extract_text(&content.content);
                if !text.trim().is_empty() {
                    parts.push(text);
                }
            }
            acp::ToolCallContent::Diff(diff) => {
                parts.push(format_unified_diff(
                    Some(diff.path.display().to_string()),
                    diff.old_text.as_deref(),
                    &diff.new_text,
                ));
            }
            _ => {}
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

fn extract_tool_details(contents: &[acp::ToolCallContent]) -> Option<String> {
    let diffs: Vec<String> = contents
        .iter()
        .filter_map(|content| match content {
            acp::ToolCallContent::Diff(diff) => Some(format_unified_diff(
                Some(diff.path.display().to_string()),
                diff.old_text.as_deref(),
                &diff.new_text,
            )),
            _ => None,
        })
        .collect();

    if diffs.is_empty() {
        None
    } else {
        Some(diffs.join("\n\n"))
    }
}

fn format_unified_diff(path: Option<String>, old_text: Option<&str>, new_text: &str) -> String {
    let old = old_text.unwrap_or("");
    let path = path.unwrap_or_else(|| "file".to_string());
    let old_header = format!("a/{path}");
    let new_header = format!("b/{path}");
    let unified = TextDiff::from_lines(old, new_text)
        .unified_diff()
        .context_radius(2)
        .header(&old_header, &new_header)
        .to_string();

    if unified.trim().is_empty() {
        format!("--- {old_header}\n+++ {new_header}\n(no changes)")
    } else {
        unified
    }
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

fn rand_draft_id() -> i64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let s = RandomState::new();
    let mut h = s.build_hasher();
    h.write_u8(0);
    h.finish() as i64
}

fn is_plan_completed(plan: &acp::Plan) -> bool {
    !plan.entries.is_empty()
        && plan
            .entries
            .iter()
            .all(|entry| matches!(entry.status, acp::PlanEntryStatus::Completed))
}

fn replace_available_commands(
    cache: &mut Vec<acp::AvailableCommand>,
    update: &acp::AvailableCommandsUpdate,
) {
    *cache = update.available_commands.clone();
}

#[cfg(test)]
mod tests {
    use super::replace_available_commands;
    use agent_client_protocol as acp;

    #[test]
    fn available_commands_update_replaces_previous_value() {
        let mut cache = vec![acp::AvailableCommand::new("old", "Old command")];
        let update = acp::AvailableCommandsUpdate::new(vec![acp::AvailableCommand::new(
            "new",
            "New command",
        )]);

        replace_available_commands(&mut cache, &update);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache[0].name, "new");
    }

    #[test]
    fn available_commands_update_can_clear_cache() {
        let mut cache = vec![acp::AvailableCommand::new("old", "Old command")];
        let update = acp::AvailableCommandsUpdate::new(vec![]);

        replace_available_commands(&mut cache, &update);
        assert!(cache.is_empty());
    }
}
