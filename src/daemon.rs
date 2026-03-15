use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use agent_client_protocol as acp_sdk;
use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use futures::future::join_all;
use rmcp::service::RxJsonRpcMessage;
use rmcp::RoleServer;
use serde_json::Value as JsonValue;
use telegraph_rs::Telegraph;
use teloxide::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::acp;
use crate::config::Config;
use crate::mcp;
use crate::persistence::{self, PersistedTopic};
use crate::session;
use crate::session_control::{self, SessionCommand};
use crate::telegram;
use crate::types::{AgentEvent, SessionRecord, SessionStatus};

/// Shared daemon state, accessible from Telegram handlers and IPC.
pub struct DaemonHandle {
    pub config: Config,
    pub bot: Bot,
    #[allow(dead_code)]
    pub telegraph: Arc<Telegraph>,
    /// Relay for starting ACP sessions inside the daemon's LocalSet task.
    local_start_tx: mpsc::UnboundedSender<StartSessionRequest>,
    /// thread_id -> TopicEntry
    pub topics: DashMap<i32, TopicEntry>,
}

pub struct TopicEntry {
    /// Currently running session, if any.
    pub active: Option<SessionEntry>,
    /// All sessions ever bound to this topic.
    pub history: Vec<SessionRecord>,
}

pub struct SessionEntry {
    pub acp_session_id: Option<String>,
    pub mcp_session_id: String,
    pub mcp: Arc<mcp::McpSession>,
    pub project_path: PathBuf,
    pub agent_command: String,
    pub agent_name: Option<String>,
    pub status: Arc<tokio::sync::Mutex<SessionStatus>>,
    pub available_commands: Arc<tokio::sync::Mutex<Vec<acp_sdk::AvailableCommand>>>,
    pub control_state: Arc<tokio::sync::Mutex<session_control::SessionControlState>>,
    pub command_tx: mpsc::UnboundedSender<SessionCommand>,
    pub cancel_tx: mpsc::UnboundedSender<oneshot::Sender<Result<()>>>,
}

struct StartSessionRequest {
    thread_id: i32,
    project_path: PathBuf,
    agent_cmd: String,
    agent_name: Option<String>,
    existing_acp_session_id: Option<String>,
    result_tx: oneshot::Sender<Result<String>>,
}

impl DaemonHandle {
    fn get_mcp_session_by_id(&self, session_id: &str) -> Option<Arc<mcp::McpSession>> {
        self.topics.iter().find_map(|entry| {
            entry
                .active
                .as_ref()
                .filter(|s| s.mcp_session_id == session_id)
                .map(|s| s.mcp.clone())
        })
    }

    pub async fn handle_mcp_message(
        &self,
        session_id: &str,
        payload: &str,
    ) -> Result<Option<String>> {
        tracing::debug!(session_id, "handle_mcp_message: looking up session");
        let mcp_session = self
            .get_mcp_session_by_id(session_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown MCP session id: {}", session_id))?;

        let payload_value: JsonValue = serde_json::from_str(payload)
            .map_err(|e| anyhow::anyhow!("Invalid MCP payload: {e}"))?;
        let expects_response = mcp_expects_response(&payload_value);
        tracing::debug!(
            session_id,
            expects_response,
            payload = %payload,
            "handle_mcp_message: routing message"
        );
        let message: RxJsonRpcMessage<RoleServer> = serde_json::from_value(payload_value)
            .map_err(|e| anyhow::anyhow!("Failed to decode MCP payload: {e}"))?;

        mcp_session.send(message).await?;
        tracing::debug!(session_id, "handle_mcp_message: message sent to McpSession");

        if expects_response {
            tracing::debug!(session_id, "handle_mcp_message: waiting for response");
            if let Some(response) = mcp_session.next_response().await {
                let payload = serde_json::to_string(&response)?;
                tracing::debug!(session_id, response = %payload, "handle_mcp_message: got response");
                Ok(Some(payload))
            } else {
                tracing::warn!(
                    session_id,
                    "handle_mcp_message: outgoing channel closed while waiting for response"
                );
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn cancel_session(&self, thread_id: i32) -> Result<()> {
        let cancel_tx = {
            let entry = self
                .topics
                .get(&thread_id)
                .ok_or_else(|| anyhow::anyhow!("No topic for this thread"))?;
            let active = entry
                .active
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("No active session in this topic"))?;
            active.cancel_tx.clone()
        };
        let (result_tx, result_rx) = oneshot::channel();
        cancel_tx
            .send(result_tx)
            .map_err(|_| anyhow::anyhow!("Session cancel channel closed"))?;
        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("Cancel request dropped"))?
    }

    /// Remove a topic from in-memory state and persisted storage.
    pub async fn remove_topic(&self, thread_id: i32) -> Option<TopicEntry> {
        let (_, entry) = self.topics.remove(&thread_id)?;
        self.persist_topics().await;
        Some(entry)
    }

    pub fn get_session_command_tx_by_thread(
        &self,
        thread_id: i32,
    ) -> Option<mpsc::UnboundedSender<SessionCommand>> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()
            .map(|e| e.command_tx.clone())
    }

    pub fn get_session_project_path_by_thread(&self, thread_id: i32) -> Option<PathBuf> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()
            .map(|e| e.project_path.clone())
    }

    pub fn get_session_agent_by_thread(&self, thread_id: i32) -> Option<String> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()
            .and_then(|e| e.agent_name.clone())
    }

    pub async fn get_available_commands_by_thread(
        &self,
        thread_id: i32,
    ) -> Option<Vec<acp_sdk::AvailableCommand>> {
        let available_commands = self
            .topics
            .get(&thread_id)?
            .active
            .as_ref()
            .map(|e| e.available_commands.clone())?;
        let commands = available_commands.lock().await.clone();
        Some(commands)
    }

    /// Persist current topics to disk.
    pub async fn persist_topics(&self) {
        let mut persisted = Vec::new();
        for entry in self.topics.iter() {
            let thread_id = *entry.key();
            let topic = entry.value();
            let active_session_id = topic.active.as_ref().and_then(|s| s.acp_session_id.clone());
            persisted.push(PersistedTopic {
                thread_id,
                active_session_id,
                sessions: topic.history.clone(),
            });
        }
        if let Err(e) = persistence::save_topics(&persisted) {
            tracing::error!("Failed to persist topics: {e}");
        }
    }

    /// Spawn a new agent session: create topic, spawn agent, wire everything up.
    /// Waits for ACP init to complete before returning.
    pub async fn spawn_session(
        &self,
        path: String,
        _prompt: Option<String>,
        agent: Option<String>,
    ) -> Result<(String, i32)> {
        let project_path = PathBuf::from(&path);
        let (agent_name, agent_cmd) = self.config.resolve_agent(agent.as_deref())?;

        // Create forum topic
        let topic_name = project_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| path.clone());
        let topic = self
            .bot
            .create_forum_topic(ChatId(self.config.chat_id), &topic_name)
            .icon_color(teloxide::types::Rgb::from_u32(0x6FB9F0))
            .await?;
        let thread_id = topic.thread_id.0 .0;

        let acp_session_id = match self
            .enqueue_start_session(thread_id, project_path, agent_cmd, Some(agent_name), None)
            .await
        {
            Ok(session_id) => session_id,
            Err(e) => {
                let delete_result = self
                    .bot
                    .delete_forum_topic(
                        ChatId(self.config.chat_id),
                        teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)),
                    )
                    .await;
                if let Err(delete_err) = delete_result {
                    tracing::warn!(
                        "Failed to delete forum topic {} after ACP init failure: {}",
                        thread_id,
                        delete_err
                    );
                }
                let _ = self
                    .bot
                    .send_message(
                        ChatId(self.config.chat_id),
                        format!(
                            "Failed to initialize ACP session for '{}' (topic {}). Topic was removed. Error:\n{:#}",
                            path, thread_id, e
                        ),
                    )
                    .await;
                return Err(e);
            }
        };

        Ok((acp_session_id, thread_id))
    }

    /// Replace the active session in an existing topic with a new one.
    pub async fn replace_session_in_thread(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent: Option<String>,
    ) -> Result<String> {
        let (agent_name, agent_cmd) = self.config.resolve_agent(agent.as_deref())?;

        // Deactivate current session (drop it)
        if let Some(mut entry) = self.topics.get_mut(&thread_id) {
            entry.active = None;
        }

        self.enqueue_start_session(thread_id, project_path, agent_cmd, Some(agent_name), None)
            .await
    }

    /// Switch to a previously used session in a topic by resuming it.
    pub async fn switch_to_session(
        &self,
        thread_id: i32,
        record: &SessionRecord,
    ) -> Result<String> {
        // Deactivate current session (drop it)
        if let Some(mut entry) = self.topics.get_mut(&thread_id) {
            entry.active = None;
        }

        self.enqueue_start_session(
            thread_id,
            record.project_path.clone(),
            record.agent_command.clone(),
            record.agent_name.clone(),
            Some(record.acp_session_id.clone()),
        )
        .await
    }

    /// Restore a previously persisted session. Skips topic creation since the topic already exists.
    async fn restore_session(&self, thread_id: i32, record: &SessionRecord) -> Result<()> {
        let acp_session_id = self
            .enqueue_start_session(
                thread_id,
                record.project_path.clone(),
                record.agent_command.clone(),
                record.agent_name.clone(),
                Some(record.acp_session_id.clone()),
            )
            .await?;

        // Reopen the topic in case it was closed
        let _ = self
            .bot
            .reopen_forum_topic(
                ChatId(self.config.chat_id),
                teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)),
            )
            .await;

        tracing::info!(
            "Restored session {} (thread {}, acp {})",
            record.project_path.display(),
            thread_id,
            acp_session_id
        );
        Ok(())
    }

    /// Common logic for starting a session (new or restored).
    async fn enqueue_start_session(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent_cmd: String,
        agent_name: Option<String>,
        existing_acp_session_id: Option<String>,
    ) -> Result<String> {
        let (result_tx, result_rx) = oneshot::channel();
        self.local_start_tx
            .send(StartSessionRequest {
                thread_id,
                project_path,
                agent_cmd,
                agent_name,
                existing_acp_session_id,
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("Daemon session starter is unavailable"))?;

        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("Daemon session starter task exited"))?
    }

    /// Common logic for starting a session (new or restored).
    /// Spawns event consumer and agent task, waits for ACP init, inserts into DashMap.
    async fn start_session_local(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent_cmd: String,
        agent_name: Option<String>,
        existing_acp_session_id: Option<String>,
    ) -> Result<String> {
        // Channels
        let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel::<oneshot::Sender<Result<()>>>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AgentEvent>();
        let available_commands = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let status = Arc::new(tokio::sync::Mutex::new(SessionStatus::Initializing));
        let mcp_session = Arc::new(
            mcp::McpSession::new(
                self.bot.clone(),
                self.telegraph.clone(),
                ChatId(self.config.chat_id),
                thread_id,
                project_path.clone(),
            )
            .await?,
        );
        let mcp_session_id = mcp_session.id.clone();
        let mcp_servers = build_mcp_servers(&mcp_session_id, &self.config.socket_path)?;

        // Spawn the event consumer within LocalSet.
        let bot = self.bot.clone();
        let chat_id = ChatId(self.config.chat_id);
        tokio::task::spawn_local(session::run_event_consumer(
            bot,
            chat_id,
            thread_id,
            event_rx,
            available_commands.clone(),
        ));

        // Insert a SessionEntry with a placeholder acp_session_id into topics
        // *before* spawning the agent. This allows the mcp-relay subprocess to
        // route its MCP `initialize` request through the IPC handler
        // (get_mcp_session_by_id) while ACP init is still in flight.
        let control_state = Arc::new(tokio::sync::Mutex::new(
            session_control::SessionControlState {
                current_permission_mode_id: None,
                permission_modes: Vec::new(),
                model_selector: None,
            },
        ));
        let session_entry = SessionEntry {
            acp_session_id: None, // filled in after init completes
            mcp_session_id: mcp_session_id.clone(),
            mcp: mcp_session.clone(),
            project_path: project_path.clone(),
            agent_command: agent_cmd.clone(),
            agent_name: agent_name.clone(),
            status: status.clone(),
            available_commands: available_commands.clone(),
            control_state: control_state.clone(),
            command_tx: command_tx.clone(),
            cancel_tx: cancel_tx.clone(),
        };
        self.topics
            .entry(thread_id)
            .or_insert_with(|| TopicEntry {
                active: None,
                history: Vec::new(),
            })
            .active = Some(session_entry);

        // Create oneshot for receiving the ACP session ID
        let (result_tx, result_rx) = oneshot::channel();

        // Spawn ACP init + session loop directly in LocalSet.
        tokio::task::spawn_local(spawn_and_run_agent(
            agent_cmd.clone(),
            project_path.clone(),
            event_tx,
            command_rx,
            cancel_rx,
            status.clone(),
            control_state,
            self.bot.clone(),
            ChatId(self.config.chat_id),
            thread_id,
            existing_acp_session_id,
            mcp_servers,
            result_tx,
        ));

        // Wait for ACP init to complete, then fill in the real acp_session_id.
        let acp_session_id = result_rx.await??;
        if let Some(mut topic) = self.topics.get_mut(&thread_id) {
            if let Some(active) = topic.active.as_mut() {
                active.acp_session_id = Some(acp_session_id.clone());
            }
        }

        // Build history record
        let now = Utc::now();
        let record = SessionRecord {
            acp_session_id: acp_session_id.clone(),
            project_path,
            agent_command: agent_cmd,
            agent_name: agent_name.clone(),
            created_at: now,
            last_updated_at: now,
        };

        // Get or create TopicEntry, set active, append to history
        let mut topic = self.topics.entry(thread_id).or_insert_with(|| TopicEntry {
            active: None,
            history: Vec::new(),
        });
        // Check if this session already exists in history (resume case)
        let existing = topic
            .history
            .iter_mut()
            .find(|r| r.acp_session_id == record.acp_session_id);
        if let Some(existing) = existing {
            existing.last_updated_at = now;
            if existing.agent_name.is_none() {
                existing.agent_name = agent_name;
            }
        } else {
            topic.history.push(record);
        }
        drop(topic);

        // Persist after successful init
        self.persist_topics().await;

        Ok(acp_session_id)
    }
}

/// Init phase: spawn agent, initialize/resume ACP session, send result back via oneshot.
/// Run phase: enter session runtime (continues in same task).
async fn spawn_and_run_agent(
    agent_cmd: String,
    project_path: PathBuf,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    cancel_rx: mpsc::UnboundedReceiver<oneshot::Sender<Result<()>>>,
    status: Arc<tokio::sync::Mutex<SessionStatus>>,
    control_state: Arc<tokio::sync::Mutex<session_control::SessionControlState>>,
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    existing_acp_session_id: Option<String>,
    mcp_servers: Vec<acp_sdk::McpServer>,
    result_tx: oneshot::Sender<Result<String>>,
) {
    match init_agent(
        &agent_cmd,
        &project_path,
        event_tx.clone(),
        &existing_acp_session_id,
        mcp_servers,
    )
    .await
    {
        Ok((conn, mut child, bootstrap)) => {
            // Send the ACP session ID back
            let session_id_str = bootstrap.session_id.to_string();
            if result_tx.send(Ok(session_id_str.clone())).is_err() {
                tracing::error!("Failed to send ACP session ID back (receiver dropped)");
                let _ = child.kill().await;
                return;
            }

            {
                let mut s = status.lock().await;
                *s = SessionStatus::Idle;
            }

            // Populate initial control state from bootstrap data
            {
                let mut cs = control_state.lock().await;
                *cs = session_control::build_control_state(
                    &bootstrap.modes,
                    &bootstrap.config_options,
                );
            }

            // Run the session runtime
            let conn = Arc::new(conn);
            session::run_session_runtime(
                conn,
                bootstrap.session_id,
                bot,
                chat_id,
                thread_id,
                command_rx,
                cancel_rx,
                event_tx,
                status,
                control_state,
                bootstrap.modes,
                bootstrap.config_options,
            )
            .await;

            // Clean up child process
            let _ = child.kill().await;
        }
        Err(e) => {
            tracing::error!(
                "Failed to initialize ACP agent (cmd: {}, project: {}): {:#}",
                agent_cmd,
                project_path.display(),
                e
            );
            let _ = result_tx.send(Err(e));
        }
    }
}

/// Spawn agent subprocess, handle IO, and initialize or resume the ACP session.
async fn init_agent(
    agent_cmd: &str,
    project_path: &PathBuf,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    existing_acp_session_id: &Option<String>,
    mcp_servers: Vec<acp_sdk::McpServer>,
) -> Result<(
    agent_client_protocol::ClientSideConnection,
    tokio::process::Child,
    acp::SessionBootstrap,
)> {
    let is_loading = Arc::new(AtomicBool::new(existing_acp_session_id.is_some()));
    let io_event_tx = event_tx.clone();

    let (conn, child, stderr_tail, handle_io) =
        acp::spawn_agent(agent_cmd, project_path, event_tx, is_loading.clone()).map_err(|e| {
            anyhow::anyhow!(
                "failed to spawn ACP agent process (cmd: {}, project: {}): {:#}",
                agent_cmd,
                project_path.display(),
                e
            )
        })?;

    tokio::task::spawn_local(async move {
        if let Err(e) = handle_io.await {
            tracing::error!("ACP IO error: {e}");
            let _ = io_event_tx.send(AgentEvent::Error(format!("Agent connection error: {e}")));
        }
    });

    let bootstrap = if let Some(old_id) = existing_acp_session_id.clone() {
        let session = acp::resume_session(&conn, project_path, old_id.clone(), mcp_servers)
            .await
            .map_err(|e| {
                let stderr_tail = acp::format_stderr_tail(&stderr_tail);
                anyhow::anyhow!(
                    "ACP resume_session failed (cmd: {}, project: {}, previous_session: {}): {:#}{}",
                    agent_cmd,
                    project_path.display(),
                    old_id,
                    e,
                    stderr_tail
                )
            })?;
        is_loading.store(false, Ordering::Relaxed);
        session
    } else {
        acp::init_session(&conn, project_path, mcp_servers)
            .await
            .map_err(|e| {
                let stderr_tail = acp::format_stderr_tail(&stderr_tail);
                anyhow::anyhow!(
                    "ACP init_session failed (cmd: {}, project: {}): {:#}{}",
                    agent_cmd,
                    project_path.display(),
                    e,
                    stderr_tail
                )
            })?
    };

    Ok((conn, child, bootstrap))
}

fn build_mcp_servers(
    mcp_session_id: &str,
    socket_path: &PathBuf,
) -> Result<Vec<acp_sdk::McpServer>> {
    let exe_path = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("Failed to resolve current executable: {e}"))?;
    let args = vec![
        "mcp-relay".to_string(),
        "--session".to_string(),
        mcp_session_id.to_string(),
        "--socket".to_string(),
        socket_path.to_string_lossy().to_string(),
    ];
    let server = acp_sdk::McpServer::Stdio(
        acp_sdk::McpServerStdio::new("telegram-acp-relay", exe_path).args(args),
    );
    Ok(vec![server])
}

fn mcp_expects_response(value: &JsonValue) -> bool {
    match value {
        JsonValue::Object(map) => map.get("id").map(|id| !id.is_null()).unwrap_or(false),
        JsonValue::Array(items) => items.iter().any(|item| {
            item.as_object()
                .and_then(|map| map.get("id"))
                .map(|id| !id.is_null())
                .unwrap_or(false)
        }),
        _ => false,
    }
}

/// Run the daemon: start bot + IPC listener.
pub async fn run_daemon(config: Config) -> Result<()> {
    tracing::info!("Starting telegram-acp daemon");

    let bot = Bot::new(&config.bot_token);
    let telegraph =
        Arc::new(crate::telegraph::create_account(config.telegraph_author.as_deref()).await?);
    let (local_start_tx, mut local_start_rx) = mpsc::unbounded_channel::<StartSessionRequest>();

    let daemon = Arc::new(DaemonHandle {
        config: config.clone(),
        bot: bot.clone(),
        telegraph,
        local_start_tx,
        topics: DashMap::new(),
    });

    let local_daemon = daemon.clone();
    tokio::task::spawn_local(async move {
        while let Some(req) = local_start_rx.recv().await {
            let local_daemon = local_daemon.clone();
            tokio::task::spawn_local(async move {
                let res = local_daemon
                    .start_session_local(
                        req.thread_id,
                        req.project_path,
                        req.agent_cmd,
                        req.agent_name,
                        req.existing_acp_session_id,
                    )
                    .await;
                let _ = req.result_tx.send(res);
            });
        }
    });

    // Spawn IPC server before restoring sessions so that mcp-relay subprocesses
    // spawned during load_session can connect to the socket immediately.
    let ipc_daemon = daemon.clone();
    let socket_path = config.socket_path.clone();
    tokio::task::spawn_local(async move {
        if let Err(e) = crate::ipc::run_ipc_server(&socket_path, move |cmd| {
            let daemon = ipc_daemon.clone();
            Box::pin(async move {
                use crate::types::{DaemonCommand, DaemonResponse};
                match cmd {
                    DaemonCommand::NewSession {
                        path,
                        prompt,
                        agent,
                    } => match daemon
                        .spawn_session(path.to_string_lossy().to_string(), prompt, agent)
                        .await
                    {
                        Ok((acp_session_id, thread_id)) => DaemonResponse::SessionCreated {
                            acp_session_id,
                            topic_url: format!(
                                "https://t.me/c/{}/{}",
                                daemon.config.chat_id, thread_id
                            ),
                        },
                        Err(e) => DaemonResponse::Error {
                            message: e.to_string(),
                        },
                    },
                    DaemonCommand::McpMessage {
                        session_id,
                        payload,
                    } => match daemon.handle_mcp_message(&session_id, &payload).await {
                        Ok(payload) => DaemonResponse::McpResponse { payload },
                        Err(e) => DaemonResponse::Error {
                            message: e.to_string(),
                        },
                    },
                    DaemonCommand::ListSessions => {
                        let mut sessions = Vec::new();
                        for entry in daemon.topics.iter() {
                            let thread_id = *entry.key();
                            let topic = entry.value();
                            if let Some(active) = &topic.active {
                                let status = *active.status.lock().await;
                                if let Some(acp_session_id) = active.acp_session_id.clone() {
                                    sessions.push(crate::types::SessionInfo {
                                        acp_session_id,
                                        project_path: active.project_path.clone(),
                                        agent_command: active.agent_command.clone(),
                                        status,
                                        thread_id,
                                    });
                                }
                            }
                        }
                        DaemonResponse::SessionList { sessions }
                    }
                }
            })
        })
        .await
        {
            tracing::error!("IPC server error: {e}");
        }
    });

    // Restore persisted topics
    let persisted = persistence::load_topics();
    if !persisted.is_empty() {
        tracing::info!("Restoring {} persisted topic(s)", persisted.len());

        // Insert topic entries with history (active=None initially)
        for pt in &persisted {
            daemon.topics.insert(
                pt.thread_id,
                TopicEntry {
                    active: None,
                    history: pt.sessions.clone(),
                },
            );
        }

        // Restore active sessions
        let restore_results = join_all(persisted.iter().filter_map(|pt| {
            let active_id = pt.active_session_id.as_ref()?;
            let record = pt
                .sessions
                .iter()
                .find(|r| &r.acp_session_id == active_id)?;
            let daemon = daemon.clone();
            let thread_id = pt.thread_id;
            let record = record.clone();
            Some(async move {
                let restore_result = daemon.restore_session(thread_id, &record).await;
                (thread_id, record, restore_result)
            })
        }))
        .await;

        for (thread_id, record, restore_result) in restore_results {
            if let Err(e) = restore_result {
                tracing::error!(
                    "Failed to restore session for {} (thread {}): {e}",
                    record.project_path.display(),
                    thread_id
                );
            }
        }
        // Re-persist to update any sessions that got new ACP IDs from fallback
        daemon.persist_topics().await;
    }

    // Run Telegram bot (blocks)
    telegram::run_bot(bot, daemon).await;

    Ok(())
}
