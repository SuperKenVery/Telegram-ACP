use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use agent_client_protocol as acp_sdk;
use anyhow::Result;
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
use crate::persistence;
use crate::session;
use crate::session_control::SessionCommand;
use crate::telegram;
use crate::types::{AgentEvent, SessionInfo, SessionStatus};

/// Shared daemon state, accessible from Telegram handlers and IPC.
pub struct DaemonHandle {
    pub config: Config,
    pub bot: Bot,
    #[allow(dead_code)]
    pub telegraph: Telegraph,
    /// Relay for starting ACP sessions inside the daemon's LocalSet task.
    local_start_tx: mpsc::UnboundedSender<StartSessionRequest>,
    /// thread_id -> SessionEntry
    pub sessions: DashMap<i32, SessionEntry>,
}

pub struct SessionEntry {
    pub acp_session_id: String,
    pub mcp_session_id: String,
    pub mcp: Arc<mcp::McpSession>,
    pub project_path: PathBuf,
    pub agent_command: String,
    pub status: Arc<tokio::sync::Mutex<SessionStatus>>,
    pub available_commands: Arc<tokio::sync::Mutex<Vec<acp_sdk::AvailableCommand>>>,
    pub command_tx: mpsc::UnboundedSender<SessionCommand>,
    pub cancel_tx: mpsc::UnboundedSender<oneshot::Sender<Result<()>>>,
}

struct StartSessionRequest {
    thread_id: i32,
    project_path: PathBuf,
    agent_cmd: String,
    existing_acp_session_id: Option<String>,
    result_tx: oneshot::Sender<Result<String>>,
}

impl DaemonHandle {
    fn get_mcp_session_by_id(&self, session_id: &str) -> Option<Arc<mcp::McpSession>> {
        self.sessions.iter().find_map(|entry| {
            if entry.mcp_session_id == session_id {
                Some(entry.mcp.clone())
            } else {
                None
            }
        })
    }

    pub async fn handle_mcp_message(
        &self,
        session_id: &str,
        payload: &str,
    ) -> Result<Option<String>> {
        let mcp_session = self
            .get_mcp_session_by_id(session_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown MCP session id: {}", session_id))?;

        let payload_value: JsonValue = serde_json::from_str(payload)
            .map_err(|e| anyhow::anyhow!("Invalid MCP payload: {e}"))?;
        let expects_response = mcp_expects_response(&payload_value);
        let message: RxJsonRpcMessage<RoleServer> = serde_json::from_value(payload_value)
            .map_err(|e| anyhow::anyhow!("Failed to decode MCP payload: {e}"))?;

        mcp_session.send(message).await?;

        if expects_response {
            if let Some(response) = mcp_session.next_response().await {
                let payload = serde_json::to_string(&response)?;
                Ok(Some(payload))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn cancel_session(&self, thread_id: i32) -> Result<()> {
        let entry = self
            .sessions
            .get(&thread_id)
            .ok_or_else(|| anyhow::anyhow!("No active session in this topic"))?;
        let (result_tx, result_rx) = oneshot::channel();
        entry
            .cancel_tx
            .send(result_tx)
            .map_err(|_| anyhow::anyhow!("Session cancel channel closed"))?;
        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("Cancel request dropped"))?
    }

    /// Remove a session from in-memory state and persisted storage.
    pub async fn remove_session(&self, thread_id: i32) -> Option<SessionEntry> {
        let (_, entry) = self.sessions.remove(&thread_id)?;
        self.persist_sessions().await;
        Some(entry)
    }

    pub fn get_session_command_tx_by_thread(
        &self,
        thread_id: i32,
    ) -> Option<mpsc::UnboundedSender<SessionCommand>> {
        self.sessions.get(&thread_id).map(|e| e.command_tx.clone())
    }

    pub fn get_session_project_path_by_thread(&self, thread_id: i32) -> Option<PathBuf> {
        self.sessions
            .get(&thread_id)
            .map(|entry| entry.project_path.clone())
    }

    pub async fn get_available_commands_by_thread(
        &self,
        thread_id: i32,
    ) -> Option<Vec<acp_sdk::AvailableCommand>> {
        let available_commands = self
            .sessions
            .get(&thread_id)
            .map(|entry| entry.available_commands.clone())?;
        let commands = available_commands.lock().await.clone();
        Some(commands)
    }

    pub async fn list_sessions(&self) -> Vec<SessionInfo> {
        let entries: Vec<_> = self
            .sessions
            .iter()
            .map(|entry| {
                let thread_id = *entry.key();
                let e = entry.value();
                (
                    e.acp_session_id.clone(),
                    e.project_path.clone(),
                    e.agent_command.clone(),
                    e.status.clone(),
                    thread_id,
                )
            })
            .collect();

        let mut result = Vec::with_capacity(entries.len());
        for (acp_session_id, project_path, agent_command, status, thread_id) in entries {
            let status = *status.lock().await;
            result.push(SessionInfo {
                acp_session_id,
                project_path,
                status,
                thread_id,
                agent_command,
            });
        }
        result
    }

    /// Persist current sessions to disk.
    pub async fn persist_sessions(&self) {
        let sessions = self.list_sessions().await;
        if let Err(e) = persistence::save_sessions(&sessions) {
            tracing::error!("Failed to persist sessions: {e}");
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
        let agent_cmd = self.config.resolve_agent_command(agent.as_deref())?;

        // Create forum topic
        let topic_name = project_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| path.clone());
        let topic = self
            .bot
            .create_forum_topic(ChatId(self.config.chat_id), &topic_name, 0x6FB9F0, "")
            .await?;
        let thread_id = topic.thread_id.0 .0;

        let acp_session_id = match self
            .enqueue_start_session(thread_id, project_path, agent_cmd, None)
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

    /// Restore a previously persisted session. Skips topic creation since the topic already exists.
    pub async fn restore_session(&self, info: &SessionInfo) -> Result<()> {
        let acp_session_id = self
            .enqueue_start_session(
                info.thread_id,
                info.project_path.clone(),
                info.agent_command.clone(),
                Some(info.acp_session_id.clone()),
            )
            .await?;

        // Reopen the topic in case it was closed
        let _ = self
            .bot
            .reopen_forum_topic(
                ChatId(self.config.chat_id),
                teloxide::types::ThreadId(teloxide::types::MessageId(info.thread_id)),
            )
            .await;

        tracing::info!(
            "Restored session {} (thread {}, acp {})",
            info.project_path.display(),
            info.thread_id,
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
        existing_acp_session_id: Option<String>,
    ) -> Result<String> {
        let (result_tx, result_rx) = oneshot::channel();
        self.local_start_tx
            .send(StartSessionRequest {
                thread_id,
                project_path,
                agent_cmd,
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
        existing_acp_session_id: Option<String>,
    ) -> Result<String> {
        // Channels
        let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel::<oneshot::Sender<Result<()>>>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AgentEvent>();
        let available_commands = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let status = Arc::new(tokio::sync::Mutex::new(SessionStatus::Initializing));
        let mcp_session = Arc::new(mcp::McpSession::new().await?);
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
            self.bot.clone(),
            ChatId(self.config.chat_id),
            thread_id,
            existing_acp_session_id,
            mcp_servers,
            result_tx,
        ));

        // Wait for ACP init to complete
        let acp_session_id = result_rx.await??;

        // Store session
        self.sessions.insert(
            thread_id,
            SessionEntry {
                acp_session_id: acp_session_id.clone(),
                mcp_session_id,
                mcp: mcp_session,
                project_path,
                agent_command: agent_cmd,
                status,
                available_commands,
                command_tx,
                cancel_tx,
            },
        );

        // Persist after successful init
        self.persist_sessions().await;

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
        JsonValue::Object(map) => map
            .get("id")
            .map(|id| !id.is_null())
            .unwrap_or(false),
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
    let telegraph = crate::telegraph::create_account(config.telegraph_author.as_deref()).await?;
    let (local_start_tx, mut local_start_rx) = mpsc::unbounded_channel::<StartSessionRequest>();

    let daemon = Arc::new(DaemonHandle {
        config: config.clone(),
        bot: bot.clone(),
        telegraph,
        local_start_tx,
        sessions: DashMap::new(),
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
                        req.existing_acp_session_id,
                    )
                    .await;
                let _ = req.result_tx.send(res);
            });
        }
    });

    // Restore persisted sessions
    let persisted = persistence::load_sessions();
    if !persisted.is_empty() {
        tracing::info!("Restoring {} persisted session(s)", persisted.len());
        let restore_results = join_all(persisted.into_iter().map(|info| {
            let daemon = daemon.clone();
            async move {
                let restore_result = daemon.restore_session(&info).await;
                (info, restore_result)
            }
        }))
        .await;
        for (info, restore_result) in restore_results {
            if let Err(e) = restore_result {
                tracing::error!(
                    "Failed to restore session for {} (thread {}): {e}",
                    info.project_path.display(),
                    info.thread_id
                );
            }
        }
        // Re-persist to update any sessions that got new ACP IDs from fallback
        daemon.persist_sessions().await;
    }

    // Spawn IPC server
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
                    DaemonCommand::McpMessage { session_id, payload } => {
                        match daemon.handle_mcp_message(&session_id, &payload).await {
                            Ok(payload) => DaemonResponse::McpResponse { payload },
                            Err(e) => DaemonResponse::Error {
                                message: e.to_string(),
                            },
                        }
                    }
                    DaemonCommand::ListSessions => DaemonResponse::SessionList {
                        sessions: daemon.list_sessions().await,
                    },
                }
            })
        })
        .await
        {
            tracing::error!("IPC server error: {e}");
        }
    });

    // Run Telegram bot (blocks)
    telegram::run_bot(bot, daemon).await;

    Ok(())
}
