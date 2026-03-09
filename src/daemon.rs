use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use telegraph_rs::Telegraph;
use teloxide::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::acp;
use crate::config::Config;
use crate::persistence;
use crate::session;
use crate::session_control::SessionCommand;
use crate::telegram;
use crate::types::{AgentEvent, SessionInfo, SessionStatus};

/// Request to spawn an agent, relayed to the LocalSet context.
struct SpawnLocalRequest {
    agent_cmd: String,
    project_path: PathBuf,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    status: Arc<tokio::sync::Mutex<SessionStatus>>,
    /// If Some, resume this ACP session instead of creating a new one.
    existing_acp_session_id: Option<String>,
    /// Sends back the ACP session ID (or error) after init completes.
    result_tx: oneshot::Sender<Result<String>>,
}

/// Shared daemon state, accessible from Telegram handlers and IPC.
pub struct DaemonHandle {
    pub config: Config,
    pub bot: Bot,
    #[allow(dead_code)]
    pub telegraph: Telegraph,
    /// thread_id -> SessionEntry
    pub sessions: DashMap<i32, SessionEntry>,
    /// Channel to relay spawn_local work into the LocalSet.
    spawn_tx: mpsc::UnboundedSender<SpawnLocalRequest>,
}

pub struct SessionEntry {
    pub acp_session_id: String,
    pub project_path: PathBuf,
    pub agent_command: String,
    pub status: Arc<tokio::sync::Mutex<SessionStatus>>,
    pub command_tx: mpsc::UnboundedSender<SessionCommand>,
}

impl DaemonHandle {
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
        agent_cmd: Option<String>,
    ) -> Result<(String, i32)> {
        let project_path = PathBuf::from(&path);
        let agent_cmd = agent_cmd.unwrap_or_else(|| self.config.default_agent_command.clone());

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

        let acp_session_id = self
            .start_session(thread_id, project_path, agent_cmd, None)
            .await?;

        Ok((acp_session_id, thread_id))
    }

    /// Restore a previously persisted session. Skips topic creation since the topic already exists.
    pub async fn restore_session(&self, info: &SessionInfo) -> Result<()> {
        let acp_session_id = self
            .start_session(
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
    /// Spawns event consumer, sends SpawnLocalRequest, waits for ACP init, inserts into DashMap.
    async fn start_session(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent_cmd: String,
        existing_acp_session_id: Option<String>,
    ) -> Result<String> {
        // Channels
        let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AgentEvent>();

        let status = Arc::new(tokio::sync::Mutex::new(SessionStatus::Initializing));

        // Spawn the event consumer (sends AgentEvents to Telegram) — regular tokio::spawn is fine
        let bot = self.bot.clone();
        let chat_id = ChatId(self.config.chat_id);
        tokio::spawn(telegram::run_event_consumer(
            bot, chat_id, thread_id, event_rx,
        ));

        // Create oneshot for receiving the ACP session ID
        let (result_tx, result_rx) = oneshot::channel();

        // Relay the agent spawn to the LocalSet via channel
        self.spawn_tx.send(SpawnLocalRequest {
            agent_cmd: agent_cmd.clone(),
            project_path: project_path.clone(),
            event_tx,
            command_rx,
            status: status.clone(),
            existing_acp_session_id,
            result_tx,
        })?;

        // Wait for ACP init to complete
        let acp_session_id = result_rx.await??;

        // Store session
        self.sessions.insert(
            thread_id,
            SessionEntry {
                acp_session_id: acp_session_id.clone(),
                project_path,
                agent_command: agent_cmd,
                status,
                command_tx,
            },
        );

        // Persist after successful init
        self.persist_sessions().await;

        Ok(acp_session_id)
    }
}

/// Init phase: spawn agent, initialize/resume ACP session, send result back via oneshot.
/// Run phase: enter prompt loop (continues in same task).
async fn spawn_and_run_agent(
    agent_cmd: &str,
    project_path: &PathBuf,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    status: Arc<tokio::sync::Mutex<SessionStatus>>,
    existing_acp_session_id: Option<String>,
    result_tx: oneshot::Sender<Result<String>>,
) {
    match init_agent(
        agent_cmd,
        project_path,
        event_tx.clone(),
        &existing_acp_session_id,
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

            // Run the prompt loop
            let conn = Arc::new(conn);
            session::run_prompt_loop(
                conn,
                bootstrap.session_id,
                command_rx,
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
            let _ = result_tx.send(Err(anyhow::anyhow!("{e}")));
        }
    }
}

/// Spawn agent subprocess, handle IO, and initialize or resume the ACP session.
async fn init_agent(
    agent_cmd: &str,
    project_path: &PathBuf,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    existing_acp_session_id: &Option<String>,
) -> Result<(
    agent_client_protocol::ClientSideConnection,
    tokio::process::Child,
    acp::SessionBootstrap,
)> {
    let is_loading = Arc::new(AtomicBool::new(existing_acp_session_id.is_some()));

    let (conn, child, handle_io) =
        acp::spawn_agent(agent_cmd, project_path, event_tx, is_loading.clone())?;

    tokio::task::spawn_local(async {
        if let Err(e) = handle_io.await {
            tracing::error!("ACP IO error: {e}");
        }
    });

    let bootstrap = if let Some(old_id) = existing_acp_session_id.clone() {
        let session = acp::resume_session(&conn, project_path, old_id).await?;
        is_loading.store(false, Ordering::Relaxed);
        session
    } else {
        acp::init_session(&conn, project_path).await?
    };

    Ok((conn, child, bootstrap))
}

/// Run the daemon: start bot + IPC listener.
pub async fn run_daemon(config: Config) -> Result<()> {
    tracing::info!("Starting telegram-acp daemon");

    let bot = Bot::new(&config.bot_token);
    let telegraph = crate::telegraph::create_account(config.telegraph_author.as_deref()).await?;

    let (spawn_tx, mut spawn_rx) = mpsc::unbounded_channel::<SpawnLocalRequest>();

    let daemon = Arc::new(DaemonHandle {
        config: config.clone(),
        bot: bot.clone(),
        telegraph,
        sessions: DashMap::new(),
        spawn_tx,
    });

    // LocalSet task: receives spawn requests and runs them with spawn_local
    tokio::task::spawn_local(async move {
        while let Some(req) = spawn_rx.recv().await {
            let event_tx = req.event_tx.clone();
            let agent_cmd = req.agent_cmd.clone();
            tokio::task::spawn_local(async move {
                spawn_and_run_agent(
                    &agent_cmd,
                    &req.project_path,
                    req.event_tx,
                    req.command_rx,
                    req.status,
                    req.existing_acp_session_id,
                    req.result_tx,
                )
                .await;

                // If we get here, the session is done. The event_tx drop will signal the consumer.
                drop(event_tx);
            });
        }
    });

    // Restore persisted sessions
    let persisted = persistence::load_sessions();
    if !persisted.is_empty() {
        tracing::info!("Restoring {} persisted session(s)", persisted.len());
        for info in &persisted {
            if let Err(e) = daemon.restore_session(info).await {
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
    tokio::spawn(async move {
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
