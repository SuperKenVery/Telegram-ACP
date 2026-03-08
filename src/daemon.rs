use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use teloxide::prelude::*;
use telegraph_rs::Telegraph;
use tokio::sync::mpsc;

use crate::acp;
use crate::config::Config;
use crate::session;
use crate::telegram;
use crate::types::{AgentEvent, SessionInfo, SessionStatus};

/// Request to spawn an agent, relayed to the LocalSet context.
struct SpawnLocalRequest {
    agent_cmd: String,
    project_path: PathBuf,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    user_rx: mpsc::UnboundedReceiver<String>,
    status: Arc<tokio::sync::Mutex<SessionStatus>>,
    session_id: String,
}

/// Shared daemon state, accessible from Telegram handlers and IPC.
pub struct DaemonHandle {
    pub config: Config,
    pub bot: Bot,
    #[allow(dead_code)]
    pub telegraph: Telegraph,
    /// session_id -> AgentSession metadata + user_tx
    pub sessions: DashMap<String, SessionEntry>,
    /// thread_id (i32) -> session_id
    pub thread_to_session: DashMap<i32, String>,
    /// Channel to relay spawn_local work into the LocalSet.
    spawn_tx: mpsc::UnboundedSender<SpawnLocalRequest>,
}

pub struct SessionEntry {
    pub session_id: String,
    pub project_path: PathBuf,
    pub thread_id: i32,
    pub status: Arc<tokio::sync::Mutex<SessionStatus>>,
    pub user_tx: mpsc::UnboundedSender<String>,
}

impl DaemonHandle {
    pub fn get_session_tx(&self, session_id: &str) -> Option<mpsc::UnboundedSender<String>> {
        self.sessions.get(session_id).map(|e| e.user_tx.clone())
    }

    pub async fn list_sessions(&self) -> Vec<SessionInfo> {
        let entries: Vec<_> = self
            .sessions
            .iter()
            .map(|entry| {
                let e = entry.value();
                (
                    e.session_id.clone(),
                    e.project_path.clone(),
                    e.status.clone(),
                    e.thread_id,
                )
            })
            .collect();

        let mut result = Vec::with_capacity(entries.len());
        for (session_id, project_path, status, thread_id) in entries {
            let status = *status.lock().await;
            result.push(SessionInfo {
                session_id,
                project_path,
                status,
                thread_id,
            });
        }
        result
    }

    /// Spawn a new agent session: create topic, spawn agent, wire everything up.
    pub async fn spawn_session(
        &self,
        path: String,
        _prompt: Option<String>,
        agent_cmd: Option<String>,
    ) -> Result<(String, i32)> {
        let session_id = uuid::Uuid::new_v4().to_string();
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

        // Channels
        let (user_tx, user_rx) = mpsc::unbounded_channel::<String>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AgentEvent>();

        let status = Arc::new(tokio::sync::Mutex::new(SessionStatus::Initializing));

        // Store session
        self.sessions.insert(
            session_id.clone(),
            SessionEntry {
                session_id: session_id.clone(),
                project_path: project_path.clone(),
                thread_id,
                status: status.clone(),
                user_tx: user_tx.clone(),
            },
        );
        self.thread_to_session
            .insert(thread_id, session_id.clone());

        // Spawn the event consumer (sends AgentEvents to Telegram) — regular tokio::spawn is fine
        let bot = self.bot.clone();
        let chat_id = ChatId(self.config.chat_id);
        tokio::spawn(telegram::run_event_consumer(
            bot,
            chat_id,
            thread_id,
            event_rx,
        ));

        // Relay the agent spawn to the LocalSet via channel
        self.spawn_tx.send(SpawnLocalRequest {
            agent_cmd,
            project_path,
            event_tx,
            user_rx,
            status,
            session_id: session_id.clone(),
        })?;

        Ok((session_id, thread_id))
    }
}

async fn spawn_and_run_agent(
    agent_cmd: &str,
    project_path: &PathBuf,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    user_rx: mpsc::UnboundedReceiver<String>,
    status: Arc<tokio::sync::Mutex<SessionStatus>>,
) -> Result<()> {
    let (conn, mut child, handle_io) =
        acp::spawn_agent(agent_cmd, project_path, event_tx.clone())?;

    tokio::task::spawn_local(async {
        if let Err(e) = handle_io.await {
            tracing::error!("ACP IO error: {e}");
        }
    });

    // Initialize ACP session
    let acp_session_id = acp::init_session(&conn, project_path).await?;

    {
        let mut s = status.lock().await;
        *s = SessionStatus::Idle;
    }

    // Run the prompt loop
    let conn = Arc::new(conn);
    session::run_prompt_loop(conn, acp_session_id, user_rx, event_tx, status).await;

    // Clean up child process
    let _ = child.kill().await;

    Ok(())
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
        thread_to_session: DashMap::new(),
        spawn_tx,
    });

    // LocalSet task: receives spawn requests and runs them with spawn_local
    tokio::task::spawn_local(async move {
        while let Some(req) = spawn_rx.recv().await {
            let session_id = req.session_id.clone();
            let event_tx = req.event_tx.clone();
            tokio::task::spawn_local(async move {
                match spawn_and_run_agent(
                    &req.agent_cmd,
                    &req.project_path,
                    req.event_tx,
                    req.user_rx,
                    req.status,
                )
                .await
                {
                    Ok(()) => {
                        tracing::info!("Session {} finished", session_id);
                    }
                    Err(e) => {
                        tracing::error!("Session {} failed: {e}", session_id);
                        let _ = event_tx.send(AgentEvent::Error(format!("Session failed: {e}")));
                    }
                }
            });
        }
    });

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
                        .spawn_session(
                            path.to_string_lossy().to_string(),
                            prompt,
                            agent,
                        )
                        .await
                    {
                        Ok((session_id, thread_id)) => DaemonResponse::SessionCreated {
                            session_id,
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
