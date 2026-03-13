use agent_client_protocol as acp;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// === IPC Protocol ===

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DaemonCommand {
    NewSession {
        path: PathBuf,
        prompt: Option<String>,
        agent: Option<String>,
    },
    McpMessage {
        session_id: String,
        payload: String,
    },
    ListSessions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DaemonResponse {
    SessionCreated {
        acp_session_id: String,
        topic_url: String,
    },
    McpResponse {
        payload: Option<String>,
    },
    SessionList {
        sessions: Vec<SessionInfo>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub acp_session_id: String,
    pub project_path: PathBuf,
    pub status: SessionStatus,
    pub thread_id: i32,
    pub agent_command: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionStatus {
    Initializing,
    Idle,
    Prompting,
    Finished,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub acp_session_id: String,
    pub project_path: PathBuf,
    pub agent_command: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

// === Agent Events (sent from ACP Client impl to Telegram sender) ===

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AgentEvent {
    Working,
    Update(acp::SessionUpdate),
    Finished(String),
    Error(String),
}
