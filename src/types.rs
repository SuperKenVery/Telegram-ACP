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
    ListSessions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DaemonResponse {
    SessionCreated {
        session_id: String,
        topic_url: String,
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
    pub session_id: String,
    pub project_path: PathBuf,
    pub status: SessionStatus,
    pub thread_id: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionStatus {
    Initializing,
    Idle,
    Prompting,
    Finished,
    Error,
}

// === Agent Events (sent from ACP Client impl to Telegram sender) ===

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AgentEvent {
    Working,
    TextMessage(String),
    ToolCall {
        id: String,
        name: String,
    },
    ToolCallUpdate {
        id: String,
        name: String,
        output: Option<String>,
    },
    Finished(String),
    Error(String),
}
