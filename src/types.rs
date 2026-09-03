use agent_client_protocol as acp;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
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
#[serde(rename_all = "lowercase")]
pub enum VerboseMode {
    Off,
    Compact,
    On,
}

impl Default for VerboseMode {
    fn default() -> Self {
        Self::On
    }
}

impl std::fmt::Display for VerboseMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Off => "off",
            Self::Compact => "compact",
            Self::On => "on",
        };
        f.write_str(value)
    }
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

/// Arguments for creating a new session, shared between the /new slash command
/// and the MCP `create_session` tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct NewSessionArgs {
    /// Agent name from config (e.g. claude, codex). Falls back to default_agent if not set.
    pub agent: Option<String>,
    /// Project path. If not provided, the agent's current working directory is used.
    pub project_path: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AgentEvent {
    Working,
    Update(acp::SessionUpdate),
    Finished(String),
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::VerboseMode;

    #[test]
    fn verbose_defaults_to_on() {
        assert_eq!(VerboseMode::default(), VerboseMode::On);
    }
}
