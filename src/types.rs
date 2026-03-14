use agent_client_protocol as acp;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// === Session Control State ===

#[derive(Debug, Clone)]
pub struct SelectionOption {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ModelSelectorState {
    pub config_id: String,
    pub current_value_id: String,
    pub options: Vec<SelectionOption>,
}

#[derive(Debug, Clone)]
pub struct SessionControlState {
    pub current_permission_mode_id: Option<String>,
    pub permission_modes: Vec<SelectionOption>,
    pub model_selector: Option<ModelSelectorState>,
}

fn flatten_select_options(
    options: &acp::SessionConfigSelectOptions,
) -> Vec<&acp::SessionConfigSelectOption> {
    match options {
        acp::SessionConfigSelectOptions::Ungrouped(values) => values.iter().collect(),
        acp::SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| group.options.iter())
            .collect(),
        _ => Vec::new(),
    }
}

pub fn build_control_state(
    mode_state: &Option<acp::SessionModeState>,
    config_options: &[acp::SessionConfigOption],
) -> SessionControlState {
    let (current_permission_mode_id, permission_modes) = match mode_state {
        Some(state) => (
            Some(state.current_mode_id.0.to_string()),
            state
                .available_modes
                .iter()
                .map(|mode| SelectionOption {
                    id: mode.id.0.to_string(),
                    name: mode.name.clone(),
                })
                .collect(),
        ),
        None => (None, Vec::new()),
    };

    let model_selector = config_options
        .iter()
        .find(|opt| matches!(opt.category, Some(acp::SessionConfigOptionCategory::Model)))
        .and_then(|model_option| match &model_option.kind {
            acp::SessionConfigKind::Select(select) => Some(ModelSelectorState {
                config_id: model_option.id.0.to_string(),
                current_value_id: select.current_value.0.to_string(),
                options: flatten_select_options(&select.options)
                    .into_iter()
                    .map(|value| SelectionOption {
                        id: value.value.0.to_string(),
                        name: value.name.clone(),
                    })
                    .collect(),
            }),
            _ => None,
        });

    SessionControlState {
        current_permission_mode_id,
        permission_modes,
        model_selector,
    }
}

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
