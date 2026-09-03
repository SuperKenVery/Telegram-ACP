use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::types::{SessionInfo, SessionRecord, VerboseMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedTopic {
    pub thread_id: i32,
    pub active_session_id: Option<String>,
    pub sessions: Vec<SessionRecord>,
    #[serde(default)]
    pub verbose: VerboseMode,
}

fn sessions_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".config")
        .join("telegram-acp")
        .join("sessions.json")
}

pub fn save_topics(topics: &[PersistedTopic]) -> Result<()> {
    let path = sessions_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let data = serde_json::to_string_pretty(topics)?;
    std::fs::write(&tmp_path, data)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

pub fn load_topics() -> Vec<PersistedTopic> {
    let path = sessions_path();
    let data = match std::fs::read_to_string(&path) {
        Ok(data) => data,
        Err(_) => return Vec::new(),
    };

    // Try new format first
    if let Ok(topics) = serde_json::from_str::<Vec<PersistedTopic>>(&data) {
        return topics;
    }

    // Fall back to legacy Vec<SessionInfo> format
    if let Ok(sessions) = serde_json::from_str::<Vec<SessionInfo>>(&data) {
        tracing::info!(
            "Migrating {} legacy session(s) to topic format",
            sessions.len()
        );
        return sessions
            .into_iter()
            .map(|s| {
                let now = chrono::Utc::now();
                let record = SessionRecord {
                    acp_session_id: s.acp_session_id.clone(),
                    project_path: s.project_path,
                    agent_command: s.agent_command,
                    agent_name: None,
                    created_at: now,
                    last_updated_at: now,
                };
                PersistedTopic {
                    thread_id: s.thread_id,
                    active_session_id: Some(s.acp_session_id),
                    sessions: vec![record],
                    verbose: VerboseMode::default(),
                }
            })
            .collect();
    }

    tracing::warn!("Failed to parse sessions file in any known format");
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::PersistedTopic;
    use crate::types::VerboseMode;

    #[test]
    fn old_topics_default_to_on() {
        let topic: PersistedTopic = serde_json::from_str(
            r#"{
                "thread_id": 1,
                "active_session_id": null,
                "sessions": []
            }"#,
        )
        .unwrap();

        assert_eq!(topic.verbose, VerboseMode::On);
    }
}
