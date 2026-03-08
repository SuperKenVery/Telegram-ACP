use anyhow::Result;
use std::path::PathBuf;

use crate::types::SessionInfo;

fn sessions_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".config")
        .join("telegram-acp")
        .join("sessions.json")
}

pub fn save_sessions(sessions: &[SessionInfo]) -> Result<()> {
    let path = sessions_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let data = serde_json::to_string_pretty(sessions)?;
    std::fs::write(&tmp_path, data)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

pub fn load_sessions() -> Vec<SessionInfo> {
    let path = sessions_path();
    match std::fs::read_to_string(&path) {
        Ok(data) => serde_json::from_str(&data).unwrap_or_else(|e| {
            tracing::warn!("Failed to parse sessions file: {e}");
            Vec::new()
        }),
        Err(_) => Vec::new(),
    }
}
