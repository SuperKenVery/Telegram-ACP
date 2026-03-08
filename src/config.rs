use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bot_token: String,
    pub chat_id: i64,
    pub telegraph_author: Option<String>,
    #[allow(dead_code)]
    pub telegraph_author_url: Option<String>,
    pub socket_path: PathBuf,
    pub default_agent_command: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct FileConfig {
    bot_token: Option<String>,
    chat_id: Option<i64>,
    telegraph_author: Option<String>,
    telegraph_author_url: Option<String>,
    socket_path: Option<PathBuf>,
    default_agent_command: Option<String>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_path = dirs_config_path();
        let file_config = if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path)
                .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;
            toml::from_str::<FileConfig>(&contents)
                .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?
        } else {
            FileConfig::default()
        };

        let bot_token = env_or("TELEGRAM_ACP_BOT_TOKEN", file_config.bot_token)
            .context("bot_token is required (set TELEGRAM_ACP_BOT_TOKEN or config file)")?;

        let chat_id = env_or("TELEGRAM_ACP_CHAT_ID", file_config.chat_id.map(|id| id.to_string()))
            .context("chat_id is required (set TELEGRAM_ACP_CHAT_ID or config file)")?
            .parse::<i64>()
            .context("chat_id must be a valid integer")?;

        let socket_path = env_or("TELEGRAM_ACP_SOCKET_PATH", file_config.socket_path.map(|p| p.to_string_lossy().into_owned()))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/telegram-acp.sock"));

        let default_agent_command = env_or("TELEGRAM_ACP_AGENT_COMMAND", file_config.default_agent_command)
            .unwrap_or_else(|| "claude-agent-acp".to_string());

        let telegraph_author = env_or("TELEGRAM_ACP_TELEGRAPH_AUTHOR", file_config.telegraph_author);
        let telegraph_author_url = env_or("TELEGRAM_ACP_TELEGRAPH_AUTHOR_URL", file_config.telegraph_author_url);

        Ok(Config {
            bot_token,
            chat_id,
            telegraph_author,
            telegraph_author_url,
            socket_path,
            default_agent_command,
        })
    }
}

fn dirs_config_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".config")
        .join("telegram-acp")
        .join("config.toml")
}

fn env_or(key: &str, fallback: Option<String>) -> Option<String> {
    std::env::var(key).ok().or(fallback)
}
