use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bot_token: String,
    pub chat_id: i64,
    pub socket_path: PathBuf,
    pub default_agent: String,
    pub agents: HashMap<String, String>,
    pub tray: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct FileConfig {
    bot_token: Option<String>,
    chat_id: Option<i64>,
    socket_path: Option<PathBuf>,
    default_agent: Option<String>,
    tray: Option<bool>,
    #[serde(flatten)]
    extra_tables: HashMap<String, toml::Table>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_path = dirs_config_path();
        let file_config = if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path).with_context(|| {
                format!("Failed to read config file: {}", config_path.display())
            })?;
            toml::from_str::<FileConfig>(&contents).with_context(|| {
                format!("Failed to parse config file: {}", config_path.display())
            })?
        } else {
            FileConfig::default()
        };

        let bot_token = env_or("TELEGRAM_ACP_BOT_TOKEN", file_config.bot_token)
            .context("bot_token is required (set TELEGRAM_ACP_BOT_TOKEN or config file)")?;

        let chat_id = env_or(
            "TELEGRAM_ACP_CHAT_ID",
            file_config.chat_id.map(|id| id.to_string()),
        )
        .context("chat_id is required (set TELEGRAM_ACP_CHAT_ID or config file)")?
        .parse::<i64>()
        .context("chat_id must be a valid integer")?;

        let socket_path = env_or(
            "TELEGRAM_ACP_SOCKET_PATH",
            file_config
                .socket_path
                .map(|p| p.to_string_lossy().into_owned()),
        )
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/telegram-acp.sock"));

        let agents = parse_agents(&file_config.extra_tables);
        let default_agent = env_or("TELEGRAM_ACP_DEFAULT_AGENT", file_config.default_agent)
            .or_else(|| {
                if agents.len() == 1 {
                    agents.keys().next().cloned()
                } else {
                    None
                }
            })
            .context("default_agent is required (set TELEGRAM_ACP_DEFAULT_AGENT or config file)")?;
        ensure_agent_exists(&default_agent, &agents)?;

        Ok(Config {
            bot_token,
            chat_id,
            socket_path,
            default_agent,
            agents,
            tray: file_config.tray,
        })
    }

    pub fn resolve_agent_command(&self, selected_agent: Option<&str>) -> Result<String> {
        self.resolve_agent(selected_agent)
            .map(|(_, command)| command)
    }

    pub fn resolve_agent(&self, selected_agent: Option<&str>) -> Result<(String, String)> {
        let selected_agent = selected_agent
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or(&self.default_agent);

        let command =
            self.agents.get(selected_agent).cloned().ok_or_else(|| {
                anyhow::anyhow!(unknown_agent_message(selected_agent, &self.agents))
            })?;

        Ok((selected_agent.to_string(), command))
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

fn parse_agents(extra_tables: &HashMap<String, toml::Table>) -> HashMap<String, String> {
    let mut agents = HashMap::new();
    for (name, table) in extra_tables {
        if let Some(toml::Value::String(cmd)) = table.get("cmd") {
            let trimmed = cmd.trim();
            if !trimmed.is_empty() {
                agents.insert(name.clone(), trimmed.to_string());
            }
        }
    }
    agents
}

fn ensure_agent_exists(default_agent: &str, agents: &HashMap<String, String>) -> Result<()> {
    if agents.contains_key(default_agent) {
        return Ok(());
    }
    anyhow::bail!(
        "default_agent '{}' has no matching [<agent>] table with cmd. {}",
        default_agent,
        unknown_agent_message(default_agent, agents)
    );
}

fn unknown_agent_message(agent: &str, agents: &HashMap<String, String>) -> String {
    let mut available: Vec<String> = agents.keys().cloned().collect();
    available.sort();
    if available.is_empty() {
        format!(
            "Unknown agent '{}'. No agents are configured. Add tables like [codex] with cmd = \"...\".",
            agent
        )
    } else {
        format!(
            "Unknown agent '{}'. Available agents: {}",
            agent,
            available.join(", ")
        )
    }
}
