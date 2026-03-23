mod acp;
mod commands;
mod config;
mod daemon;
mod formatting;
mod handlers;
mod ipc;
mod mcp;
mod mcp_relay;
mod persistence;
mod session;
mod session_control;
mod telegram;
#[allow(dead_code)]
mod telegraph;
mod types;

use clap::{Parser, Subcommand};
use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use std::env;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser)]
#[command(name = "telegram-acp", about = "Bridge Telegram and ACP coding agents")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the daemon (bot + IPC listener)
    Daemon,
    /// Spawn a new agent session
    New {
        /// Project path for the agent to work in
        path: PathBuf,
        /// Initial prompt to send to the agent
        #[arg(short, long)]
        prompt: Option<String>,
        /// Agent name from config (e.g. codex, claude)
        #[arg(short, long)]
        agent: Option<String>,
    },
    /// List active sessions
    Status,
    /// Run MCP relay over stdio and forward to the daemon
    McpRelay {
        /// MCP session id to route messages to
        #[arg(long)]
        session: String,
        /// Unix socket path for the daemon IPC
        #[arg(short, long)]
        socket: Option<PathBuf>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let log_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("telegram-acp");
    std::fs::create_dir_all(&log_dir)?;

    let file_appender = BasicRollingFileAppender::new(
        log_dir.join("telegram-acp.log"),
        RollingConditionBasic::new().daily().max_size(1_000_000),
        3,
    )
    .expect("failed to create log file appender");

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(Mutex::new(file_appender)),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon => {
            let config = config::Config::load()?;
            // Run inside a LocalSet since ACP requires spawn_local
            let local = tokio::task::LocalSet::new();
            local.run_until(daemon::run_daemon(config)).await?;
        }
        Commands::New {
            mut path,
            prompt,
            agent,
        } => {
            if path.is_relative() {
                path = env::current_dir()?.join(path);
            }
            let config = config::Config::load()?;
            let cmd = types::DaemonCommand::NewSession {
                path,
                prompt,
                agent,
            };
            let response = ipc::send_command(&config.socket_path, &cmd).await?;
            match response {
                types::DaemonResponse::SessionCreated {
                    acp_session_id,
                    topic_url,
                } => {
                    println!("Session created: {acp_session_id}");
                    println!("Topic: {topic_url}");
                }
                types::DaemonResponse::Error { message } => {
                    eprintln!("Error: {message}");
                    std::process::exit(1);
                }
                _ => {
                    eprintln!("Unexpected response");
                    std::process::exit(1);
                }
            }
        }
        Commands::Status => {
            let config = config::Config::load()?;
            let cmd = types::DaemonCommand::ListSessions;
            let response = ipc::send_command(&config.socket_path, &cmd).await?;
            match response {
                types::DaemonResponse::SessionList { sessions } => {
                    if sessions.is_empty() {
                        println!("No active sessions.");
                    } else {
                        for s in sessions {
                            println!(
                                "{} | {} | {:?} | thread:{}",
                                s.acp_session_id,
                                s.project_path.display(),
                                s.status,
                                s.thread_id
                            );
                        }
                    }
                }
                types::DaemonResponse::Error { message } => {
                    eprintln!("Error: {message}");
                    std::process::exit(1);
                }
                _ => {
                    eprintln!("Unexpected response");
                    std::process::exit(1);
                }
            }
        }
        Commands::McpRelay { session, socket } => {
            let socket_path = match socket {
                Some(socket_path) => socket_path,
                None => config::Config::load()?.socket_path,
            };
            mcp_relay::run(session, socket_path).await?;
        }
    }

    Ok(())
}
