mod acp;
mod commands;
mod config;
mod daemon;
mod formatting;
mod ipc;
mod persistence;
mod session;
mod session_control;
mod telegram;
#[allow(dead_code)]
mod telegraph;
mod types;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
        /// Agent command to use (overrides config default)
        #[arg(short, long)]
        agent: Option<String>,
    },
    /// List active sessions
    Status,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
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
            path,
            prompt,
            agent,
        } => {
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
    }

    Ok(())
}
