use anyhow::{anyhow, Result};
use std::path::PathBuf;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::ipc;
use crate::types::{DaemonCommand, DaemonResponse};

pub async fn run(session_id: String, socket_path: PathBuf) -> Result<()> {
    tracing::debug!(session_id = %session_id, socket = %socket_path.display(), "MCP relay started");

    let mut reader = BufReader::new(stdin());
    let mut writer = stdout();
    let mut line = String::new();
    let mut msg_count: u64 = 0;

    loop {
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            tracing::debug!(session_id = %session_id, messages = msg_count, "MCP relay stdin closed, exiting");
            break;
        }
        let payload = line.trim_end_matches(&['\r', '\n'][..]);
        if payload.trim().is_empty() {
            line.clear();
            continue;
        }
        msg_count += 1;
        tracing::debug!(session_id = %session_id, n = msg_count, payload = %payload, "MCP relay -> daemon");

        let cmd = DaemonCommand::McpMessage {
            session_id: session_id.clone(),
            payload: payload.to_string(),
        };
        let response = match ipc::send_command(&socket_path, &cmd).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(session_id = %session_id, n = msg_count, error = %e, "MCP relay IPC send failed");
                return Err(e);
            }
        };

        match response {
            DaemonResponse::McpResponse {
                payload: Some(payload),
            } => {
                tracing::debug!(session_id = %session_id, n = msg_count, payload = %payload, "MCP relay <- daemon");
                writer.write_all(payload.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
            }
            DaemonResponse::McpResponse { payload: None } => {
                tracing::debug!(session_id = %session_id, n = msg_count, "MCP relay <- daemon (no response)");
            }
            DaemonResponse::Error { message } => {
                tracing::error!(session_id = %session_id, n = msg_count, message = %message, "MCP relay daemon returned error");
                return Err(anyhow!(message));
            }
            _ => return Err(anyhow!("Unexpected daemon response to MCP relay")),
        }
        line.clear();
    }

    Ok(())
}
