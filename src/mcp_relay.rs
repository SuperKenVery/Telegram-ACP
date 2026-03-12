use anyhow::{anyhow, Result};
use std::path::PathBuf;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::ipc;
use crate::types::{DaemonCommand, DaemonResponse};

pub async fn run(session_id: String, socket_path: PathBuf) -> Result<()> {
    let mut reader = BufReader::new(stdin());
    let mut writer = stdout();
    let mut line = String::new();

    loop {
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            break;
        }
        let payload = line.trim_end_matches(&['\r', '\n'][..]);
        if payload.trim().is_empty() {
            line.clear();
            continue;
        }
        let cmd = DaemonCommand::McpMessage {
            session_id: session_id.clone(),
            payload: payload.to_string(),
        };
        let response = ipc::send_command(&socket_path, &cmd).await?;

        match response {
            DaemonResponse::McpResponse { payload: Some(payload) } => {
                writer.write_all(payload.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
            }
            DaemonResponse::McpResponse { payload: None } => {}
            DaemonResponse::Error { message } => return Err(anyhow!(message)),
            _ => return Err(anyhow!("Unexpected daemon response to MCP relay")),
        }
        line.clear();
    }

    Ok(())
}
