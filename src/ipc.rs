use anyhow::{Context, Result};
use serde_json;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::types::{DaemonCommand, DaemonResponse};

// === Server (daemon side) ===

/// Start listening for IPC commands on a Unix socket.
pub async fn run_ipc_server(
    socket_path: &Path,
    handler: impl Fn(DaemonCommand) -> futures::future::BoxFuture<'static, DaemonResponse>
        + Send
        + Sync
        + 'static,
) -> Result<()> {
    // Remove stale socket file
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("Failed to bind Unix socket at {}", socket_path.display()))?;

    tracing::info!("IPC server listening on {}", socket_path.display());

    let handler = std::sync::Arc::new(handler);

    loop {
        let (stream, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_ipc_connection(stream, &*handler).await {
                tracing::error!("IPC connection error: {e}");
            }
        });
    }
}

async fn handle_ipc_connection(
    stream: UnixStream,
    handler: &(dyn Fn(DaemonCommand) -> futures::future::BoxFuture<'static, DaemonResponse>
          + Send
          + Sync),
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await? > 0 {
        let cmd: DaemonCommand = serde_json::from_str(line.trim())
            .with_context(|| format!("Failed to parse IPC command: {}", line.trim()))?;

        let response = handler(cmd).await;
        let response_json = serde_json::to_string(&response)?;

        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        line.clear();
    }

    Ok(())
}

// === Client (CLI side) ===

/// Send a command to the daemon via Unix socket and get a response.
pub async fn send_command(socket_path: &Path, cmd: &DaemonCommand) -> Result<DaemonResponse> {
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Failed to connect to daemon at {}", socket_path.display()))?;

    let (reader, mut writer) = stream.into_split();

    let cmd_json = serde_json::to_string(cmd)?;
    writer.write_all(cmd_json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let response: DaemonResponse = serde_json::from_str(line.trim())
        .context("Failed to parse daemon response")?;

    Ok(response)
}
