//! A mock ACP agent for testing telegram-acp.
//!
//! Speaks the ACP protocol over stdin/stdout. On each prompt it:
//! 1. Parses the input as JSON with format: `{ "messages": [...], "delay": 0.5 }`
//! 2. Sends each message in the array sequentially
//! 3. Adds delay between pairs of messages (every 2 messages)
//! 4. Returns with StopReason::EndTurn

use acp::Client; // needed to call session_notification on AgentSideConnection
use agent_client_protocol as acp;
use std::cell::OnceCell;
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

struct MockAgent {
    conn: Rc<OnceCell<acp::AgentSideConnection>>,
}

#[async_trait::async_trait(?Send)]
impl acp::Agent for MockAgent {
    async fn initialize(
        &self,
        _args: acp::InitializeRequest,
    ) -> acp::Result<acp::InitializeResponse> {
        Ok(acp::InitializeResponse::new(acp::ProtocolVersion::V1)
            .agent_info(acp::Implementation::new("mock-agent", "0.1.0").title("Mock Agent"))
            .agent_capabilities(acp::AgentCapabilities::new()))
    }

    async fn authenticate(
        &self,
        _args: acp::AuthenticateRequest,
    ) -> acp::Result<acp::AuthenticateResponse> {
        Ok(acp::AuthenticateResponse::new())
    }

    async fn new_session(
        &self,
        args: acp::NewSessionRequest,
    ) -> acp::Result<acp::NewSessionResponse> {
        // Probe any advertised MCP servers to verify tool discovery (e.g. upload_markdown).
        let mcp_servers = args.mcp_servers.clone();
        if mcp_servers.is_empty() {
            eprintln!("mock-agent: no MCP servers advertised in new_session");
        } else {
            for server in mcp_servers {
                tokio::task::spawn_local(async move {
                    if let Err(err) = mcp_probe(server).await {
                        eprintln!("mock-agent: MCP probe error: {err}");
                    }
                });
            }
        }

        Ok(acp::NewSessionResponse::new(acp::SessionId::new(
            "mock-session-1",
        )))
    }

    async fn prompt(&self, args: acp::PromptRequest) -> acp::Result<acp::PromptResponse> {
        let conn = self.conn.get().expect("connection not set");
        let sid = args.session_id.clone();

        // Extract prompt text and parse as JSON
        let prompt_text: String = args
            .prompt
            .iter()
            .filter_map(|block| match block {
                acp::ContentBlock::Text(tc) => Some(tc.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        let input: serde_json::Value = match serde_json::from_str(&prompt_text) {
            Ok(v) => v,
            Err(e) => {
                send_text(conn, &sid, &format!("Failed to parse input JSON: {e}")).await;
                return Ok(acp::PromptResponse::new(acp::StopReason::EndTurn));
            }
        };

        let messages = match input.get("messages").and_then(|m| m.as_array()) {
            Some(m) => m.clone(),
            None => {
                send_text(conn, &sid, "No 'messages' array in input JSON").await;
                return Ok(acp::PromptResponse::new(acp::StopReason::EndTurn));
            }
        };

        let delay_secs = input.get("delay").and_then(|d| d.as_f64()).unwrap_or(0.5);
        let delay = Duration::from_secs_f64(delay_secs);

        // Send each message back-to-back with delay between pairs
        for (i, msg) in messages.iter().enumerate() {
            if let Err(e) = send_json_notification(conn, &sid, msg).await {
                eprintln!("mock-agent: failed to send message {i}: {e}");
            }

            // Add delay between pairs (every 2 messages)
            if i % 2 == 1 && i < messages.len() - 1 {
                tokio::time::sleep(delay).await;
            }
        }

        Ok(acp::PromptResponse::new(acp::StopReason::EndTurn))
    }

    async fn cancel(&self, _args: acp::CancelNotification) -> acp::Result<()> {
        Ok(())
    }
}

async fn send_text(conn: &acp::AgentSideConnection, sid: &acp::SessionId, text: &str) {
    conn.session_notification(acp::SessionNotification::new(
        sid.clone(),
        acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(acp::ContentBlock::from(
            text.to_string(),
        ))),
    ))
    .await
    .ok();
}

/// Sends a raw JSON message as a session notification.
/// The JSON should be a valid ACP session update message.
async fn send_json_notification(
    conn: &acp::AgentSideConnection,
    sid: &acp::SessionId,
    msg: &serde_json::Value,
) -> anyhow::Result<()> {
    // Clone and overwrite sessionId to our real session id
    let mut msg = msg.clone();
    let sid_str = sid.to_string();
    if let Some(params) = msg.get_mut("params") {
        // Overwrite sessionId in params
        params["sessionId"] = serde_json::json!(sid_str);
    }

    // Parse the JSON into an ACP SessionUpdate enum
    let update: acp::SessionUpdate = serde_json::from_value(msg["params"]["update"].clone())?;
    conn.session_notification(acp::SessionNotification::new(sid.clone(), update))
        .await?;
    Ok(())
}

async fn mcp_probe(server: acp::McpServer) -> anyhow::Result<()> {
    match server {
        acp::McpServer::Stdio(stdio) => {
            eprintln!("mock-agent: probing MCP stdio server: {:?}", stdio);

            let mut cmd = Command::new(stdio.command);
            cmd.args(stdio.args);
            cmd.stdin(std::process::Stdio::piped());
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::piped());

            let mut child = cmd.spawn()?;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| anyhow::anyhow!("missing stdin"))?;
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| anyhow::anyhow!("missing stdout"))?;
            let stderr = child
                .stderr
                .take()
                .ok_or_else(|| anyhow::anyhow!("missing stderr"))?;

            let stderr_handle: JoinHandle<()> = tokio::task::spawn_local(async move {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                loop {
                    line.clear();
                    let bytes = reader.read_line(&mut line).await.unwrap_or(0);
                    if bytes == 0 {
                        break;
                    }
                    eprintln!("mcp-relay stderr: {}", line.trim_end());
                }
            });

            // Send initialize request, then wait for its response before sending initialized.
            let init = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "clientInfo": { "name": "mock-agent", "version": "0.1.0" },
                    "capabilities": {}
                }
            });
            write_json_line(&mut stdin, &init).await?;

            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let mut got_init = false;
            let mut got_tools = false;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(6);

            while tokio::time::Instant::now() < deadline && !(got_init && got_tools) {
                line.clear();
                match tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
                    .await
                {
                    Ok(Ok(0)) => break,
                    Ok(Ok(_)) => {
                        let trimmed = line.trim_end();
                        if trimmed.is_empty() {
                            continue;
                        }
                        eprintln!("mcp-relay stdout: {trimmed}");

                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            let id = value.get("id").and_then(|v| v.as_i64());
                            if id == Some(1) && !got_init {
                                got_init = true;
                                let initialized = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "method": "notifications/initialized",
                                    "params": {}
                                });
                                write_json_line(&mut stdin, &initialized).await?;

                                let list_tools = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "id": 2,
                                    "method": "tools/list",
                                    "params": {}
                                });
                                write_json_line(&mut stdin, &list_tools).await?;
                            } else if id == Some(2) {
                                got_tools = true;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        eprintln!("mock-agent: MCP read error: {e}");
                        break;
                    }
                    Err(_) => {
                        eprintln!("mock-agent: MCP read timeout");
                        break;
                    }
                }
            }

            let _ = child.kill();
            let _ = stderr_handle.await;
        }
        other => {
            eprintln!(
                "mock-agent: MCP server type not supported for probe: {:?}",
                other
            );
        }
    }

    Ok(())
}

async fn write_json_line(
    stdin: &mut tokio::process::ChildStdin,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    let mut buf = serde_json::to_vec(value)?;
    buf.push(b'\n');
    stdin.write_all(&buf).await?;
    stdin.flush().await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let conn_cell = Rc::new(OnceCell::new());
            let agent = MockAgent {
                conn: conn_cell.clone(),
            };

            let stdin = tokio::io::stdin().compat();
            let stdout = tokio::io::stdout().compat_write();

            let (connection, io_task) =
                acp::AgentSideConnection::new(agent, stdout, stdin, |fut| {
                    tokio::task::spawn_local(fut);
                });

            conn_cell.set(connection).ok();

            if let Err(e) = io_task.await {
                eprintln!("Mock agent IO error: {e}");
            }
        })
        .await;
}
