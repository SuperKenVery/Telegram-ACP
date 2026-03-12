//! A mock ACP agent for testing telegram-acp.
//!
//! Speaks the ACP protocol over stdin/stdout. On each prompt it:
//! 1. Sends a greeting text message
//! 2. Simulates a "Read" tool call
//! 3. Simulates a "Write" tool call with some code output
//! 4. Sends a summary text message
//! 5. Returns with StopReason::EndTurn

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

        // Extract prompt text
        let prompt_text: String = args
            .prompt
            .iter()
            .filter_map(|block| match block {
                acp::ContentBlock::Text(tc) => Some(tc.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        // 1. Send a greeting
        send_text(
            conn,
            &sid,
            &format!("Hello! I received your prompt: \"{prompt_text}\"\n\nLet me work on that."),
        )
        .await;

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // 2. Simulate a "Read" tool call
        let read_id = acp::ToolCallId::new("tool-1");
        conn.session_notification(acp::SessionNotification::new(
            sid.clone(),
            acp::SessionUpdate::ToolCall(
                acp::ToolCall::new(read_id.clone(), "Read src/main.rs")
                    .status(acp::ToolCallStatus::InProgress),
            ),
        ))
        .await
        .ok();

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Read tool call complete
        conn.session_notification(acp::SessionNotification::new(
            sid.clone(),
            acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
                read_id,
                acp::ToolCallUpdateFields::new()
                    .status(acp::ToolCallStatus::Completed)
                    .title("Read src/main.rs".to_string())
                    .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                        "fn main() {\n    println!(\"Hello, world!\");\n}",
                    ))]),
            )),
        ))
        .await
        .ok();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // 3. Simulate a "Write" tool call
        let write_id = acp::ToolCallId::new("tool-2");
        conn.session_notification(acp::SessionNotification::new(
            sid.clone(),
            acp::SessionUpdate::ToolCall(
                acp::ToolCall::new(write_id.clone(), "Write src/main.rs")
                    .status(acp::ToolCallStatus::InProgress),
            ),
        ))
        .await
        .ok();

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        conn.session_notification(acp::SessionNotification::new(
            sid.clone(),
            acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
                write_id,
                acp::ToolCallUpdateFields::new()
                    .status(acp::ToolCallStatus::Completed)
                    .title("Write src/main.rs".to_string())
                    .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                        concat!(
                            "fn main() {\n",
                            "    println!(\"Hello from mock agent!\");\n",
                            "    // Added by mock agent\n",
                            "    let x = 42;\n",
                            "    println!(\"The answer is {x}\");\n",
                            "}",
                        ),
                    ))]),
            )),
        ))
        .await
        .ok();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // 4. Send summary
        send_text(
            conn,
            &sid,
            "Done! I've updated `src/main.rs` to print a greeting and the answer to life.",
        )
        .await;

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
            let mut stdin = child.stdin.take().ok_or_else(|| anyhow::anyhow!("missing stdin"))?;
            let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("missing stdout"))?;
            let stderr = child.stderr.take().ok_or_else(|| anyhow::anyhow!("missing stderr"))?;

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
                match tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line)).await
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
            eprintln!("mock-agent: MCP server type not supported for probe: {:?}", other);
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
