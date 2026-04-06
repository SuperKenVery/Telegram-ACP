//! A mock ACP agent for testing telegram-acp.
//!
//! Speaks the ACP protocol (JSON-RPC 2.0) over stdin/stdout directly.
//! On each prompt it:
//! 1. Parses the prompt text as JSON: `{ "messages": [...], "delay": 0.5 }`
//! 2. Sends each message in the array as raw JSON notifications
//! 3. Returns a prompt response with the stopReason from the last `session/prompt` message,
//!    or hangs indefinitely if none is found.

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("mock-agent: read error: {e}");
                break;
            }
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let req: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("mock-agent: failed to parse JSON: {e}");
                continue;
            }
        };

        let id = req.get("id").cloned();
        let method = req["method"].as_str().unwrap_or("");

        match method {
            "initialize" => {
                write_response(
                    &mut stdout,
                    id,
                    json!({
                        "protocolVersion": "1",
                        "agentInfo": {
                            "name": "mock-agent",
                            "version": "0.1.0",
                            "title": "Mock Agent"
                        },
                        "capabilities": {}
                    }),
                )
                .await;
            }
            "authenticate" => {
                write_response(&mut stdout, id, json!({})).await;
            }
            "session/new" => {
                write_response(&mut stdout, id, json!({ "sessionId": "mock-session-1" })).await;
            }
            "session/prompt" => {
                handle_prompt(&mut stdout, &req).await;
            }
            "session/cancel" => {
                // notification, no response needed
            }
            other => {
                eprintln!("mock-agent: unknown method: {other}");
                if id.is_some() {
                    write_error(&mut stdout, id, -32601, "Method not found").await;
                }
            }
        }
    }
}

async fn handle_prompt(stdout: &mut tokio::io::Stdout, req: &Value) {
    let id = req.get("id").cloned();
    let params = &req["params"];
    let session_id = params["sessionId"].as_str().unwrap_or("");

    // Extract prompt text from the prompt content blocks
    let prompt_text: String = params["prompt"]
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|b| {
                    if b["type"] == "text" {
                        b["text"].as_str().map(str::to_owned)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();

    let config: Value = match serde_json::from_str(&prompt_text) {
        Ok(v) => v,
        Err(e) => {
            write_text_notification(
                stdout,
                session_id,
                &format!("Failed to parse input JSON: {e}"),
            )
            .await;
            write_response(stdout, id, json!({ "stopReason": "end_turn" })).await;
            return;
        }
    };

    let messages = match config["messages"].as_array() {
        Some(m) => m.clone(),
        None => {
            write_text_notification(stdout, session_id, "Missing 'messages' array").await;
            write_response(stdout, id, json!({ "stopReason": "end_turn" })).await;
            return;
        }
    };

    let delay_secs = config["delay"].as_f64().unwrap_or(0.5);
    let delay = tokio::time::Duration::from_secs_f64(delay_secs);
    let response = config.get("response").cloned();

    for (i, msg) in messages.iter().enumerate() {
        let mut msg = msg.clone();
        if let Some(p) = msg.get_mut("params") {
            p["sessionId"] = json!(session_id);
        }
        if let Err(e) = write_line(stdout, &msg).await {
            eprintln!("mock-agent: failed to send message {i}: {e}");
        }
        tokio::time::sleep(delay).await;
    }

    if let Some(r) = response {
        write_response(stdout, id, r).await;
    }
}

async fn write_text_notification(stdout: &mut tokio::io::Stdout, session_id: &str, text: &str) {
    let msg = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text }
            }
        }
    });
    write_line(stdout, &msg).await.ok();
}

async fn write_response(stdout: &mut tokio::io::Stdout, id: Option<Value>, result: Value) {
    let msg = json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    });
    write_line(stdout, &msg).await.ok();
}

async fn write_error(stdout: &mut tokio::io::Stdout, id: Option<Value>, code: i32, message: &str) {
    let msg = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    });
    write_line(stdout, &msg).await.ok();
}

async fn write_line(stdout: &mut tokio::io::Stdout, value: &Value) -> anyhow::Result<()> {
    let mut buf = serde_json::to_vec(value)?;
    buf.push(b'\n');
    stdout.write_all(&buf).await?;
    stdout.flush().await?;
    Ok(())
}
