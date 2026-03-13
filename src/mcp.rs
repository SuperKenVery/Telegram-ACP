use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD as Base64;
use base64::Engine;
use futures::channel::mpsc;
use futures::StreamExt;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, ErrorCode, ErrorData, Implementation,
    ListToolsResult, PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InputFile, MessageId, ThreadId};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::telegraph;

#[derive(Clone)]
struct McpServer {
    bot: Bot,
    telegraph: Arc<telegraph_rs::Telegraph>,
    chat_id: ChatId,
    thread_id: i32,
    project_path: PathBuf,
}

impl McpServer {
    fn new(
        bot: Bot,
        telegraph: Arc<telegraph_rs::Telegraph>,
        chat_id: ChatId,
        thread_id: i32,
        project_path: PathBuf,
    ) -> Self {
        Self {
            bot,
            telegraph,
            chat_id,
            thread_id,
            project_path,
        }
    }
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION"))
                    .with_title("Telegram MCP Relay"),
            )
            .with_protocol_version(ProtocolVersion::V_2025_03_26)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let tools = vec![
            Tool::new(
                "upload_markdown",
                "Render Markdown to Telegraph and send link to the user",
                serde_json::from_value::<rmcp::model::JsonObject>(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "markdown": { "type": "string" },
                        "title": { "type": "string" }
                    },
                    "required": ["markdown"]
                }))
                .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?,
            ),
            Tool::new(
                "upload_file",
                "Upload a file to the Telegram topic",
                serde_json::from_value::<rmcp::model::JsonObject>(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "Absolute or project-relative path" },
                        "content_base64": { "type": "string", "description": "Base64-encoded file content" },
                        "filename": { "type": "string", "description": "Filename to use for base64 content" },
                        "caption": { "type": "string" }
                    }
                }))
                .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?,
            ),
            Tool::new(
                "upload_image",
                "Upload an image to the Telegram topic",
                serde_json::from_value::<rmcp::model::JsonObject>(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "Absolute or project-relative path" },
                        "content_base64": { "type": "string", "description": "Base64-encoded image data" },
                        "filename": { "type": "string", "description": "Filename to use for base64 content" },
                        "caption": { "type": "string" }
                    }
                }))
                .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?,
            ),
        ];

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        match request.name.as_ref() {
            "upload_markdown" => self.handle_upload_markdown(request).await,
            "upload_file" => self.handle_upload_file(request).await,
            "upload_image" => self.handle_upload_image(request).await,
            _ => Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                "Unknown tool".to_string(),
                None,
            )),
        }
    }
}

pub struct McpSession {
    pub id: String,
    incoming_tx: Mutex<mpsc::UnboundedSender<RxJsonRpcMessage<RoleServer>>>,
    outgoing_rx: Mutex<mpsc::UnboundedReceiver<TxJsonRpcMessage<RoleServer>>>,
}

impl McpSession {
    pub async fn new(
        bot: Bot,
        telegraph: Arc<telegraph_rs::Telegraph>,
        chat_id: ChatId,
        thread_id: i32,
        project_path: PathBuf,
    ) -> Result<Self> {
        let id = Uuid::new_v4().to_string();
        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();

        let server = McpServer::new(bot, telegraph, chat_id, thread_id, project_path);
        let session_id_for_log = id.clone();
        // Spawn the MCP serve handshake in the background. It will block until
        // the agent subprocess connects and sends the MCP initialize request.
        // We must not await it here because the agent hasn't been spawned yet.
        tokio::task::spawn_local(async move {
            tracing::debug!(session_id = %session_id_for_log, "MCP server task started, waiting for initialize");
            match server.serve((outgoing_tx, incoming_rx)).await {
                Ok(service) => {
                    tracing::debug!(session_id = %session_id_for_log, "MCP server handshake complete, waiting for session end");
                    let _ = service.waiting().await;
                    tracing::debug!(session_id = %session_id_for_log, "MCP server session ended");
                }
                Err(e) => {
                    tracing::warn!(session_id = %session_id_for_log, "MCP server init error: {e}")
                }
            }
        });

        Ok(Self {
            id,
            incoming_tx: Mutex::new(incoming_tx),
            outgoing_rx: Mutex::new(outgoing_rx),
        })
    }

    pub async fn send(&self, message: RxJsonRpcMessage<RoleServer>) -> Result<()> {
        tracing::debug!(session_id = %self.id, "MCP session: sending message to server");
        let tx = self.incoming_tx.lock().await;
        tx.unbounded_send(message).map_err(|e| {
            tracing::error!(session_id = %self.id, "MCP incoming channel closed: {e}");
            anyhow!("MCP incoming channel closed")
        })?;
        tracing::debug!(session_id = %self.id, "MCP session: message enqueued");
        Ok(())
    }

    pub async fn next_response(&self) -> Option<TxJsonRpcMessage<RoleServer>> {
        tracing::debug!(session_id = %self.id, "MCP session: waiting for response");
        let mut rx = self.outgoing_rx.lock().await;
        let result = rx.next().await;
        match &result {
            Some(_) => tracing::debug!(session_id = %self.id, "MCP session: got response"),
            None => {
                tracing::warn!(session_id = %self.id, "MCP session: outgoing channel closed, no response")
            }
        }
        result
    }
}

#[derive(Deserialize)]
struct UploadMarkdownArgs {
    markdown: String,
    title: Option<String>,
}

#[derive(Deserialize)]
struct UploadFileArgs {
    path: Option<String>,
    content_base64: Option<String>,
    filename: Option<String>,
    caption: Option<String>,
}

#[derive(Deserialize)]
struct UploadImageArgs {
    path: Option<String>,
    content_base64: Option<String>,
    filename: Option<String>,
    caption: Option<String>,
}

impl McpServer {
    async fn handle_upload_markdown(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResult, ErrorData> {
        let args = parse_args::<UploadMarkdownArgs>(request.arguments)?;
        let title = args.title.unwrap_or_else(|| "Markdown Upload".to_string());
        let url = telegraph::create_markdown_post(&self.telegraph, &title, &args.markdown)
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;

        let thread_id = ThreadId(MessageId(self.thread_id));
        if let Err(e) = self
            .bot
            .send_message(self.chat_id, format!("Telegraph: {url}"))
            .message_thread_id(thread_id)
            .await
        {
            return Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                format!("Failed to send Telegram message: {e}"),
                None,
            ));
        }

        Ok(CallToolResult::success(vec![Content::text(url)]))
    }

    async fn handle_upload_file(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResult, ErrorData> {
        let args = parse_args::<UploadFileArgs>(request.arguments)?;
        let (input, filename) = build_input_file(
            &self.project_path,
            args.path,
            args.content_base64,
            args.filename,
        )?;

        let thread_id = ThreadId(MessageId(self.thread_id));
        let mut request = self
            .bot
            .send_document(self.chat_id, input)
            .message_thread_id(thread_id);
        if let Some(caption) = args.caption {
            request = request.caption(caption);
        }
        request
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Uploaded file: {filename}"
        ))]))
    }

    async fn handle_upload_image(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResult, ErrorData> {
        let args = parse_args::<UploadImageArgs>(request.arguments)?;
        let (input, filename) = build_input_file(
            &self.project_path,
            args.path,
            args.content_base64,
            args.filename,
        )?;

        let thread_id = ThreadId(MessageId(self.thread_id));
        let mut request = self
            .bot
            .send_photo(self.chat_id, input)
            .message_thread_id(thread_id);
        if let Some(caption) = args.caption {
            request = request.caption(caption);
        }
        request
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Uploaded image: {filename}"
        ))]))
    }
}

fn parse_args<T: for<'de> Deserialize<'de>>(
    args: Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<T, ErrorData> {
    let value = match args {
        Some(map) => serde_json::Value::Object(map),
        None => serde_json::Value::Object(serde_json::Map::new()),
    };
    serde_json::from_value(value)
        .map_err(|e| ErrorData::new(ErrorCode::INVALID_PARAMS, e.to_string(), None))
}

fn build_input_file(
    project_path: &Path,
    path: Option<String>,
    content_base64: Option<String>,
    filename: Option<String>,
) -> Result<(InputFile, String), ErrorData> {
    if let Some(path) = path {
        let resolved = resolve_path(project_path, &path);
        let display = resolved
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("upload.bin")
            .to_string();
        return Ok((InputFile::file(resolved), display));
    }

    if let Some(content) = content_base64 {
        let bytes = Base64
            .decode(content)
            .map_err(|e| ErrorData::new(ErrorCode::INVALID_PARAMS, e.to_string(), None))?;
        let name = filename.unwrap_or_else(|| "upload.bin".to_string());
        let input = InputFile::memory(bytes).file_name(name.clone());
        return Ok((input, name));
    }

    Err(ErrorData::new(
        ErrorCode::INVALID_PARAMS,
        "Expected 'path' or 'content_base64'".to_string(),
        None,
    ))
}

fn resolve_path(project_path: &Path, path: &str) -> PathBuf {
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        candidate
    } else {
        project_path.join(candidate)
    }
}
