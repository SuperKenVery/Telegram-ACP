use anyhow::{anyhow, Result};
use futures::channel::mpsc;
use futures::StreamExt;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, Content, ErrorCode, ErrorData, Implementation, ProtocolVersion,
    ServerCapabilities, ServerInfo,
};
use rmcp::service::{RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::{tool, tool_handler, tool_router, RoleServer, ServerHandler, ServiceExt};
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InputFile, MessageId, ThreadId};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::daemon::DaemonHandle;
use crate::types::NewSessionArgs;

#[derive(Clone)]
struct McpServer {
    bot: Bot,
    daemon: Arc<DaemonHandle>,
    chat_id: ChatId,
    thread_id: i32,
    project_path: PathBuf,
    tool_router: ToolRouter<McpServer>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UploadMarkdownArgs {
    /// Absolute or project-relative path to a Markdown file
    path: String,
    title: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UploadFileArgs {
    /// Absolute or project-relative path
    path: String,
    caption: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UploadImageArgs {
    /// Absolute or project-relative path
    path: String,
    caption: Option<String>,
}

#[tool_router]
impl McpServer {
    /// Upload a Markdown file to Telegram
    #[tool]
    async fn upload_markdown(
        &self,
        Parameters(args): Parameters<UploadMarkdownArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let (input, filename) = build_input_file(&self.project_path, &args.path)?;
        let thread_id = ThreadId(MessageId(self.thread_id));
        let mut request = self
            .bot
            .send_document(self.chat_id, input)
            .message_thread_id(thread_id);
        if let Some(title) = args.title {
            request = request.caption(title);
        }
        request
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Uploaded Markdown file: {filename}"
        ))]))
    }

    /// Upload a file to Telegram
    #[tool]
    async fn upload_file(
        &self,
        Parameters(args): Parameters<UploadFileArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let (input, filename) = build_input_file(&self.project_path, &args.path)?;

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

    /// Upload an image to Telegram
    #[tool]
    async fn upload_image(
        &self,
        Parameters(args): Parameters<UploadImageArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let (input, filename) = build_input_file(&self.project_path, &args.path)?;

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

    /// Create a new Telegram-ACP session in a new forum topic
    #[tool]
    async fn create_session(
        &self,
        Parameters(args): Parameters<NewSessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let project_path = match &args.project_path {
            Some(p) if Path::new(p).is_absolute() => p.clone(),
            Some(p) => std::env::current_dir()
                .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?
                .join(p)
                .to_string_lossy()
                .to_string(),
            None => self.project_path.to_string_lossy().to_string(),
        };

        match self
            .daemon
            .spawn_session(project_path, None, args.agent)
            .await
        {
            Ok((acp_session_id, thread_id)) => {
                let topic_url = format!("https://t.me/c/{}/{}", self.chat_id.0, thread_id);
                Ok(CallToolResult::success(vec![
                    Content::text(format!(
                        "Session created successfully!\nACP session: {acp_session_id}\nTopic: {topic_url}\nYou can now interact with this session in the Telegram topic."
                    )),
                ]))
            }
            Err(e) => Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                format!("Failed to create session: {e}"),
                None,
            )),
        }
    }
}

#[tool_handler]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION"))
                    .with_title("Telegram MCP Relay"),
            )
            .with_protocol_version(ProtocolVersion::V_2025_03_26)
    }
}

impl McpServer {
    fn new(
        bot: Bot,
        daemon: Arc<DaemonHandle>,
        chat_id: ChatId,
        thread_id: i32,
        project_path: PathBuf,
    ) -> Self {
        Self {
            bot,
            daemon,
            chat_id,
            thread_id,
            project_path,
            tool_router: Self::tool_router(),
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
        daemon: Arc<DaemonHandle>,
        chat_id: ChatId,
        thread_id: i32,
        project_path: PathBuf,
    ) -> Result<Self> {
        let id = Uuid::new_v4().to_string();
        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();

        let server = McpServer::new(bot, daemon, chat_id, thread_id, project_path);
        let session_id_for_log = id.clone();
        tokio::spawn(async move {
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

fn build_input_file(project_path: &Path, path: &str) -> Result<(InputFile, String), ErrorData> {
    let resolved = resolve_path(project_path, path);
    let display = resolved
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("upload.bin")
        .to_string();
    Ok((InputFile::file(resolved), display))
}

fn resolve_path(project_path: &Path, path: &str) -> PathBuf {
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        candidate
    } else {
        project_path.join(candidate)
    }
}
