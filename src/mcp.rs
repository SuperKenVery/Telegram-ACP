use anyhow::{anyhow, Result};
use futures::channel::mpsc;
use futures::StreamExt;
use rmcp::service::{RxJsonRpcMessage, RunningService, TxJsonRpcMessage};
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
struct McpSkeleton;

impl ServerHandler for McpSkeleton {}

pub struct McpSession {
    pub id: String,
    incoming_tx: Mutex<mpsc::UnboundedSender<RxJsonRpcMessage<RoleServer>>>,
    outgoing_rx: Mutex<mpsc::UnboundedReceiver<TxJsonRpcMessage<RoleServer>>>,
    // Keeps the MCP service alive for the lifetime of the session.
    _service: Mutex<RunningService<RoleServer, McpSkeleton>>,
}

impl McpSession {
    pub async fn new() -> Result<Self> {
        let id = Uuid::new_v4().to_string();
        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();

        let service = McpSkeleton.serve((outgoing_tx, incoming_rx)).await?;

        Ok(Self {
            id,
            incoming_tx: Mutex::new(incoming_tx),
            outgoing_rx: Mutex::new(outgoing_rx),
            _service: Mutex::new(service),
        })
    }

    pub async fn send(&self, message: RxJsonRpcMessage<RoleServer>) -> Result<()> {
        let mut tx = self.incoming_tx.lock().await;
        tx.unbounded_send(message)
            .map_err(|_| anyhow!("MCP incoming channel closed"))?;
        Ok(())
    }

    pub async fn next_response(&self) -> Option<TxJsonRpcMessage<RoleServer>> {
        let mut rx = self.outgoing_rx.lock().await;
        rx.next().await
    }
}
