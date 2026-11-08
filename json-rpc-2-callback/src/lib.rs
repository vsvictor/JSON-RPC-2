//! Server-to-client callbacks over JSON-RPC 2.0 notifications.
//!
//! Backends never write to client connections. They ask the gateway Control Plane
//! ([`GatewayControl`]) to deliver a JSON-RPC 2.0 notification to a registered
//! client, so delivery stays centralized no matter which backend instance handled
//! the request. Clients run a [`CallbackListener`] over a [`PushSource`] and receive
//! decoded [`CallbackMessage`]s.

use std::{fmt, future::Future, io, pin::Pin, sync::Arc, time::Duration};

use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const VERSION: &str = "2.0";
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RESPONSE_BYTES: u64 = 64 * 1024;

#[derive(Debug)]
pub enum CallbackError {
    /// The message is not a valid JSON-RPC 2.0 notification.
    InvalidMessage(String),
    /// The Control Plane could not be reached or answered incorrectly.
    Control(String),
    /// The Control Plane refused the command (for example, unknown client).
    Rejected(String),
}

impl fmt::Display for CallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMessage(reason) => write!(f, "invalid callback message: {reason}"),
            Self::Control(reason) => write!(f, "control plane error: {reason}"),
            Self::Rejected(reason) => write!(f, "control plane rejected command: {reason}"),
        }
    }
}

impl std::error::Error for CallbackError {}

/// A decoded server-to-client JSON-RPC 2.0 notification.
#[derive(Debug, Clone, PartialEq)]
pub struct CallbackMessage {
    pub method: String,
    pub params: Option<Value>,
}

/// Serializes a JSON-RPC 2.0 notification (a request without `id`).
pub fn encode_notification(method: &str, params: Option<Value>) -> Result<String, CallbackError> {
    if method.trim().is_empty() || method.starts_with("rpc.") {
        return Err(CallbackError::InvalidMessage("invalid method name".into()));
    }
    let mut message = json!({"jsonrpc": VERSION, "method": method});
    if let Some(params) = params {
        if !(params.is_object() || params.is_array()) {
            return Err(CallbackError::InvalidMessage(
                "params must be an object or an array".into(),
            ));
        }
        message["params"] = params;
    }
    Ok(message.to_string())
}

/// Parses a JSON-RPC 2.0 notification; requests with an `id` and responses are rejected.
pub fn decode_notification(bytes: &[u8]) -> Result<CallbackMessage, CallbackError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Wire {
        jsonrpc: String,
        method: String,
        #[serde(default)]
        params: Option<Value>,
    }
    let wire: Wire = serde_json::from_slice(bytes)
        .map_err(|error| CallbackError::InvalidMessage(error.to_string()))?;
    if wire.jsonrpc != VERSION {
        return Err(CallbackError::InvalidMessage("jsonrpc must be 2.0".into()));
    }
    if wire.method.is_empty() {
        return Err(CallbackError::InvalidMessage("empty method".into()));
    }
    if let Some(params) = &wire.params
        && !(params.is_object() || params.is_array())
    {
        return Err(CallbackError::InvalidMessage(
            "params must be an object or an array".into(),
        ));
    }
    Ok(CallbackMessage {
        method: wire.method,
        params: wire.params,
    })
}

/// Backend-side client of the gateway Control Plane (newline-delimited JSON over TCP).
///
/// The Control Plane is unauthenticated: keep its address on an internal network.
#[derive(Debug, Clone)]
pub struct GatewayControl {
    address: String,
}

impl GatewayControl {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }

    /// Reads `GATEWAY_CONTROL_ADDR` (for example `gateway:8500`).
    pub fn from_env() -> Result<Self, CallbackError> {
        match std::env::var("GATEWAY_CONTROL_ADDR") {
            Ok(address) if !address.trim().is_empty() => Ok(Self::new(address)),
            _ => Err(CallbackError::Control(
                "GATEWAY_CONTROL_ADDR is required".into(),
            )),
        }
    }

    /// Binds a gateway connection (taken from `TransportContext::connection_id`) to a
    /// stable client id such as the authenticated user id.
    pub async fn register_client(
        &self,
        connection_id: &str,
        client_id: &str,
    ) -> Result<(), CallbackError> {
        self.command(json!({
            "action": "register_client",
            "connection_id": connection_id,
            "client_id": client_id,
        }))
        .await
    }

    /// Delivers a notification to every connection registered for `client_id`.
    pub async fn notify_client(
        &self,
        client_id: &str,
        method: &str,
        params: Option<Value>,
    ) -> Result<(), CallbackError> {
        let payload = encode_notification(method, params)?;
        self.command(json!({
            "action": "push_unicast",
            "client_id": client_id,
            "payload": payload,
        }))
        .await
    }

    async fn command(&self, command: Value) -> Result<(), CallbackError> {
        let control = |error: &dyn fmt::Display| CallbackError::Control(error.to_string());
        let exchange = async {
            let stream = TcpStream::connect(&self.address).await?;
            let (read, mut write) = stream.into_split();
            write.write_all(command.to_string().as_bytes()).await?;
            write.write_all(b"\n").await?;
            let mut line = String::new();
            BufReader::new(tokio::io::AsyncReadExt::take(read, MAX_RESPONSE_BYTES))
                .read_line(&mut line)
                .await?;
            Ok::<_, io::Error>(line)
        };
        let line = timeout(CONTROL_TIMEOUT, exchange)
            .await
            .map_err(|_| CallbackError::Control("timed out".into()))?
            .map_err(|error| control(&error))?;
        let response: Value = serde_json::from_str(&line).map_err(|error| control(&error))?;
        if response["success"].as_bool() == Some(true) {
            Ok(())
        } else {
            Err(CallbackError::Rejected(
                response["error"].as_str().unwrap_or("unknown").to_owned(),
            ))
        }
    }
}

/// Future type used by [`PushSource`].
pub type PushFuture<'a> = Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send + 'a>>;

/// A client connection able to receive server-initiated messages.
pub trait PushSource: Send + Sync + 'static {
    /// Waits for the next pushed message; an error means the connection is gone.
    fn recv_push(&self) -> PushFuture<'_>;
}

#[cfg(feature = "quic")]
impl PushSource for json_rpc_2_quic::client::transports::quic::QuicClientTransport {
    fn recv_push(&self) -> PushFuture<'_> {
        Box::pin(async move {
            Self::recv_push(self)
                .await
                .map_err(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error.to_string()))
        })
    }
}

/// Receives decoded callbacks from a [`CallbackListener`].
pub trait CallbackHandler: Send + Sync + 'static {
    fn on_message(&self, message: CallbackMessage);

    /// Called when the push source fails; the listener retries after a short delay.
    fn on_closed(&self) {}
}

/// Background task that forwards pushed JSON-RPC notifications to a handler.
pub struct CallbackListener {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl CallbackListener {
    pub fn spawn<S: PushSource>(source: Arc<S>, handler: Arc<dyn CallbackHandler>) -> Self {
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let task = tokio::spawn(async move {
            loop {
                let received = tokio::select! {
                    _ = stop.cancelled() => return,
                    received = source.recv_push() => received,
                };
                match received {
                    Ok(bytes) => match decode_notification(&bytes) {
                        Ok(message) => handler.on_message(message),
                        Err(error) => tracing::warn!(%error, "dropping invalid callback"),
                    },
                    Err(error) => {
                        tracing::debug!(%error, "callback source interrupted");
                        handler.on_closed();
                        tokio::select! {
                            _ = stop.cancelled() => return,
                            _ = tokio::time::sleep(RETRY_DELAY) => {}
                        }
                    }
                }
            }
        });
        Self { cancel, task }
    }

    pub fn is_running(&self) -> bool {
        !self.task.is_finished()
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for CallbackListener {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests;
