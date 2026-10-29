//! HTTP-independent WebSocket session contract and JSON-RPC session adapter.

use std::sync::Arc;

use bytes::Bytes;
pub use json_rpc_2_core::{WebSocketFuture, WebSocketMessage};

use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, RunningTransport, TransportContext, TransportFuture,
    TransportSession,
};

/// Protocol carried over an established WebSocket session.
pub trait WebSocketProtocol: Send + Sync + 'static {
    fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a>;

    fn handle_message_with_context<'a>(
        &'a self,
        message: WebSocketMessage,
        _context: &'a TransportContext,
    ) -> WebSocketFuture<'a> {
        self.handle_message(message)
    }

    fn session_opened(&self, _context: &TransportContext) {}

    fn session_closed(&self, _context: &TransportContext) {}

    fn session_cancelled<'a>(
        &'a self,
        _context: &'a TransportContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

pub struct WebSocketSessionGuard {
    protocol: Arc<dyn WebSocketProtocol>,
    context: TransportContext,
}

impl WebSocketSessionGuard {
    pub fn new(protocol: Arc<dyn WebSocketProtocol>, context: TransportContext) -> Self {
        protocol.session_opened(&context);
        Self { protocol, context }
    }
}

impl Drop for WebSocketSessionGuard {
    fn drop(&mut self) {
        self.protocol.session_closed(&self.context);
    }
}

#[cfg(all(feature = "websocket", feature = "dccp"))]
pub mod dccp;
#[cfg(all(feature = "websocket", feature = "http1"))]
pub mod http1;
#[cfg(all(feature = "websocket", feature = "http3"))]
pub mod http3;
#[cfg(all(feature = "websocket", feature = "quic"))]
pub mod quic;
#[cfg(all(feature = "websocket", feature = "rtp"))]
pub mod rtp;
#[cfg(all(feature = "websocket", feature = "rudp"))]
pub mod rudp;
#[cfg(all(feature = "websocket", feature = "sctp"))]
pub mod sctp;
#[cfg(all(feature = "websocket", feature = "srt"))]
pub mod srt;
#[cfg(all(feature = "websocket", feature = "tcp"))]
pub mod tcp;
#[cfg(all(feature = "websocket", feature = "udp"))]
pub mod udp;
#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
pub mod udplite;

/// JSON-RPC session running over a WebSocket connection.
pub struct JsonRpcWebSocket {
    protocol: Arc<dyn JsonRpcProtocol>,
}

impl JsonRpcWebSocket {
    pub fn new(protocol: Arc<dyn JsonRpcProtocol>) -> Self {
        Self { protocol }
    }
}

#[cfg(any(feature = "dccp", feature = "sctp"))]
pub fn protocol_adapter(protocol: Arc<dyn WebSocketProtocol>) -> Arc<dyn JsonRpcProtocol> {
    protocol_adapter_with_kind(protocol, "websocket")
}

#[cfg(any(feature = "dccp", feature = "sctp", feature = "srt"))]
pub fn protocol_adapter_with_kind(
    protocol: Arc<dyn WebSocketProtocol>,
    kind: &'static str,
) -> Arc<dyn JsonRpcProtocol> {
    Arc::new(WebSocketJsonRpcAdapter {
        protocol,
        kind,
        sessions: std::sync::Mutex::new(std::collections::HashMap::new()),
    })
}

#[cfg(any(feature = "dccp", feature = "sctp", feature = "srt"))]
struct WebSocketJsonRpcAdapter {
    protocol: Arc<dyn WebSocketProtocol>,
    kind: &'static str,
    sessions: std::sync::Mutex<std::collections::HashMap<String, AdaptedSession>>,
}

#[cfg(any(feature = "dccp", feature = "sctp", feature = "srt"))]
struct AdaptedSession {
    context: TransportContext,
    relay: Option<tokio::task::JoinHandle<()>>,
    closing: std::sync::atomic::AtomicBool,
}

#[cfg(any(feature = "dccp", feature = "sctp", feature = "srt"))]
impl Drop for AdaptedSession {
    fn drop(&mut self) {
        if let Some(relay) = &self.relay {
            relay.abort();
        }
    }
}

#[cfg(any(all(
    feature = "websocket",
    any(
        feature = "tcp",
        feature = "http1",
        feature = "http3",
        feature = "quic",
        feature = "udp",
        feature = "rtp",
        feature = "rudp",
        feature = "coap",
        all(feature = "udplite", any(target_os = "linux", target_os = "android"),),
    ),
),))]
fn message_bytes_for_log(message: &WebSocketMessage) -> Bytes {
    // Retain binary payloads without copying while the protocol owns the message.
    match message {
        WebSocketMessage::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
        WebSocketMessage::Binary(bytes)
        | WebSocketMessage::Ping(bytes)
        | WebSocketMessage::Pong(bytes) => bytes.clone(),
    }
}

#[cfg(any(
    test,
    feature = "udp",
    feature = "rtp",
    feature = "rudp",
    feature = "coap",
    feature = "udplite",
    feature = "dccp",
    feature = "sctp",
    feature = "srt",
))]
async fn handle_stateless_message(
    protocol: &dyn WebSocketProtocol,
    message: WebSocketMessage,
    kind: &str,
) -> Option<WebSocketMessage> {
    let message = match message {
        WebSocketMessage::Ping(payload) => return Some(WebSocketMessage::Pong(payload)),
        WebSocketMessage::Pong(_) => return None,
        message @ (WebSocketMessage::Text(_) | WebSocketMessage::Binary(_)) => message,
    };
    let context = TransportSession::stateless(kind);
    protocol
        .handle_message_with_context(message, &context)
        .await
}

#[cfg(any(feature = "dccp", feature = "sctp", feature = "srt"))]
impl JsonRpcProtocol for WebSocketJsonRpcAdapter {
    fn session_cancelled<'a>(
        &'a self,
        context: &'a TransportContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        let mut context = context.clone();
        context.transport = self.kind.to_owned();
        Box::pin(async move { self.protocol.session_cancelled(&context).await })
    }

    fn handle_message<'a>(&'a self, message: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
        Box::pin(async move {
            let inbound = match json_rpc_2_core::decode_websocket_message(message) {
                Ok(Some(inbound)) => inbound,
                Ok(None) => return None,
                Err(error) => {
                    json_rpc_2_transport::log_transport_error(
                        self.kind,
                        "stateless",
                        "decode frame",
                        &error,
                    );
                    return None;
                }
            };
            let inbound = match inbound {
                WebSocketMessage::Ping(payload) => {
                    return match json_rpc_2_core::encode_websocket_message(&WebSocketMessage::Pong(
                        payload,
                    )) {
                        Ok(frame) => Some(Bytes::from(frame)),
                        Err(error) => {
                            json_rpc_2_transport::log_transport_error(
                                self.kind,
                                "stateless",
                                "encode pong",
                                &error,
                            );
                            None
                        }
                    };
                }
                WebSocketMessage::Pong(_) => return None,
                message @ (WebSocketMessage::Text(_) | WebSocketMessage::Binary(_)) => message,
            };
            let outbound =
                handle_stateless_message(self.protocol.as_ref(), inbound, self.kind).await?;
            match json_rpc_2_core::encode_websocket_message(&outbound) {
                Ok(frame) => Some(Bytes::from(frame)),
                Err(error) => {
                    json_rpc_2_transport::log_transport_error(
                        self.kind,
                        "stateless",
                        "encode response",
                        &error,
                    );
                    None
                }
            }
        })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        message: &'a [u8],
        context: &'a TransportContext,
    ) -> json_rpc_2_transport::ProtocolFuture<'a> {
        let context = self
            .sessions
            .lock()
            .unwrap()
            .get(&context.connection_id)
            .map(|session| session.context.clone())
            .unwrap_or_else(|| {
                let mut context = context.clone();
                context.transport = self.kind.to_owned();
                context
            });
        Box::pin(async move {
            let inbound = match json_rpc_2_core::decode_websocket_message(message) {
                Ok(Some(inbound)) => inbound,
                Ok(None) => {
                    if let Some(session) = self.sessions.lock().unwrap().get(&context.connection_id)
                    {
                        session
                            .closing
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                    return None;
                }
                result => {
                    if let Err(error) = result {
                        json_rpc_2_transport::log_transport_error(
                            "WebSocket",
                            &context.connection_id,
                            "decode frame",
                            &error,
                        );
                    }
                    if let Some(session) = self.sessions.lock().unwrap().get(&context.connection_id)
                    {
                        session
                            .closing
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                    return None;
                }
            };
            let inbound = match inbound {
                WebSocketMessage::Ping(payload) => {
                    return match json_rpc_2_core::encode_websocket_message(&WebSocketMessage::Pong(
                        payload,
                    )) {
                        Ok(frame) => Some(Bytes::from(frame)),
                        Err(error) => {
                            json_rpc_2_transport::log_transport_error(
                                self.kind,
                                &context.connection_id,
                                "encode pong",
                                &error,
                            );
                            None
                        }
                    };
                }
                WebSocketMessage::Pong(_) => return None,
                message @ (WebSocketMessage::Text(_) | WebSocketMessage::Binary(_)) => message,
            };
            let outbound = self
                .protocol
                .handle_message_with_context(inbound, &context)
                .await?;
            match json_rpc_2_core::encode_websocket_message(&outbound) {
                Ok(frame) => Some(Bytes::from(frame)),
                Err(error) => {
                    json_rpc_2_transport::log_transport_error(
                        self.kind,
                        &context.connection_id,
                        "encode response",
                        &error,
                    );
                    None
                }
            }
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        let mut translated = context.clone();
        translated.transport = self.kind.to_owned();
        let relay = context.push_sender.clone().map(|carrier| {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
            translated.push_sender = Some(sender);
            tokio::spawn(async move {
                while let Some(payload) = receiver.recv().await {
                    let frame =
                        match json_rpc_2_core::encode_websocket_message(&push_message(payload)) {
                            Ok(frame) => frame,
                            Err(error) => {
                                json_rpc_2_transport::log_transport_error(
                                    "WebSocket",
                                    "session",
                                    "encode push",
                                    &error,
                                );
                                break;
                            }
                        };
                    if carrier.send(Bytes::from(frame)).is_err() {
                        break;
                    }
                }
            })
        });
        self.sessions.lock().unwrap().insert(
            context.connection_id.clone(),
            AdaptedSession {
                context: translated.clone(),
                relay,
                closing: std::sync::atomic::AtomicBool::new(false),
            },
        );
        self.protocol.session_opened(&translated);
    }

    fn session_closed(&self, context: &TransportContext) {
        let translated = self.sessions.lock().unwrap().remove(&context.connection_id);
        if let Some(session) = translated {
            self.protocol.session_closed(&session.context);
        }
    }

    fn session_close_requested(&self, context: &TransportContext) -> bool {
        self.sessions
            .lock()
            .unwrap()
            .get(&context.connection_id)
            .is_some_and(|session| session.closing.load(std::sync::atomic::Ordering::Acquire))
    }
}

#[cfg(any(test, feature = "sctp", feature = "srt", feature = "dccp",))]
fn push_message(payload: Bytes) -> WebSocketMessage {
    if let Ok(text) = std::str::from_utf8(&payload) {
        WebSocketMessage::Text(text.to_owned())
    } else {
        WebSocketMessage::Binary(payload)
    }
}

impl WebSocketProtocol for JsonRpcWebSocket {
    fn session_cancelled<'a>(
        &'a self,
        context: &'a TransportContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        self.protocol.session_cancelled(context)
    }

    fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a> {
        self.process_message(message, None)
    }

    fn handle_message_with_context<'a>(
        &'a self,
        message: WebSocketMessage,
        context: &'a TransportContext,
    ) -> WebSocketFuture<'a> {
        self.process_message(message, Some(context))
    }

    fn session_opened(&self, context: &TransportContext) {
        self.protocol.session_opened(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.protocol.session_closed(context);
    }
}

impl JsonRpcWebSocket {
    fn process_message<'a>(
        &'a self,
        message: WebSocketMessage,
        context: Option<&'a TransportContext>,
    ) -> WebSocketFuture<'a> {
        Box::pin(async move {
            let (bytes, is_text) = match message {
                WebSocketMessage::Text(text) => (Bytes::from(text.into_bytes()), true),
                WebSocketMessage::Binary(bytes) => (bytes, false),
                WebSocketMessage::Ping(payload) => {
                    return Some(WebSocketMessage::Pong(payload));
                }
                WebSocketMessage::Pong(_) => return None,
            };
            let fallback_context;
            let context = match context {
                Some(context) => context,
                None => {
                    fallback_context = TransportSession::stateless("websocket");
                    &fallback_context
                }
            };
            // The WebSocket codec requires one contiguous application frame.
            let response = self.protocol.handle_owned_message(bytes, context).await?;
            let is_text = is_text && !response.binary;
            let response = response.into_bytes();
            Some(match std::str::from_utf8(&response) {
                Ok(text) if is_text => WebSocketMessage::Text(text.to_owned()),
                _ => WebSocketMessage::Binary(response),
            })
        })
    }
}

/// Abstract server transport that establishes WebSocket connections.
///
/// Implement this for an HTTP/1.1 Upgrade adapter, an HTTP/3 extended CONNECT
/// adapter, or another carrier without changing the WebSocket session protocol.
pub trait WebSocketTransport: Send + 'static {
    /// Serve until `shutdown` is cancelled, then close every session with a
    /// WebSocket Close frame (1001 Going Away) and close the carrier.
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture;

    /// Run the transport in a background docs that can be closed later.
    fn start(self, protocol: Arc<dyn WebSocketProtocol>) -> RunningTransport
    where
        Self: Sized,
    {
        RunningTransport::spawn(|shutdown| self.run(protocol, shutdown))
    }
}

/// Future returned by a WebSocket carrier server.
pub type WebSocketTransportFuture = TransportFuture;

#[cfg(test)]
mod tests;
