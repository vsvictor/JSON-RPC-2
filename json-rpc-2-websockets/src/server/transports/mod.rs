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

#[cfg(all(feature = "websocket", feature = "amqp"))]
pub mod amqp;
#[cfg(all(feature = "websocket", feature = "coap"))]
pub mod coap;
#[cfg(all(feature = "websocket", feature = "dccp"))]
pub mod dccp;
#[cfg(all(feature = "websocket", feature = "http1"))]
pub mod http1;
#[cfg(all(feature = "websocket", feature = "http3"))]
pub mod http3;
#[cfg(all(feature = "websocket", feature = "mqtt"))]
pub mod mqtt;
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
fn protocol_adapter_with_kind(
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

#[cfg(any(feature = "mqtt", feature = "amqp"))]
async fn handle_broker_message(
    protocol: &dyn WebSocketProtocol,
    message: WebSocketMessage,
    transport: &str,
    context: &TransportContext,
) -> Option<WebSocketMessage> {
    let request = message_bytes_for_log(&message);
    let started = std::time::Instant::now();
    let response = protocol.handle_message_with_context(message, context).await;
    let response_bytes = response
        .as_ref()
        .map(json_rpc_2_core::websocket_message_bytes);
    let transport = match transport {
        "ws-mqtt" => "WebSocket/MQTT",
        "ws-amqp" => "WebSocket/AMQP",
        other => other,
    };
    json_rpc_2_transport::log_json_rpc(
        transport,
        "external",
        &request,
        response_bytes,
        started.elapsed(),
    );
    response
}

#[cfg(any(feature = "mqtt", feature = "amqp"))]
pub(super) struct BrokerWebSocketAdapter {
    protocol: Arc<dyn WebSocketProtocol>,
    kind: &'static str,
    sessions: std::sync::Mutex<std::collections::HashMap<String, BrokerWebSocketSession>>,
}

#[cfg(any(feature = "mqtt", feature = "amqp"))]
struct BrokerWebSocketSession {
    context: TransportContext,
    _guard: WebSocketSessionGuard,
    relay: tokio::task::JoinHandle<()>,
    closing: bool,
}

#[cfg(any(feature = "mqtt", feature = "amqp"))]
impl Drop for BrokerWebSocketSession {
    fn drop(&mut self) {
        self.relay.abort();
    }
}

#[cfg(any(feature = "mqtt", feature = "amqp"))]
impl BrokerWebSocketAdapter {
    pub fn new(protocol: Arc<dyn WebSocketProtocol>, kind: &'static str) -> Self {
        Self {
            protocol,
            kind,
            sessions: std::sync::Mutex::new(Default::default()),
        }
    }
}

#[cfg(any(feature = "mqtt", feature = "amqp"))]
impl JsonRpcProtocol for BrokerWebSocketAdapter {
    fn handle_message<'a>(&'a self, message: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
        Box::pin(async move {
            let context = TransportSession::stateless(self.kind);
            self.handle_message_with_context(message, &context).await
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
            .unwrap_or_else(|| TransportContext {
                transport: self.kind.to_owned(),
                push_sender: None,
                ..context.clone()
            });
        Box::pin(async move {
            let message = match json_rpc_2_core::decode_websocket_message(message) {
                Ok(message) => message,
                Err(error) => {
                    json_rpc_2_transport::log_transport_error(
                        self.kind,
                        &context.connection_id,
                        "decode frame",
                        &error,
                    );
                    None
                }
            };
            let Some(message) = message else {
                if let Some(session) = self
                    .sessions
                    .lock()
                    .unwrap()
                    .get_mut(&context.connection_id)
                {
                    session.closing = true;
                }
                return None;
            };
            let response =
                handle_broker_message(self.protocol.as_ref(), message, self.kind, &context).await?;
            json_rpc_2_core::encode_websocket_message(&response)
                .ok()
                .map(Bytes::from)
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        let mut translated = context.clone();
        translated.transport = self.kind.to_owned();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let raw_sender = context.push_sender.clone();
        translated.push_sender = raw_sender.as_ref().map(|_| sender);
        let guard = WebSocketSessionGuard::new(self.protocol.clone(), translated.clone());
        let relay = tokio::spawn(async move {
            while let Some(payload) = receiver.recv().await {
                let frame = match json_rpc_2_core::encode_websocket_message(&push_message(payload))
                {
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
                if raw_sender
                    .as_ref()
                    .is_none_or(|sender| sender.send(Bytes::from(frame)).is_err())
                {
                    break;
                }
            }
        });
        self.sessions.lock().unwrap().insert(
            context.connection_id.clone(),
            BrokerWebSocketSession {
                context: translated,
                _guard: guard,
                relay,
                closing: false,
            },
        );
    }

    fn session_closed(&self, context: &TransportContext) {
        self.sessions.lock().unwrap().remove(&context.connection_id);
    }

    fn session_close_requested(&self, context: &TransportContext) -> bool {
        self.sessions
            .lock()
            .unwrap()
            .get(&context.connection_id)
            .is_some_and(|session| session.closing)
    }
}

fn message_bytes_for_log(message: &WebSocketMessage) -> Bytes {
    // Retain binary payloads without copying while the protocol owns the message.
    match message {
        WebSocketMessage::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
        WebSocketMessage::Binary(bytes) => bytes.clone(),
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
    let context = TransportSession::stateless(kind);
    protocol
        .handle_message_with_context(message, &context)
        .await
}

#[cfg(any(feature = "dccp", feature = "sctp", feature = "srt"))]
impl JsonRpcProtocol for WebSocketJsonRpcAdapter {
    fn handle_message<'a>(&'a self, message: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
        Box::pin(async move {
            let inbound = json_rpc_2_core::decode_websocket_message(message).ok()??;
            let outbound =
                handle_stateless_message(self.protocol.as_ref(), inbound, self.kind).await?;
            json_rpc_2_core::encode_websocket_message(&outbound)
                .ok()
                .map(Bytes::from)
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
            let outbound = self
                .protocol
                .handle_message_with_context(inbound, &context)
                .await?;
            json_rpc_2_core::encode_websocket_message(&outbound)
                .ok()
                .map(Bytes::from)
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

#[cfg(any(
    test,
    feature = "mqtt",
    feature = "amqp",
    feature = "sctp",
    feature = "srt",
    feature = "dccp",
))]
fn push_message(payload: Bytes) -> WebSocketMessage {
    if !json_rpc_2_transport::gateway::is_gateway_frame(&payload)
        && let Ok(text) = std::str::from_utf8(&payload)
    {
        WebSocketMessage::Text(text.to_owned())
    } else {
        WebSocketMessage::Binary(payload)
    }
}

impl WebSocketProtocol for JsonRpcWebSocket {
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
            let response = self
                .protocol
                .handle_owned_message(bytes, context)
                .await?
                .into_bytes();
            Some(if is_text {
                WebSocketMessage::Text(
                    String::from_utf8(response.to_vec())
                        .expect("serialized JSON-RPC responses are valid UTF-8"),
                )
            } else {
                WebSocketMessage::Binary(response)
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

    /// Run the transport in a background task that can be closed later.
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
mod tests {
    use super::*;

    #[test]
    fn push_preserves_text_binary_and_utf8_gateway_frames() {
        assert_eq!(
            push_message(Bytes::from_static(b"notification")),
            WebSocketMessage::Text("notification".to_owned())
        );
        for payload in [
            Bytes::from_static(b"\xff\0binary"),
            Bytes::from_static(b"JRPC-GW\x03\0utf8"),
        ] {
            assert_eq!(
                push_message(payload.clone()),
                WebSocketMessage::Binary(payload)
            );
        }
    }

    struct Reply {
        request: Bytes,
        response: Bytes,
    }

    impl JsonRpcProtocol for Reply {
        fn handle_message<'a>(
            &'a self,
            input: &'a [u8],
        ) -> json_rpc_2_transport::ProtocolFuture<'a> {
            Box::pin(async move {
                assert_eq!(input, self.request.as_ref());
                Some(self.response.clone())
            })
        }
    }

    #[tokio::test]
    async fn protocol_preserves_binary_and_text_response_kinds() {
        for (request, response, binary) in [
            (
                Bytes::from_static(b"JRPC-GW\x03\0\xff\nrequest"),
                Bytes::from_static(b"JRPC-GW\x03\0\xfe\nresponse"),
                true,
            ),
            (
                Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#),
                Bytes::from_static(br#"{"jsonrpc":"2.0","result":"ok","id":1}"#),
                false,
            ),
        ] {
            let protocol = JsonRpcWebSocket::new(Arc::new(Reply {
                request: request.clone(),
                response: response.clone(),
            }));
            let wrap = |bytes: Bytes| {
                if binary {
                    WebSocketMessage::Binary(bytes)
                } else {
                    WebSocketMessage::Text(std::str::from_utf8(&bytes).unwrap().to_owned())
                }
            };
            assert_eq!(
                protocol.handle_message(wrap(request)).await,
                Some(wrap(response))
            );
        }
    }

    struct OwnedReply {
        request: Bytes,
        kind: &'static str,
    }

    impl JsonRpcProtocol for OwnedReply {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
            panic!("WebSocket requests must use the owned message contract")
        }

        fn handle_owned_message<'a>(
            &'a self,
            input: Bytes,
            context: &'a TransportContext,
        ) -> json_rpc_2_transport::ProtocolMessageFuture<'a> {
            Box::pin(async move {
                assert_eq!(input.as_ptr(), self.request.as_ptr());
                assert_eq!(input, self.request);
                assert_eq!(context.transport, self.kind);
                Some(json_rpc_2_transport::TransportMessage {
                    header: Bytes::from_static(b"JRPC-GW\x03"),
                    payload: Bytes::from_static(b"\xff\nresponse"),
                })
            })
        }
    }

    #[tokio::test]
    async fn protocol_transfers_owned_binary_and_coalesces_response_at_boundary() {
        for kind in ["ws-tcp", "ws-udp"] {
            let request = Bytes::from_static(b"JRPC-GW\x03\xfe\nrequest");
            let protocol = JsonRpcWebSocket::new(Arc::new(OwnedReply {
                request: request.clone(),
                kind,
            }));
            let response = if kind == "ws-tcp" {
                let session = TransportSession::new(kind, true);
                protocol
                    .handle_message_with_context(
                        WebSocketMessage::Binary(request),
                        &session.context,
                    )
                    .await
            } else {
                handle_stateless_message(&protocol, WebSocketMessage::Binary(request), kind).await
            };
            assert_eq!(
                response,
                Some(WebSocketMessage::Binary(Bytes::from_static(
                    b"JRPC-GW\x03\xff\nresponse"
                )))
            );
        }
    }
}
