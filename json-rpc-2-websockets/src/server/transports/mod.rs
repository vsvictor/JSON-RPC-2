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

#[cfg(any(feature = "dccp", feature = "sctp"))]
fn protocol_adapter_with_kind(
    protocol: Arc<dyn WebSocketProtocol>,
    kind: &'static str,
) -> Arc<dyn JsonRpcProtocol> {
    Arc::new(WebSocketJsonRpcAdapter { protocol, kind })
}

#[cfg(any(feature = "dccp", feature = "sctp"))]
struct WebSocketJsonRpcAdapter {
    protocol: Arc<dyn WebSocketProtocol>,
    kind: &'static str,
}

#[cfg(any(feature = "mqtt", feature = "amqp"))]
async fn handle_broker_message(
    protocol: &dyn WebSocketProtocol,
    message: WebSocketMessage,
    transport: &str,
) -> Option<WebSocketMessage> {
    let request = message_bytes_for_log(&message);
    let started = std::time::Instant::now();
    let kind = match transport {
        "WebSocket/AMQP" => "ws-amqp",
        "WebSocket/MQTT" => "ws-mqtt",
        _ => "websocket",
    };
    let response = handle_stateless_message(protocol, message, kind).await;
    let response_bytes = response
        .as_ref()
        .map(json_rpc_2_core::websocket_message_bytes);
    json_rpc_2_transport::log_json_rpc(
        transport,
        "external",
        &request,
        response_bytes,
        started.elapsed(),
    );
    response
}

fn message_bytes_for_log(message: &WebSocketMessage) -> Bytes {
    // Retain binary payloads without copying while the protocol owns the message.
    match message {
        WebSocketMessage::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
        WebSocketMessage::Binary(bytes) => bytes.clone(),
    }
}

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

#[cfg(any(feature = "dccp", feature = "sctp"))]
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
        Box::pin(async move {
            let inbound = json_rpc_2_core::decode_websocket_message(message).ok()??;
            let outbound = self
                .protocol
                .handle_message_with_context(inbound, context)
                .await?;
            json_rpc_2_core::encode_websocket_message(&outbound)
                .ok()
                .map(Bytes::from)
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        self.protocol.session_opened(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.protocol.session_closed(context);
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
