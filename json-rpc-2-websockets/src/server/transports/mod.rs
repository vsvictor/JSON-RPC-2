//! HTTP-independent WebSocket session contract and JSON-RPC session adapter.

use std::sync::Arc;

use bytes::Bytes;
pub use json_rpc_2_core::{WebSocketFuture, WebSocketMessage, WebSocketProtocol};

use json_rpc_2_transport::{CancellationToken, JsonRpcProtocol, RunningTransport, TransportFuture};

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
    Arc::new(WebSocketJsonRpcAdapter { protocol })
}

#[cfg(any(feature = "dccp", feature = "sctp"))]
struct WebSocketJsonRpcAdapter {
    protocol: Arc<dyn WebSocketProtocol>,
}

#[cfg(any(feature = "dccp", feature = "sctp"))]
impl JsonRpcProtocol for WebSocketJsonRpcAdapter {
    fn handle_message<'a>(&'a self, message: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
        Box::pin(async move {
            let inbound = json_rpc_2_core::decode_websocket_message(message).ok()??;
            let outbound = self.protocol.handle_message(inbound).await?;
            json_rpc_2_core::encode_websocket_message(&outbound)
                .ok()
                .map(Bytes::from)
        })
    }
}

impl WebSocketProtocol for JsonRpcWebSocket {
    fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a> {
        Box::pin(async move {
            let (bytes, is_text) = match message {
                WebSocketMessage::Text(text) => (Bytes::from(text.into_bytes()), true),
                WebSocketMessage::Binary(bytes) => (bytes, false),
            };
            let response = self.protocol.handle_message(&bytes).await?;
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
