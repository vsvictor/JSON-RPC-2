use std::net::SocketAddr;

use json_rpc_2_iot::client::transports::amqp::AmqpClientTransport;
use json_rpc_2_transport::{RpcClientTransport, TransportError};

use super::DatagramWebSocketClientTransport;
pub use json_rpc_2_iot::AmqpConfig;

/// WebSocket application envelopes over broker-backed AMQP (not AMQP-over-WS).
pub struct AmqpWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<AmqpClientTransport>,
}

impl AmqpWebSocketClientTransport {
    /// Receive and decode an unsolicited server WebSocket push.
    pub async fn receive_push(&self) -> Result<bytes::Bytes, TransportError> {
        let frame = self.inner.inner().receive_push().await?;
        match json_rpc_2_core::decode_websocket_message(&frame).map_err(TransportError::new)? {
            Some(json_rpc_2_core::WebSocketMessage::Binary(payload)) => Ok(payload),
            Some(json_rpc_2_core::WebSocketMessage::Text(payload)) => Ok(payload.into()),
            Some(
                json_rpc_2_core::WebSocketMessage::Ping(_)
                | json_rpc_2_core::WebSocketMessage::Pong(_),
            ) => Err(TransportError::new(
                "unexpected WebSocket control frame in push",
            )),
            None => Err(TransportError::new(
                "WebSocket/AMQP peer closed the session",
            )),
        }
    }
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_config(AmqpConfig {
            request_queue: "json-rpc-2.ws.requests".into(),
            ..AmqpConfig::for_address(address)
        })
        .await
    }

    pub async fn connect_with_config(config: AmqpConfig) -> Result<Self, TransportError> {
        Ok(Self {
            inner: DatagramWebSocketClientTransport::new(
                AmqpClientTransport::connect_with_config(config).await?,
                "ws-amqp",
                "WebSocket/AMQP peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for AmqpWebSocketClientTransport {
    fn send<'a>(
        &'a self,
        message: bytes::Bytes,
    ) -> json_rpc_2_transport::ClientTransportFuture<'a> {
        self.inner.send(message)
    }
    fn send_message<'a>(
        &'a self,
        message: json_rpc_2_transport::TransportMessage,
        expects_response: bool,
    ) -> json_rpc_2_transport::ClientTransportFuture<'a> {
        self.inner.send_message(message, expects_response)
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: bytes::Bytes,
        expects_response: bool,
    ) -> json_rpc_2_transport::ClientTransportFuture<'a> {
        self.inner.send_with_expectation(message, expects_response)
    }
    fn send_cancelable<'a>(
        &'a self,
        message: bytes::Bytes,
        expects_response: bool,
        cancel: tokio_util::sync::CancellationToken,
    ) -> json_rpc_2_transport::ClientTransportFuture<'a> {
        self.inner
            .send_cancelable(message, expects_response, cancel)
    }
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }
    fn peer(&self) -> Option<String> {
        self.inner.peer()
    }
    fn transport_ping<'a>(&'a self) -> json_rpc_2_transport::ClientPingFuture<'a> {
        self.inner.transport_ping()
    }
    fn reconnect<'a>(&'a self) -> json_rpc_2_transport::ClientRecoverFuture<'a> {
        self.inner.reconnect()
    }
    fn close<'a>(&'a self) -> json_rpc_2_transport::ClientCloseFuture<'a> {
        self.inner.close()
    }
}
