use std::net::SocketAddr;

use json_rpc_2_core::{RpcClientTransport, TransportError};
use json_rpc_2_iot::client::transports::amqp::AmqpClientTransport;

use super::DatagramWebSocketClientTransport;

/// Dedicated WebSocket-style transport over AMQP-style frames.
pub struct AmqpWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<AmqpClientTransport>,
}

impl AmqpWebSocketClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Ok(Self {
            inner: DatagramWebSocketClientTransport::new(
                AmqpClientTransport::connect(address).await?,
                "ws-amqp",
                "WebSocket/AMQP peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for AmqpWebSocketClientTransport {
    fn send<'a>(&'a self, message: bytes::Bytes) -> json_rpc_2_core::ClientTransportFuture<'a> { self.inner.send(message) }
    fn send_with_expectation<'a>(&'a self, message: bytes::Bytes, expects_response: bool) -> json_rpc_2_core::ClientTransportFuture<'a> { self.inner.send_with_expectation(message, expects_response) }
    fn send_cancelable<'a>(&'a self, message: bytes::Bytes, expects_response: bool, cancel: tokio_util::sync::CancellationToken) -> json_rpc_2_core::ClientTransportFuture<'a> { self.inner.send_cancelable(message, expects_response, cancel) }
    fn kind(&self) -> &'static str { self.inner.kind() }
    fn peer(&self) -> Option<String> { self.inner.peer() }
    fn transport_ping<'a>(&'a self) -> json_rpc_2_core::ClientPingFuture<'a> { self.inner.transport_ping() }
    fn reconnect<'a>(&'a self) -> json_rpc_2_core::ClientRecoverFuture<'a> { self.inner.reconnect() }
    fn close<'a>(&'a self) -> json_rpc_2_core::ClientCloseFuture<'a> { self.inner.close() }
}
