//! WebSocket-style framing over the DCCP transport.

use std::net::SocketAddr;

use json_rpc_2_datagram::client::transports::dccp::DccpClientTransport;
use json_rpc_2_transport::{RpcClientTransport, TransportError};

use super::DatagramWebSocketClientTransport;

/// Dedicated WebSocket-style transport over DCCP datagrams.
pub struct DccpWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<DccpClientTransport>,
}

impl DccpWebSocketClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Ok(Self {
            inner: DatagramWebSocketClientTransport::new(
                DccpClientTransport::connect(address).await?,
                "ws-dccp",
                "WebSocket/DCCP peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for DccpWebSocketClientTransport {
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
