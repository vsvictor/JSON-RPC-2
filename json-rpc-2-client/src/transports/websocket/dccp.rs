//! WebSocket-style framing over the DCCP transport.

use std::net::SocketAddr;

use crate::{RpcClientTransport, TransportError, transports::dccp::DccpClientTransport};

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
    fn send<'a>(&'a self, message: bytes::Bytes) -> crate::ClientTransportFuture<'a> {
        self.inner.send(message)
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: bytes::Bytes,
        expects_response: bool,
    ) -> crate::ClientTransportFuture<'a> {
        self.inner.send_with_expectation(message, expects_response)
    }

    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn peer(&self) -> Option<String> {
        self.inner.peer()
    }

    fn transport_ping<'a>(&'a self) -> crate::ClientPingFuture<'a> {
        self.inner.transport_ping()
    }

    fn reconnect<'a>(&'a self) -> crate::ClientRecoverFuture<'a> {
        self.inner.reconnect()
    }

    fn close<'a>(&'a self) -> crate::ClientCloseFuture<'a> {
        self.inner.close()
    }
}
