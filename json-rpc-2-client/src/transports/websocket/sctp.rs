//! WebSocket-style framing over the SCTP transport.

use std::net::SocketAddr;

use crate::{RpcClientTransport, TransportError, transports::sctp::SctpClientTransport};

use super::DatagramWebSocketClientTransport;

/// Dedicated WebSocket-style transport over SCTP packets.
pub struct SctpWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<SctpClientTransport>,
}

impl SctpWebSocketClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Ok(Self {
            inner: DatagramWebSocketClientTransport::new(
                SctpClientTransport::connect(address).await?,
                "ws-sctp",
                "WebSocket/SCTP peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for SctpWebSocketClientTransport {
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
