//! WebSocket-style framing over the SCTP transport.

use std::net::SocketAddr;

use json_rpc_2_datagram::client::transports::sctp::SctpClientTransport;
use json_rpc_2_transport::{RpcClientTransport, TransportError};

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
    fn send<'a>(
        &'a self,
        message: bytes::Bytes,
    ) -> json_rpc_2_transport::ClientTransportFuture<'a> {
        self.inner.send(message)
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
