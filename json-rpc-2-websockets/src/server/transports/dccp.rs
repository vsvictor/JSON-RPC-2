//! WebSocket-style framing over the DCCP transport.

use std::{io, net::SocketAddr, sync::Arc};

use super::{WebSocketProtocol, WebSocketTransportFuture, protocol_adapter};
use json_rpc_2_datagram::server::transports::dccp::DccpTransport;
use json_rpc_2_transport::{CancellationToken, Transport};

/// Dedicated WebSocket-style framing over DCCP datagrams.
pub struct DccpWebSocketTransport {
    inner: DccpTransport,
}

impl DccpWebSocketTransport {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            inner: DccpTransport::bind(address)?,
        })
    }

    pub fn with_runtime(mut self, runtime: Arc<json_rpc_2_transport::TransportRuntime>) -> Self {
        self.inner = self.inner.with_runtime(runtime);
        self
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }
}

impl super::WebSocketTransport for DccpWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        self.inner.run(protocol_adapter(protocol), shutdown)
    }
}
