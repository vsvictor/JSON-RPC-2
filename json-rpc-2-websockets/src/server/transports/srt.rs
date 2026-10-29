//! WebSocket application frames over standard encrypted libsrt connections.

use std::{io, net::SocketAddr, sync::Arc};

use json_rpc_2_core::SrtConfig;
use json_rpc_2_datagram::server::transports::srt::SrtTransport;
use json_rpc_2_transport::{Transport, TransportRuntime};

use super::{CancellationToken, WebSocketProtocol, WebSocketTransport, WebSocketTransportFuture};

pub struct SrtWebSocketTransport {
    inner: SrtTransport,
}

impl SrtWebSocketTransport {
    pub async fn bind(address: SocketAddr, config: SrtConfig) -> io::Result<Self> {
        Ok(Self {
            inner: SrtTransport::bind(address, config).await?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.inner = self.inner.with_runtime(runtime);
        self
    }
}

impl WebSocketTransport for SrtWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        self.inner.run(
            super::protocol_adapter_with_kind(protocol, "ws-srt"),
            shutdown,
        )
    }
}

#[cfg(test)]
mod tests;
