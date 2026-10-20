use std::net::SocketAddr;

use json_rpc_2_core::{RpcClientTransport, TransportError};
use json_rpc_2_datagram::client::transports::udplite::UdpLiteClientTransport;

use super::DatagramWebSocketClientTransport;
use json_rpc_2_core::{UdpLiteConfig, UdpReliabilityConfig};

/// Dedicated WebSocket-style transport over reliable UDP-Lite datagrams.
pub struct UdpLiteWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<UdpLiteClientTransport>,
}

impl UdpLiteWebSocketClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_configs(
            address,
            UdpReliabilityConfig::default(),
            UdpLiteConfig::default(),
        )
        .await
    }

    pub async fn connect_with_config(
        address: SocketAddr,
        config: UdpReliabilityConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_configs(address, config, UdpLiteConfig::default()).await
    }

    pub async fn connect_with_configs(
        address: SocketAddr,
        config: UdpReliabilityConfig,
        udp_lite: UdpLiteConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            inner: DatagramWebSocketClientTransport::new(
                UdpLiteClientTransport::connect_with_configs(address, config, udp_lite).await?,
                "ws-udplite",
                "WebSocket/UDP-Lite peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for UdpLiteWebSocketClientTransport {
    fn send<'a>(&'a self, message: bytes::Bytes) -> json_rpc_2_core::ClientTransportFuture<'a> {
        self.inner.send(message)
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: bytes::Bytes,
        expects_response: bool,
    ) -> json_rpc_2_core::ClientTransportFuture<'a> {
        self.inner.send_with_expectation(message, expects_response)
    }

    fn send_cancelable<'a>(
        &'a self,
        message: bytes::Bytes,
        expects_response: bool,
        cancel: tokio_util::sync::CancellationToken,
    ) -> json_rpc_2_core::ClientTransportFuture<'a> {
        self.inner
            .send_cancelable(message, expects_response, cancel)
    }

    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn peer(&self) -> Option<String> {
        self.inner.peer()
    }

    fn transport_ping<'a>(&'a self) -> json_rpc_2_core::ClientPingFuture<'a> {
        self.inner.transport_ping()
    }

    fn reconnect<'a>(&'a self) -> json_rpc_2_core::ClientRecoverFuture<'a> {
        self.inner.reconnect()
    }

    fn close<'a>(&'a self) -> json_rpc_2_core::ClientCloseFuture<'a> {
        self.inner.close()
    }
}
