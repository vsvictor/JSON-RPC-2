use std::net::SocketAddr;

use json_rpc_2_client::{
    RpcClientTransport, TransportError,
    transports::{srt::SrtClientTransport, websocket::DatagramWebSocketClientTransport},
};
use json_rpc_2_core::{SrtConfig, UdpReliabilityConfig};

/// Dedicated WebSocket-style transport over encrypted reliable SRT-style datagrams.
pub struct SrtWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<SrtClientTransport>,
}

impl SrtWebSocketClientTransport {
    pub async fn connect(
        address: SocketAddr,
        passphrase: impl Into<String>,
    ) -> Result<Self, TransportError> {
        Self::connect_with_config(
            address,
            UdpReliabilityConfig::default(),
            SrtConfig {
                passphrase: passphrase.into(),
            },
        )
        .await
    }

    pub async fn connect_with_config(
        address: SocketAddr,
        reliability: UdpReliabilityConfig,
        srt_config: SrtConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            inner: DatagramWebSocketClientTransport::new(
                SrtClientTransport::connect_with_config(address, reliability, srt_config).await?,
                "ws-srt",
                "WebSocket/SRT peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for SrtWebSocketClientTransport {
    fn send<'a>(&'a self, message: bytes::Bytes) -> json_rpc_2_client::ClientTransportFuture<'a> {
        self.inner.send(message)
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: bytes::Bytes,
        expects_response: bool,
    ) -> json_rpc_2_client::ClientTransportFuture<'a> {
        self.inner.send_with_expectation(message, expects_response)
    }

    fn send_cancelable<'a>(
        &'a self,
        message: bytes::Bytes,
        expects_response: bool,
        cancel: tokio_util::sync::CancellationToken,
    ) -> json_rpc_2_client::ClientTransportFuture<'a> {
        self.inner
            .send_cancelable(message, expects_response, cancel)
    }

    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn peer(&self) -> Option<String> {
        self.inner.peer()
    }

    fn transport_ping<'a>(&'a self) -> json_rpc_2_client::ClientPingFuture<'a> {
        self.inner.transport_ping()
    }

    fn reconnect<'a>(&'a self) -> json_rpc_2_client::ClientRecoverFuture<'a> {
        self.inner.reconnect()
    }

    fn close<'a>(&'a self) -> json_rpc_2_client::ClientCloseFuture<'a> {
        self.inner.close()
    }
}
