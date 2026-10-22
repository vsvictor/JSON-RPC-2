use std::net::SocketAddr;

use json_rpc_2_datagram::client::transports::srt::{SrtClientTransport, WS_SRT_STREAM_ID};
use json_rpc_2_transport::{RpcClientTransport, TransportError};

use super::DatagramWebSocketClientTransport;
use json_rpc_2_core::{SrtConfig, UdpReliabilityConfig};

/// WebSocket application frames over a standard encrypted libsrt caller connection.
pub struct SrtWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<SrtClientTransport>,
}

impl SrtWebSocketClientTransport {
    pub async fn recv_push(&self) -> Result<json_rpc_2_core::WebSocketMessage, TransportError> {
        let frame = self.inner.inner.recv_push().await?;
        json_rpc_2_core::decode_websocket_message(&frame)
            .map_err(json_rpc_2_transport::transport_error)?
            .ok_or_else(|| TransportError::new("received WebSocket close instead of push"))
    }

    pub async fn connect(
        address: SocketAddr,
        passphrase: impl Into<String>,
    ) -> Result<Self, TransportError> {
        Self::connect_with_config(
            address,
            UdpReliabilityConfig {
                retransmit_interval: std::time::Duration::from_secs(1),
                ..Default::default()
            },
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
                SrtClientTransport::connect_with_stream_id(
                    address,
                    reliability,
                    srt_config,
                    WS_SRT_STREAM_ID,
                )
                .await?,
                "ws-srt",
                "WebSocket/SRT peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for SrtWebSocketClientTransport {
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
