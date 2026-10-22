use std::net::SocketAddr;

use json_rpc_2_datagram::client::transports::rtp::RtpClientTransport;
use json_rpc_2_transport::{RpcClientTransport, TransportError};

use super::DatagramWebSocketClientTransport;
use json_rpc_2_core::UdpReliabilityConfig;

/// Dedicated WebSocket-style transport over RTP data packets with RTCP control.
pub struct RtpWebSocketClientTransport {
    inner: DatagramWebSocketClientTransport<RtpClientTransport>,
}

impl RtpWebSocketClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_config(address, UdpReliabilityConfig::default()).await
    }

    pub async fn connect_with_config(
        address: SocketAddr,
        config: UdpReliabilityConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            inner: DatagramWebSocketClientTransport::new(
                RtpClientTransport::connect_with_config(address, config).await?,
                "ws-rtp",
                "WebSocket/RTP peer closed the session",
            ),
        })
    }
}

impl RpcClientTransport for RtpWebSocketClientTransport {
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
