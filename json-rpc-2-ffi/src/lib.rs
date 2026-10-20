//! UniFFI wrapper around the `json-rpc-2` Rust client.
//!
//! This crate does **not** reimplement the JSON-RPC 2.0 protocol or any
//! transport: it is a thin binding layer that drives the existing
//! [`json_rpc_2::client::RpcClient`] and every client transport the crate
//! ships (`tcp`, `udp`, `rtp`, `rudp`, `srt`, `udplite`, `dccp`, `sctp`, `http1`, `quic`,
//! `http3`, `ws-tcp`, `ws-udp`, `ws-rtp`, `ws-rudp`, `ws-srt`, `ws-udplite`, `ws-dccp`,
//! `ws-sctp`, `ws-quic`, `ws-http1`, `ws-http3`), exposing
//! an async-free, blocking API (methods block the calling thread on a
//! dedicated Tokio runtime) that UniFFI turns into idiomatic Kotlin and
//! Swift bindings. The Kotlin/Swift sides wrap these blocking calls in
//! coroutines/async functions so callers still get a non-blocking API.

#[cfg(any(feature = "quic", feature = "http3"))]
use std::net::{Ipv4Addr, Ipv6Addr};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
#[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
use json_rpc_2::client::transports::dccp::DccpClientTransport;
#[cfg(feature = "http1")]
use json_rpc_2::client::transports::http1::Http1ClientTransport;
#[cfg(feature = "http3")]
use json_rpc_2::client::transports::http3::Http3ClientTransport;
#[cfg(feature = "quic")]
use json_rpc_2::client::transports::quic::QuicClientTransport;
#[cfg(feature = "rtp")]
use json_rpc_2::client::transports::rtp::RtpClientTransport;
#[cfg(feature = "rudp")]
use json_rpc_2::client::transports::rudp::RudpClientTransport;
#[cfg(feature = "mqtt")]
use json_rpc_2::client::transports::mqtt::MqttClientTransport;
#[cfg(feature = "coap")]
use json_rpc_2::client::transports::coap::CoapClientTransport;
#[cfg(feature = "amqp")]
use json_rpc_2::client::transports::amqp::AmqpClientTransport;
#[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
use json_rpc_2::client::transports::sctp::SctpClientTransport;
#[cfg(feature = "srt")]
use json_rpc_2::client::transports::srt::SrtClientTransport;
#[cfg(feature = "tcp")]
use json_rpc_2::client::transports::tcp::TcpClientTransport;
#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
use json_rpc_2::client::transports::udplite::UdpLiteClientTransport;
#[cfg(feature = "udp")]
use json_rpc_2::client::transports::udp::UdpClientTransport;
#[cfg(all(
    feature = "websocket",
    feature = "dccp",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2::client::transports::websocket::dccp::DccpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "http1"))]
use json_rpc_2::client::transports::websocket::http1::Http1WebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "http3"))]
use json_rpc_2::client::transports::websocket::http3::Http3WebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "quic"))]
use json_rpc_2::client::transports::websocket::quic::QuicWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "rtp"))]
use json_rpc_2::client::transports::websocket::rtp::RtpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "rudp"))]
use json_rpc_2::client::transports::websocket::rudp::RudpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "mqtt"))]
use json_rpc_2::client::transports::websocket::mqtt::MqttWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "coap"))]
use json_rpc_2::client::transports::websocket::coap::CoapWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "amqp"))]
use json_rpc_2::client::transports::websocket::amqp::AmqpWebSocketClientTransport;
#[cfg(all(
    feature = "websocket",
    feature = "sctp",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2::client::transports::websocket::sctp::SctpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "srt"))]
use json_rpc_2::client::transports::websocket::srt::SrtWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "tcp"))]
use json_rpc_2::client::transports::websocket::tcp::TcpWebSocketClientTransport;
#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2::client::transports::websocket::udplite::UdpLiteWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "udp"))]
use json_rpc_2::client::transports::websocket::udp::UdpWebSocketClientTransport;
#[cfg(feature = "http3")]
use json_rpc_2::server::multi::H3_ALPN;
#[cfg(feature = "quic")]
use json_rpc_2::server::multi::QUIC_ALPN;
#[cfg(all(feature = "websocket", feature = "quic"))]
use json_rpc_2::server::multi::WS_QUIC_ALPN;
use json_rpc_2::{
    RpcError, TransportCapabilities, TransportCapabilitySet, TransportMultiplexing,
    TransportSecurity, TransportSocket,
    client::{
        ClientCloseFuture, ClientPingFuture, ClientTransportFuture, RpcClient as CoreClient,
        RpcClientError, RpcClientTransport,
    },
};
#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
use json_rpc_2::UdpLiteConfig;
#[cfg(any(feature = "quic", feature = "http3"))]
use quinn::crypto::rustls::QuicClientConfig;
#[cfg(any(feature = "quic", feature = "http3"))]
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

uniffi::setup_scaffolding!();

/// Error surfaced to Kotlin/Swift callers. Mirrors
/// [`json_rpc_2::client::RpcClientError`] without needing UniFFI to know
/// about that type directly.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    #[error("transport error: {details}")]
    Transport { details: String },
    #[error("JSON error: {details}")]
    Json { details: String },
    #[error("invalid JSON-RPC response: {details}")]
    InvalidResponse { details: String },
    #[error("invalid client configuration: {details}")]
    InvalidConfiguration { details: String },
    #[error("server did not return a response")]
    MissingResponse,
    #[error("response ID does not match request ID")]
    IdMismatch,
    #[error("request `{method}` timed out after {timeout_ms} ms")]
    Timeout { method: String, timeout_ms: u64 },
    #[error("request `{method}` was cancelled")]
    Cancelled { method: String },
    #[error("client session is closed")]
    SessionClosed,
    #[error("remote error {code}: {details}")]
    Remote { code: i64, details: String },
}

impl From<RpcClientError> for FfiError {
    fn from(error: RpcClientError) -> Self {
        match error {
            RpcClientError::Transport(error) => Self::Transport {
                details: error.to_string(),
            },
            RpcClientError::Serialization(error) => Self::Json {
                details: error.to_string(),
            },
            RpcClientError::InvalidResponse(details) => Self::InvalidResponse { details },
            RpcClientError::InvalidConfiguration(details) => Self::InvalidConfiguration { details },
            RpcClientError::MissingResponse => Self::MissingResponse,
            RpcClientError::IdMismatch => Self::IdMismatch,
            RpcClientError::Timeout { method, timeout } => Self::Timeout {
                method,
                timeout_ms: timeout.as_millis() as u64,
            },
            RpcClientError::Cancelled { method } => Self::Cancelled { method },
            RpcClientError::SessionClosed => Self::SessionClosed,
            RpcClientError::Remote(RpcError { code, message, .. }) => Self::Remote {
                code: code.into(),
                details: message,
            },
        }
    }
}

impl From<serde_json::Error> for FfiError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json {
            details: error.to_string(),
        }
    }
}

/// Wrap any `Display` error (transport setup, TLS, DNS...) as [`FfiError::Transport`].
fn transport_error(error: impl std::fmt::Display) -> FfiError {
    FfiError::Transport {
        details: error.to_string(),
    }
}

/// Every client transport `json-rpc-2` ships, matching
/// [`json_rpc_2::server::multi::TransportKind`] one-to-one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum FfiTransportKind {
    /// Newline-delimited JSON over one persistent TCP connection.
    #[cfg(feature = "tcp")]
    Tcp,
    /// Reliable application-level JSON-RPC over UDP datagrams.
    #[cfg(feature = "udp")]
    Udp,
    /// RTP data packets plus RTCP APP control messages over UDP.
    #[cfg(feature = "rtp")]
    Rtp,
    /// Reliable UDP with explicit packet sequencing and retransmission.
    #[cfg(feature = "rudp")]
    Rudp,
    #[cfg(feature = "mqtt")]
    Mqtt,
    #[cfg(feature = "coap")]
    Coap,
    #[cfg(feature = "amqp")]
    Amqp,
    /// Encrypted reliable UDP-style datagrams using an SRT-like framing.
    #[cfg(feature = "srt")]
    Srt,
    /// UDP-Lite datagrams with configurable checksum coverage.
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    UdpLite,
    /// DCCP datagrams with congestion control but without guaranteed delivery.
    #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
    Dccp,
    /// SCTP packets with message boundaries and association-oriented sessions.
    #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
    Sctp,
    /// HTTP/1.1 `POST /` over one persistent keep-alive connection.
    #[cfg(feature = "http1")]
    Http1,
    /// Dedicated WebSocket framing over one persistent TCP connection.
    #[cfg(all(feature = "websocket", feature = "tcp"))]
    WsTcp,
    /// Dedicated WebSocket-style framing over reliable UDP datagrams.
    #[cfg(all(feature = "websocket", feature = "udp"))]
    WsUdp,
    /// Dedicated WebSocket-style framing over RTP data packets with RTCP control.
    #[cfg(all(feature = "websocket", feature = "rtp"))]
    WsRtp,
    /// Dedicated WebSocket-style framing over reliable RUDP datagrams.
    #[cfg(all(feature = "websocket", feature = "rudp"))]
    WsRudp,
    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    WsMqtt,
    #[cfg(all(feature = "websocket", feature = "coap"))]
    WsCoap,
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    WsAmqp,
    /// Dedicated WebSocket-style framing over encrypted SRT-style datagrams.
    #[cfg(all(feature = "websocket", feature = "srt"))]
    WsSrt,
    /// Dedicated WebSocket-style framing over reliable UDP-Lite datagrams.
    #[cfg(all(
        feature = "websocket",
        feature = "udplite",
        any(target_os = "linux", target_os = "android")
    ))]
    WsUdpLite,
    /// Dedicated WebSocket-style framing over DCCP datagrams.
    #[cfg(all(
        feature = "websocket",
        feature = "dccp",
        any(target_os = "linux", target_os = "android")
    ))]
    WsDccp,
    /// Dedicated WebSocket-style framing over SCTP packets.
    #[cfg(all(
        feature = "websocket",
        feature = "sctp",
        any(target_os = "linux", target_os = "android")
    ))]
    WsSctp,
    /// WebSocket established with the HTTP/1.1 Upgrade mechanism.
    #[cfg(all(feature = "websocket", feature = "http1"))]
    WsHttp1,
    /// Raw QUIC: one connection, one bidirectional stream per message. TLS.
    #[cfg(feature = "quic")]
    Quic,
    /// HTTP/3 `POST /`, one request stream per message. TLS.
    #[cfg(feature = "http3")]
    Http3,
    /// Dedicated WebSocket framing over one persistent QUIC connection. TLS.
    #[cfg(all(feature = "websocket", feature = "quic"))]
    WsQuic,
    /// WebSocket established with HTTP/3 extended CONNECT. TLS.
    #[cfg(all(feature = "websocket", feature = "http3"))]
    WsHttp3,
}

impl FfiTransportKind {
    fn is_tls(self) -> bool {
        self.security().is_tls()
    }
}

impl TransportCapabilities for FfiTransportKind {
    fn capabilities(&self) -> TransportCapabilitySet {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(feature = "udp")]
            Self::Udp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "rtp")]
            Self::Rtp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "rudp")]
            Self::Rudp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "mqtt")]
            Self::Mqtt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(feature = "coap")]
            Self::Coap => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "amqp")]
            Self::Amqp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(feature = "srt")]
            Self::Srt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Dccp,
                supports_ping: true,
            },
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Sctp,
                supports_ping: true,
            },
            #[cfg(feature = "http1")]
            Self::Http1 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Dccp,
                supports_ping: true,
            },
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Sctp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(feature = "quic")]
            Self::Quic => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "http3")]
            Self::Http3 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
        }
    }
}

/// Where and how to connect. `ca_pem` and `server_name` are only used by the
/// TLS-based transports (`quic`, `http3`, `ws-quic`, `ws-http3`); the self-signed
/// certificate the server publishes must be trusted explicitly, there is no
/// public CA to fall back on. `srt_passphrase` is only used by `srt` and
/// `ws-srt`. `udplite_*_coverage` is only used by `udplite` and
/// `ws-udplite`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ConnectOptions {
    /// Host name or IP address of the server.
    pub host: String,
    /// Port of the chosen transport (see the server's `.env`, e.g.
    /// `JSON_RPC_TCP_PORT`, `JSON_RPC_WS_HTTP1_PORT`, ...).
    pub port: u16,
    /// PEM-encoded certificate(s) to trust; required for `quic`, `http3`,
    /// `ws-quic`, and `ws-http3`.
    pub ca_pem: Option<String>,
    /// TLS name to verify; defaults to `host` (as in the original CLI
    /// example).
    pub server_name: Option<String>,
    /// Shared passphrase for `srt` and `ws-srt`.
    pub srt_passphrase: Option<String>,
    /// UDP-Lite outbound checksum coverage in bytes (`0` = full coverage).
    pub udplite_send_coverage: Option<u16>,
    /// UDP-Lite inbound checksum coverage in bytes (`0` = full coverage).
    pub udplite_recv_coverage: Option<u16>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct TcpConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UdpConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct RtpConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct RudpConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct MqttConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct CoapConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct AmqpConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct SrtConnectOptions {
    pub host: String,
    pub port: u16,
    pub passphrase: String,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UdpLiteConnectOptions {
    pub host: String,
    pub port: u16,
    pub send_coverage: Option<u16>,
    pub recv_coverage: Option<u16>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct DccpConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct SctpConnectOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct WebSocketConnectOptions {
    pub host: String,
    pub port: u16,
    pub path: Option<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct QuicConnectOptions {
    pub host: String,
    pub port: u16,
    pub ca_pem: String,
    pub server_name: Option<String>,
}

/// One concrete transport, so [`RpcClient`] doesn't need to be generic:
/// `RpcClientTransport` methods return boxed futures but the trait itself
/// isn't usable as `dyn` across the crate boundary in a way the orphan rule
/// allows us to blanket-implement here, so a closed enum delegating to each
/// concrete transport is the simplest local way to let one Kotlin/Swift type
/// support every transport.
enum AnyTransport {
    #[cfg(feature = "tcp")]
    Tcp(TcpClientTransport),
    #[cfg(feature = "udp")]
    Udp(UdpClientTransport),
    #[cfg(feature = "rtp")]
    Rtp(RtpClientTransport),
    #[cfg(feature = "rudp")]
    Rudp(RudpClientTransport),
    #[cfg(feature = "mqtt")]
    Mqtt(MqttClientTransport),
    #[cfg(feature = "coap")]
    Coap(CoapClientTransport),
    #[cfg(feature = "amqp")]
    Amqp(AmqpClientTransport),
    #[cfg(feature = "srt")]
    Srt(SrtClientTransport),
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    UdpLite(UdpLiteClientTransport),
    #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
    Dccp(DccpClientTransport),
    #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
    Sctp(SctpClientTransport),
    #[cfg(feature = "http1")]
    Http1(Http1ClientTransport),
    #[cfg(all(feature = "websocket", feature = "tcp"))]
    WsTcp(TcpWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "udp"))]
    WsUdp(UdpWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "rtp"))]
    WsRtp(RtpWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "rudp"))]
    WsRudp(RudpWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    WsMqtt(MqttWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "coap"))]
    WsCoap(CoapWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    WsAmqp(AmqpWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "srt"))]
    WsSrt(SrtWebSocketClientTransport),
    #[cfg(all(
        feature = "websocket",
        feature = "udplite",
        any(target_os = "linux", target_os = "android")
    ))]
    WsUdpLite(UdpLiteWebSocketClientTransport),
    #[cfg(all(
        feature = "websocket",
        feature = "dccp",
        any(target_os = "linux", target_os = "android")
    ))]
    WsDccp(DccpWebSocketClientTransport),
    #[cfg(all(
        feature = "websocket",
        feature = "sctp",
        any(target_os = "linux", target_os = "android")
    ))]
    WsSctp(SctpWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "http1"))]
    WsHttp1(Http1WebSocketClientTransport),
    #[cfg(feature = "quic")]
    Quic(QuicClientTransport),
    #[cfg(feature = "http3")]
    Http3(Http3ClientTransport),
    #[cfg(all(feature = "websocket", feature = "quic"))]
    WsQuic(QuicWebSocketClientTransport),
    #[cfg(all(feature = "websocket", feature = "http3"))]
    WsHttp3(Http3WebSocketClientTransport),
}

impl RpcClientTransport for AnyTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.send(message),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.send(message),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.send(message),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.send(message),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.send(message),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.send(message),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.send(message),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.send(message),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => transport.send(message),
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.send(message),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.send(message),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.send(message),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => transport.send(message),
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.send(message),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => transport.send(message),
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.send(message),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.send(message),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => transport.send(message),
        }
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => {
                transport.send_with_expectation(message, expects_response)
            }
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.send_with_expectation(message, expects_response),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => transport.send_with_expectation(message, expects_response),
        }
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => {
                transport.send_cancelable(message, expects_response, cancel)
            }
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => {
                transport.send_cancelable(message, expects_response, cancel)
            }
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => {
                transport.send_cancelable(message, expects_response, cancel)
            }
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.send_cancelable(message, expects_response, cancel),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => {
                transport.send_cancelable(message, expects_response, cancel)
            }
        }
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.transport_ping(),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.transport_ping(),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.transport_ping(),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.transport_ping(),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.transport_ping(),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.transport_ping(),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.transport_ping(),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.transport_ping(),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => transport.transport_ping(),
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.transport_ping(),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.transport_ping(),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.transport_ping(),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => transport.transport_ping(),
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.transport_ping(),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => transport.transport_ping(),
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.transport_ping(),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.transport_ping(),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => transport.transport_ping(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.kind(),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.kind(),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.kind(),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.kind(),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.kind(),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.kind(),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.kind(),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.kind(),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => transport.kind(),
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.kind(),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.kind(),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.kind(),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => transport.kind(),
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.kind(),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => transport.kind(),
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.kind(),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.kind(),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => transport.kind(),
        }
    }

    fn peer(&self) -> Option<String> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.peer(),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.peer(),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.peer(),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.peer(),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.peer(),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.peer(),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.peer(),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.peer(),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => transport.peer(),
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.peer(),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.peer(),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.peer(),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => transport.peer(),
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.peer(),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => transport.peer(),
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.peer(),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.peer(),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => transport.peer(),
        }
    }

    fn reconnect<'a>(&'a self) -> json_rpc_2::client::ClientRecoverFuture<'a> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.reconnect(),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.reconnect(),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.reconnect(),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.reconnect(),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.reconnect(),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.reconnect(),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.reconnect(),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.reconnect(),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => transport.reconnect(),
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.reconnect(),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.reconnect(),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.reconnect(),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => transport.reconnect(),
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.reconnect(),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => transport.reconnect(),
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.reconnect(),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.reconnect(),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => transport.reconnect(),
        }
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(transport) => transport.close(),
            #[cfg(feature = "udp")]
            Self::Udp(transport) => transport.close(),
            #[cfg(feature = "rtp")]
            Self::Rtp(transport) => transport.close(),
            #[cfg(feature = "rudp")]
            Self::Rudp(transport) => transport.close(),
            #[cfg(feature = "mqtt")]
            Self::Mqtt(transport) => transport.close(),
            #[cfg(feature = "coap")]
            Self::Coap(transport) => transport.close(),
            #[cfg(feature = "amqp")]
            Self::Amqp(transport) => transport.close(),
            #[cfg(feature = "srt")]
            Self::Srt(transport) => transport.close(),
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            Self::UdpLite(transport) => transport.close(),
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            Self::Dccp(transport) => transport.close(),
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            Self::Sctp(transport) => transport.close(),
            #[cfg(feature = "http1")]
            Self::Http1(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            Self::WsTcp(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "udp"))]
            Self::WsUdp(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            Self::WsRtp(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            Self::WsRudp(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WsMqtt(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "coap"))]
            Self::WsCoap(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WsAmqp(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "srt"))]
            Self::WsSrt(transport) => transport.close(),
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsUdpLite(transport) => transport.close(),
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsDccp(transport) => transport.close(),
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            Self::WsSctp(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "http1"))]
            Self::WsHttp1(transport) => transport.close(),
            #[cfg(feature = "quic")]
            Self::Quic(transport) => transport.close(),
            #[cfg(feature = "http3")]
            Self::Http3(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "quic"))]
            Self::WsQuic(transport) => transport.close(),
            #[cfg(all(feature = "websocket", feature = "http3"))]
            Self::WsHttp3(transport) => transport.close(),
        }
    }
}

/// Dedicated background Tokio runtime that drives every connection created
/// by this library. Exported methods call [`Runtime::block_on`] on this
/// runtime from whatever foreign thread invoked them (JNI/Swift calling
/// thread), which is never one of this runtime's own worker threads, so
/// blocking here is always safe.
fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("json-rpc-2-ffi")
            .enable_all()
            .build()
            .expect("failed to start the json-rpc-2-ffi Tokio runtime")
    })
}

/// Resolve `host:port` to one socket address, like the original CLI example:
/// an IP address is used directly, a host name is resolved and its first
/// result used.
async fn resolve(host: &str, port: u16) -> Result<SocketAddr, FfiError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    tokio::net::lookup_host((host, port))
        .await
        .map_err(transport_error)?
        .next()
        .ok_or_else(|| transport_error(format!("cannot resolve {host}")))
}

/// The unspecified address of `ip`'s family: a client socket must be of the
/// same address family as the server, the OS then picks the interface.
#[cfg(any(feature = "quic", feature = "http3"))]
fn unspecified(ip: IpAddr) -> IpAddr {
    if ip.is_ipv6() {
        Ipv6Addr::UNSPECIFIED.into()
    } else {
        Ipv4Addr::UNSPECIFIED.into()
    }
}

fn wrap_client(transport: AnyTransport) -> Arc<RpcClient> {
    Arc::new(RpcClient {
        inner: CoreClient::new(transport),
    })
}

#[cfg(any(
    feature = "tcp",
    feature = "http1",
    all(feature = "websocket", feature = "tcp")
))]
impl TcpConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(feature = "udp")]
impl UdpConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(feature = "rtp")]
impl RtpConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(feature = "rudp")]
impl RudpConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(feature = "mqtt")]
impl MqttConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(feature = "coap")]
impl CoapConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(feature = "amqp")]
impl AmqpConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(feature = "srt")]
impl SrtConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: Some(self.passphrase),
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
impl UdpLiteConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: self.send_coverage,
            udplite_recv_coverage: self.recv_coverage,
        }
    }
}

#[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
impl DccpConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
impl SctpConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: None,
            server_name: None,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

#[cfg(any(
    feature = "quic",
    feature = "http3",
    all(feature = "websocket", feature = "quic"),
    all(feature = "websocket", feature = "http3")
))]
impl QuicConnectOptions {
    fn into_connect_options(self) -> ConnectOptions {
        ConnectOptions {
            host: self.host,
            port: self.port,
            ca_pem: Some(self.ca_pem),
            server_name: self.server_name,
            srt_passphrase: None,
            udplite_send_coverage: None,
            udplite_recv_coverage: None,
        }
    }
}

/// Build a `quinn::Endpoint` trusting `ca_pem`, for `alpn` (QUIC vs HTTP/3).
/// Mirrors the original CLI example's `Tls::client`.
#[cfg(any(feature = "quic", feature = "http3"))]
fn quic_endpoint(
    local: IpAddr,
    alpn: &'static [u8],
    ca_pem: &str,
) -> Result<quinn::Endpoint, FfiError> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        let certificate = certificate.map_err(transport_error)?;
        roots.add(certificate).map_err(transport_error)?;
    }
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(transport_error)?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![alpn.to_vec()];
    let mut endpoint =
        quinn::Endpoint::client(SocketAddr::new(local, 0)).map_err(transport_error)?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls).map_err(transport_error)?,
    )));
    Ok(endpoint)
}

/// JSON-RPC 2.0 client connected over any of the transports `json-rpc-2`
/// ships.
#[derive(uniffi::Object)]
pub struct RpcClient {
    inner: CoreClient<AnyTransport>,
}

#[uniffi::export]
impl RpcClient {
    /// Connect with the given transport. `options.ca_pem` is required for
    /// `quic`, `http3`, `ws-quic`, and `ws-http3`; `options.srt_passphrase`
    /// is required for `srt` and `ws-srt`.
    #[uniffi::constructor]
    pub fn connect(kind: FfiTransportKind, options: ConnectOptions) -> Result<Arc<Self>, FfiError> {
        runtime().block_on(async move {
            let to = resolve(&options.host, options.port).await?;
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            let udplite = UdpLiteConfig {
                send_checksum_coverage: options.udplite_send_coverage.unwrap_or(8),
                recv_checksum_coverage: options.udplite_recv_coverage.unwrap_or(8),
            };
            #[cfg(any(feature = "quic", feature = "http3"))]
            let server_name = options
                .server_name
                .clone()
                .unwrap_or_else(|| options.host.clone());

            if kind.is_tls() && options.ca_pem.is_none() {
                return Err(transport_error(format!(
                    "ca_pem is required for the {kind:?} transport"
                )));
            }
            #[cfg(all(
                feature = "udplite",
                feature = "websocket",
                any(target_os = "linux", target_os = "android")
            ))]
            if matches!(kind, FfiTransportKind::UdpLite | FfiTransportKind::WsUdpLite) {
                udplite.validate().map_err(transport_error)?;
            }
            #[cfg(all(
                feature = "udplite",
                not(feature = "websocket"),
                any(target_os = "linux", target_os = "android")
            ))]
            if matches!(kind, FfiTransportKind::UdpLite) {
                udplite.validate().map_err(transport_error)?;
            }
            #[cfg(all(feature = "srt", feature = "websocket"))]
            if matches!(kind, FfiTransportKind::Srt | FfiTransportKind::WsSrt)
                && options.srt_passphrase.as_deref().is_none_or(str::is_empty)
            {
                return Err(transport_error(format!(
                    "srt_passphrase is required for the {kind:?} transport"
                )));
            }
            #[cfg(all(feature = "srt", not(feature = "websocket")))]
            if matches!(kind, FfiTransportKind::Srt)
                && options.srt_passphrase.as_deref().is_none_or(str::is_empty)
            {
                return Err(transport_error(format!(
                    "srt_passphrase is required for the {kind:?} transport"
                )));
            }

            let transport = match kind {
                #[cfg(feature = "tcp")]
                FfiTransportKind::Tcp => AnyTransport::Tcp(
                    TcpClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "udp")]
                FfiTransportKind::Udp => AnyTransport::Udp(
                    UdpClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "rtp")]
                FfiTransportKind::Rtp => AnyTransport::Rtp(
                    RtpClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "rudp")]
                FfiTransportKind::Rudp => AnyTransport::Rudp(
                    RudpClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "mqtt")]
                FfiTransportKind::Mqtt => AnyTransport::Mqtt(
                    MqttClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "coap")]
                FfiTransportKind::Coap => AnyTransport::Coap(
                    CoapClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "amqp")]
                FfiTransportKind::Amqp => AnyTransport::Amqp(
                    AmqpClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "srt")]
                FfiTransportKind::Srt => AnyTransport::Srt(
                    SrtClientTransport::connect(
                        to,
                        options.srt_passphrase.clone().expect("checked above"),
                    )
                    .await
                    .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
                FfiTransportKind::UdpLite => AnyTransport::UdpLite(
                    UdpLiteClientTransport::connect_with_configs(
                        to,
                        json_rpc_2::UdpReliabilityConfig::default(),
                        udplite,
                    )
                    .await
                    .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
                FfiTransportKind::Dccp => AnyTransport::Dccp(
                    DccpClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
                FfiTransportKind::Sctp => AnyTransport::Sctp(
                    SctpClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(feature = "http1")]
                FfiTransportKind::Http1 => AnyTransport::Http1(
                    Http1ClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "tcp"))]
                FfiTransportKind::WsTcp => AnyTransport::WsTcp(
                    TcpWebSocketClientTransport::connect(&format!("ws://{to}"))
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "udp"))]
                FfiTransportKind::WsUdp => AnyTransport::WsUdp(
                    UdpWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "rtp"))]
                FfiTransportKind::WsRtp => AnyTransport::WsRtp(
                    RtpWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "rudp"))]
                FfiTransportKind::WsRudp => AnyTransport::WsRudp(
                    RudpWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "mqtt"))]
                FfiTransportKind::WsMqtt => AnyTransport::WsMqtt(
                    MqttWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "coap"))]
                FfiTransportKind::WsCoap => AnyTransport::WsCoap(
                    CoapWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "amqp"))]
                FfiTransportKind::WsAmqp => AnyTransport::WsAmqp(
                    AmqpWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "srt"))]
                FfiTransportKind::WsSrt => AnyTransport::WsSrt(
                    SrtWebSocketClientTransport::connect(
                        to,
                        options.srt_passphrase.clone().expect("checked above"),
                    )
                    .await
                    .map_err(transport_error)?,
                ),
                #[cfg(all(
                    feature = "websocket",
                    feature = "udplite",
                    any(target_os = "linux", target_os = "android")
                ))]
                FfiTransportKind::WsUdpLite => AnyTransport::WsUdpLite(
                    UdpLiteWebSocketClientTransport::connect_with_configs(
                        to,
                        json_rpc_2::UdpReliabilityConfig::default(),
                        udplite,
                    )
                    .await
                    .map_err(transport_error)?,
                ),
                #[cfg(all(
                    feature = "websocket",
                    feature = "dccp",
                    any(target_os = "linux", target_os = "android")
                ))]
                FfiTransportKind::WsDccp => AnyTransport::WsDccp(
                    DccpWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(
                    feature = "websocket",
                    feature = "sctp",
                    any(target_os = "linux", target_os = "android")
                ))]
                FfiTransportKind::WsSctp => AnyTransport::WsSctp(
                    SctpWebSocketClientTransport::connect(to)
                        .await
                        .map_err(transport_error)?,
                ),
                #[cfg(all(feature = "websocket", feature = "http1"))]
                FfiTransportKind::WsHttp1 => {
                    let url = format!("ws://{to}/ws");
                    AnyTransport::WsHttp1(
                        Http1WebSocketClientTransport::connect(&url)
                            .await
                            .map_err(transport_error)?,
                    )
                }
                #[cfg(feature = "quic")]
                FfiTransportKind::Quic => {
                    let endpoint = quic_endpoint(
                        unspecified(to.ip()),
                        QUIC_ALPN,
                        options.ca_pem.as_deref().expect("checked above"),
                    )?;
                    AnyTransport::Quic(
                        QuicClientTransport::connect(endpoint, to, &server_name)
                            .await
                            .map_err(transport_error)?,
                    )
                }
                #[cfg(feature = "http3")]
                FfiTransportKind::Http3 => {
                    let endpoint = quic_endpoint(
                        unspecified(to.ip()),
                        H3_ALPN,
                        options.ca_pem.as_deref().expect("checked above"),
                    )?;
                    AnyTransport::Http3(
                        Http3ClientTransport::connect(endpoint, to, &server_name)
                            .await
                            .map_err(transport_error)?,
                    )
                }
                #[cfg(all(feature = "websocket", feature = "quic"))]
                FfiTransportKind::WsQuic => {
                    let endpoint = quic_endpoint(
                        unspecified(to.ip()),
                        WS_QUIC_ALPN,
                        options.ca_pem.as_deref().expect("checked above"),
                    )?;
                    AnyTransport::WsQuic(
                        QuicWebSocketClientTransport::connect(endpoint, to, &server_name)
                            .await
                            .map_err(transport_error)?,
                    )
                }
                #[cfg(all(feature = "websocket", feature = "http3"))]
                FfiTransportKind::WsHttp3 => {
                    let endpoint = quic_endpoint(
                        unspecified(to.ip()),
                        H3_ALPN,
                        options.ca_pem.as_deref().expect("checked above"),
                    )?;
                    AnyTransport::WsHttp3(
                        Http3WebSocketClientTransport::connect(endpoint, to, &server_name, "/ws")
                            .await
                            .map_err(transport_error)?,
                    )
                }
            };

            Ok(wrap_client(transport))
        })
    }

    /// Call a method and return its JSON-encoded result.
    ///
    /// `params_json`, when present, must be a JSON-encoded object or array.
    pub fn call(&self, method: String, params_json: Option<String>) -> Result<String, FfiError> {
        runtime().block_on(async {
            let params = params_json
                .map(|json| serde_json::from_str(&json))
                .transpose()?;
            let result = self.inner.request_value(method, params).await?;
            Ok(result.to_string())
        })
    }

    /// Send a notification (no response is expected or awaited).
    pub fn notify(&self, method: String, params_json: Option<String>) -> Result<(), FfiError> {
        runtime().block_on(async {
            let params = params_json
                .map(|json| serde_json::from_str(&json))
                .transpose()?;
            self.inner.notify(method, params).await?;
            Ok(())
        })
    }

    /// Gracefully close the connection.
    ///
    /// Named `disconnect` rather than `close` because UniFFI's generated
    /// Kotlin bindings already use `close()`/`AutoCloseable` for releasing
    /// the native object itself; reusing that name would silently shadow
    /// the object-disposal method instead of calling into Rust.
    pub fn disconnect(&self) -> Result<(), FfiError> {
        runtime().block_on(async { Ok(self.inner.close().await?) })
    }

    /// Perform one transport-level ping/pong. Bypasses the JSON-RPC
    /// protocol entirely (no `method`/`params`/`id`/`result`), so it never
    /// mixes with application traffic; useful to check liveness or to
    /// drive a UI indicator explicitly.
    pub fn transport_ping(&self) -> Result<(), FfiError> {
        runtime().block_on(async { Ok(self.inner.transport_ping().await?) })
    }

    /// Start (or stop) a periodic background transport-level ping used as
    /// a keep-alive. `frequency_ms = 0` disables it. Pings run on the same
    /// Tokio runtime as every other call and are stopped automatically by
    /// [`RpcClient::disconnect`].
    pub fn set_ping_frequency(&self, frequency_ms: u64) -> Result<(), FfiError> {
        runtime().block_on(async { Ok(self.inner.set_ping_frequency(frequency_ms).await?) })
    }

    /// Configure how many consecutive missed pong ticks trigger recovery.
    pub fn set_max_missed_pongs(&self, count: u32) -> Result<(), FfiError> {
        runtime().block_on(async { Ok(self.inner.set_max_missed_pongs(count).await?) })
    }

    /// Configure the JSON-RPC request timeout for subsequent calls.
    pub fn set_request_timeout_ms(&self, timeout_ms: u64) -> Result<(), FfiError> {
        self.inner.set_request_timeout_millis(timeout_ms)?;
        Ok(())
    }
}

#[cfg(feature = "tcp")]
#[uniffi::export]
pub fn connect_tcp(options: TcpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Tcp, options.into_connect_options())
}

#[cfg(feature = "udp")]
#[uniffi::export]
pub fn connect_udp(options: UdpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Udp, options.into_connect_options())
}

#[cfg(feature = "rtp")]
#[uniffi::export]
pub fn connect_rtp(options: RtpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Rtp, options.into_connect_options())
}

#[cfg(feature = "rudp")]
#[uniffi::export]
pub fn connect_rudp(options: RudpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Rudp, options.into_connect_options())
}

#[cfg(feature = "mqtt")]
#[uniffi::export]
pub fn connect_mqtt(options: MqttConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Mqtt, options.into_connect_options())
}

#[cfg(feature = "coap")]
#[uniffi::export]
pub fn connect_coap(options: CoapConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Coap, options.into_connect_options())
}

#[cfg(feature = "amqp")]
#[uniffi::export]
pub fn connect_amqp(options: AmqpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Amqp, options.into_connect_options())
}

#[cfg(feature = "srt")]
#[uniffi::export]
pub fn connect_srt(options: SrtConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Srt, options.into_connect_options())
}

#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
#[uniffi::export]
pub fn connect_udplite(options: UdpLiteConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::UdpLite, options.into_connect_options())
}

#[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
#[uniffi::export]
pub fn connect_dccp(options: DccpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Dccp, options.into_connect_options())
}

#[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
#[uniffi::export]
pub fn connect_sctp(options: SctpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Sctp, options.into_connect_options())
}

#[cfg(feature = "http1")]
#[uniffi::export]
pub fn connect_http1(options: TcpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Http1, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "tcp"))]
#[uniffi::export]
pub fn connect_ws_tcp(options: TcpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsTcp, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "udp"))]
#[uniffi::export]
pub fn connect_ws_udp(options: UdpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsUdp, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "rtp"))]
#[uniffi::export]
pub fn connect_ws_rtp(options: RtpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsRtp, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "rudp"))]
#[uniffi::export]
pub fn connect_ws_rudp(options: RudpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsRudp, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "mqtt"))]
#[uniffi::export]
pub fn connect_ws_mqtt(options: MqttConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsMqtt, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "coap"))]
#[uniffi::export]
pub fn connect_ws_coap(options: CoapConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsCoap, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "amqp"))]
#[uniffi::export]
pub fn connect_ws_amqp(options: AmqpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsAmqp, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "srt"))]
#[uniffi::export]
pub fn connect_ws_srt(options: SrtConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsSrt, options.into_connect_options())
}

#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
#[uniffi::export]
pub fn connect_ws_udplite(options: UdpLiteConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsUdpLite, options.into_connect_options())
}

#[cfg(all(
    feature = "websocket",
    feature = "dccp",
    any(target_os = "linux", target_os = "android")
))]
#[uniffi::export]
pub fn connect_ws_dccp(options: DccpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsDccp, options.into_connect_options())
}

#[cfg(all(
    feature = "websocket",
    feature = "sctp",
    any(target_os = "linux", target_os = "android")
))]
#[uniffi::export]
pub fn connect_ws_sctp(options: SctpConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsSctp, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "http1"))]
#[uniffi::export]
pub fn connect_ws_http1(options: WebSocketConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    runtime().block_on(async move {
        let to = resolve(&options.host, options.port).await?;
        let path = options.path.unwrap_or_else(|| "/ws".to_owned());
        let url = format!("ws://{to}{path}");
        let transport = AnyTransport::WsHttp1(
            Http1WebSocketClientTransport::connect(&url)
                .await
                .map_err(transport_error)?,
        );
        Ok(wrap_client(transport))
    })
}

#[cfg(feature = "quic")]
#[uniffi::export]
pub fn connect_quic(options: QuicConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Quic, options.into_connect_options())
}

#[cfg(feature = "http3")]
#[uniffi::export]
pub fn connect_http3(options: QuicConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::Http3, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "quic"))]
#[uniffi::export]
pub fn connect_ws_quic(options: QuicConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    RpcClient::connect(FfiTransportKind::WsQuic, options.into_connect_options())
}

#[cfg(all(feature = "websocket", feature = "http3"))]
#[uniffi::export]
pub fn connect_ws_http3(options: QuicConnectOptions) -> Result<Arc<RpcClient>, FfiError> {
    runtime().block_on(async move {
        let to = resolve(&options.host, options.port).await?;
        let server_name = options
            .server_name
            .clone()
            .unwrap_or_else(|| options.host.clone());
        let endpoint = quic_endpoint(unspecified(to.ip()), H3_ALPN, &options.ca_pem)?;
        let transport = AnyTransport::WsHttp3(
            Http3WebSocketClientTransport::connect(endpoint, to, &server_name, "/ws")
                .await
                .map_err(transport_error)?,
        );
        Ok(wrap_client(transport))
    })
}
