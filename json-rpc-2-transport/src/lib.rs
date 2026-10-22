//! Transport contracts and runtime primitives shared by clients and servers.

mod client_transport;
mod server_transport;
mod udplite;

pub use udplite::apply_udp_lite_coverage;

pub use client_transport::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, closed, connect_tcp_stream, expects_response,
    transport_error,
};
pub use server_transport::{
    CancellationToken, GatewayRequestMetadata, H3_NO_ERROR, JsonRpcProtocol, ProtocolFuture,
    ProtocolSessionGuard, QUIC_NO_ERROR, RunningTransport, RuntimeMetrics, RuntimeMetricsSnapshot,
    ServerLimits, ServerShutdownPolicy, Transport, TransportContext, TransportFuture,
    TransportLimits, TransportRuntime, TransportSession, apply_tcp_listener_tuning,
    apply_tcp_stream_tuning, log_control, log_json_rpc, log_transport_error, multiplex,
};

/// TCP congestion control profile used by TCP-based transports.
#[derive(
    Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash,
)]
#[serde(rename_all = "kebab-case")]
pub enum TcpCongestionControl {
    #[default]
    SystemDefault,
    Bbr,
}

impl TcpCongestionControl {
    pub fn as_socket_name(self) -> Option<&'static [u8]> {
        match self {
            Self::SystemDefault => None,
            Self::Bbr => Some(b"bbr"),
        }
    }
}

/// Tuning knobs shared by TCP-based transports.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash)]
pub struct TcpTuningConfig {
    /// Disable Nagle's algorithm on connected sockets.
    pub no_delay: bool,
    /// Per-socket congestion control override.
    pub congestion_control: TcpCongestionControl,
    /// Enable TCP Fast Open where the OS and the transport adapter support it.
    pub fast_open: bool,
    /// Pending Fast Open queue length used by server listeners when Fast Open
    /// is enabled.
    pub fast_open_queue_len: u32,
}

impl Default for TcpTuningConfig {
    fn default() -> Self {
        Self {
            no_delay: true,
            congestion_control: TcpCongestionControl::SystemDefault,
            fast_open: false,
            fast_open_queue_len: 1024,
        }
    }
}

pub const CONTROL_PATH: &str = "/.well-known/json-rpc-2/ping";
pub const CONTROL_PING: &[u8] = b"#json-rpc-2-control-ping";
pub const CONTROL_PONG: &[u8] = b"#json-rpc-2-control-pong";
pub const QUIC_ALPN: &[u8] = b"json-rpc-2";
pub const TLS_ALPN: &[u8] = b"json-rpc-2-tls";
pub const WS_QUIC_ALPN: &[u8] = b"json-rpc-2-ws";
pub const H3_ALPN: &[u8] = b"h3";

/// Whether a transport serializes exchanges on one carrier or multiplexes
/// concurrent exchanges over independent streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransportMultiplexing {
    Serialized,
    Multiplexed,
}

/// Whether a transport requires TLS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransportSecurity {
    Plain,
    Tls,
}

impl TransportSecurity {
    pub fn is_tls(self) -> bool {
        matches!(self, Self::Tls)
    }
}

/// The underlying socket family a transport binds to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransportSocket {
    Tcp,
    Udp,
    Dccp,
    Sctp,
}

impl TransportSocket {
    pub fn is_udp(self) -> bool {
        matches!(self, Self::Udp)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
            Self::Dccp => "DCCP",
            Self::Sctp => "SCTP",
        }
    }
}

/// Structured capability set for a concrete transport kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TransportCapabilitySet {
    pub multiplexing: TransportMultiplexing,
    pub security: TransportSecurity,
    pub socket: TransportSocket,
    pub supports_ping: bool,
}

/// Capability model implemented by transport kinds and transport adapters.
pub trait TransportCapabilities {
    fn capabilities(&self) -> TransportCapabilitySet;

    fn multiplexing(&self) -> TransportMultiplexing {
        self.capabilities().multiplexing
    }

    fn security(&self) -> TransportSecurity {
        self.capabilities().security
    }

    fn socket(&self) -> TransportSocket {
        self.capabilities().socket
    }

    fn supports_ping(&self) -> bool {
        self.capabilities().supports_ping
    }
}
