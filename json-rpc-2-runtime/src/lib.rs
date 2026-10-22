//! Multi-transport server orchestration over the dispatcher and transport crates.

pub use json_rpc_2_server::{
    RegistrationError, RpcError, RpcServer, ServerDispatcher, ServerDispatcherBuilder,
    ServerMetricsSnapshot,
};
pub use json_rpc_2_transport::{
    CONTROL_PATH, CONTROL_PING, CONTROL_PONG, CancellationToken, GatewayRequestMetadata, H3_ALPN,
    H3_NO_ERROR, ProtocolSessionGuard, QUIC_ALPN, RunningTransport, RuntimeMetrics,
    RuntimeMetricsSnapshot, ServerLimits, ServerShutdownPolicy, TcpCongestionControl,
    TcpTuningConfig, Transport, TransportContext, TransportFuture, TransportRuntime,
    TransportSession, WS_QUIC_ALPN, apply_tcp_listener_tuning, apply_tcp_stream_tuning,
    log_control, log_json_rpc, log_transport_error, multiplex,
};
pub use json_rpc_2_transport::{
    TransportCapabilities, TransportCapabilitySet, TransportMultiplexing, TransportSecurity,
    TransportSocket,
};

pub type TransportLimits = ServerLimits;

pub mod multi;
#[cfg(feature = "amqp")]
pub use multi::AmqpConfig;
#[cfg(feature = "mqtt")]
pub use multi::MqttConfig;

pub use multi::{
    HasBind, HasTransport, HealthHandle, HealthSnapshot, MissingBind, MultiTransportServer,
    MultiTransportServerBuilder, NoTransport, RunningServer, TransportKind,
};
