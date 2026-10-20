#![cfg_attr(
    not(any(
        feature = "tcp",
        feature = "udp",
        feature = "rtp",
        feature = "rudp",
        feature = "srt",
        feature = "udplite",
        feature = "mqtt",
        feature = "coap",
        feature = "amqp",
        feature = "dccp",
        feature = "sctp",
        feature = "quic",
        feature = "http1",
        feature = "http3"
    )),
    allow(
        unused_imports,
        unused_variables,
        unreachable_code,
        unreachable_patterns,
        dead_code
    )
)]

//! One JSON-RPC server exposed over several transports at once.
//!
//! [`MultiTransportServer`] is configured either directly with `set_*`
//! methods, or via [`MultiTransportServer::builder`] which uses typestate to
//! require at least one bind address and at least one enabled transport before
//! build/start.
//!
//! MQTT and AMQP are external broker services, not local listeners. Configure
//! their authenticated URLs and service topic/queue with `set_mqtt_config` or
//! `set_amqp_config` (and `set_ws_*_config` for WebSocket application frames).
//! Broker-only builders need no bind IP. Legacy broker port setters target
//! localhost and never create a broker; port zero is therefore invalid.
//! [`RunningServer::listeners`] lists only local endpoints, while
//! [`RunningServer::broker_transports`] lists outbound service consumers.
//!
//! ```no_run
//! # async fn example(
//! #     certificates: Vec<rustls::pki_types::CertificateDer<'static>>,
//! #     key: rustls::pki_types::PrivateKeyDer<'static>,
//! # ) -> std::io::Result<()> {
//! use std::sync::Arc;
//! use json_rpc_2_runtime::MultiTransportServer;
//! use json_rpc_2_server::RpcServer;
//!
//! let rpc = Arc::new(RpcServer::new());
//! rpc.register("echo", |params| async move { Ok(params) }).unwrap();
//!
//! let server = MultiTransportServer::builder(rpc)
//!     .set_bind(["0.0.0.0".parse().unwrap(), "::".parse().unwrap()])
//!     .set_tcp_port(40400)
//!     .set_udp_port(40401)
//!     .set_rtp_port(40402)
//!     .set_rudp_port(40403)
//!     .set_srt_port(40404)
//!     .set_quic_port(40405)
//!     .set_http1_port(40406)
//!     .set_http3_port(40407)
//!     .set_ws_tcp_port(40408)
//!     .set_ws_udp_port(40409)
//!     .set_ws_rtp_port(40410)
//!     .set_ws_rudp_port(40411)
//!     .set_ws_srt_port(40412)
//!     .set_ws_quic_port(40413)
//!     .set_ws_http1_port(40414)
//!     .set_ws_http3_port(40415)
//!     .set_tls(certificates, key)
//!     .build();
//! let running = server.start().await?;
//! let _listeners = running.listeners().count();
//! // ...
//! running.close().await
//! # }
//! ```

use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

#[cfg(feature = "http1")]
use axum::{Json, Router, routing::get};
#[cfg(any(feature = "quic", feature = "http3"))]
use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Serialize;
#[cfg(any(
    feature = "tcp",
    feature = "http1",
    all(feature = "websocket", feature = "tcp"),
    all(feature = "websocket", feature = "http1")
))]
use tokio::net::TcpListener;

use crate::{
    TransportCapabilities, TransportCapabilitySet, TransportMultiplexing, TransportSecurity,
    TransportSocket,
};
use json_rpc_2_core::{SrtConfig, UdpLiteConfig, UdpReliabilityConfig};
#[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
use json_rpc_2_datagram::server::transports::dccp::DccpTransport;
#[cfg(feature = "rtp")]
use json_rpc_2_datagram::server::transports::rtp::RtpTransport;
#[cfg(feature = "rudp")]
use json_rpc_2_datagram::server::transports::rudp::RudpTransport;
#[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
use json_rpc_2_datagram::server::transports::sctp::SctpTransport;
#[cfg(feature = "srt")]
use json_rpc_2_datagram::server::transports::srt::SrtTransport;
#[cfg(feature = "udp")]
use json_rpc_2_datagram::server::transports::udp::UdpTransport;
#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
use json_rpc_2_datagram::server::transports::udplite::UdpLiteTransport;
#[cfg(feature = "amqp")]
pub use json_rpc_2_iot::AmqpConfig;
#[cfg(feature = "mqtt")]
pub use json_rpc_2_iot::MqttConfig;
#[cfg(feature = "amqp")]
use json_rpc_2_iot::server::transports::amqp::AmqpTransport;
#[cfg(feature = "coap")]
use json_rpc_2_iot::server::transports::coap::CoapTransport;
#[cfg(feature = "mqtt")]
use json_rpc_2_iot::server::transports::mqtt::MqttTransport;
#[cfg(feature = "http3")]
use json_rpc_2_quic::server::transports::http3::Http3Transport;
#[cfg(feature = "quic")]
use json_rpc_2_quic::server::transports::quic::QuicTransport;
use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
#[cfg(feature = "http1")]
use json_rpc_2_tcp::server::transports::http1::Http1Transport;
#[cfg(feature = "tcp")]
use json_rpc_2_tcp::server::transports::tcp::TcpTransport;
use json_rpc_2_transport::TcpTuningConfig;
use json_rpc_2_transport::{
    RunningTransport, RuntimeMetricsSnapshot, ServerLimits, ServerShutdownPolicy, Transport,
    TransportRuntime,
};
#[cfg(all(feature = "websocket", feature = "amqp"))]
use json_rpc_2_websockets::server::transports::amqp::AmqpWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "coap"))]
use json_rpc_2_websockets::server::transports::coap::CoapWebSocketTransport;
#[cfg(all(
    feature = "websocket",
    feature = "dccp",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2_websockets::server::transports::dccp::DccpWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "http1"))]
use json_rpc_2_websockets::server::transports::http1::Http1WebSocketTransport;
#[cfg(all(feature = "websocket", feature = "http3"))]
use json_rpc_2_websockets::server::transports::http3::Http3WebSocketTransport;
#[cfg(all(feature = "websocket", feature = "mqtt"))]
use json_rpc_2_websockets::server::transports::mqtt::MqttWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "quic"))]
use json_rpc_2_websockets::server::transports::quic::QuicWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "rtp"))]
use json_rpc_2_websockets::server::transports::rtp::RtpWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "rudp"))]
use json_rpc_2_websockets::server::transports::rudp::RudpWebSocketTransport;
#[cfg(all(
    feature = "websocket",
    feature = "sctp",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2_websockets::server::transports::sctp::SctpWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "srt"))]
use json_rpc_2_websockets::server::transports::srt::SrtWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "tcp"))]
use json_rpc_2_websockets::server::transports::tcp::TcpWebSocketTransport;
#[cfg(all(feature = "websocket", feature = "udp"))]
use json_rpc_2_websockets::server::transports::udp::UdpWebSocketTransport;
#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2_websockets::server::transports::udplite::UdpLiteWebSocketTransport;
#[cfg(feature = "websocket")]
use json_rpc_2_websockets::server::transports::{
    JsonRpcWebSocket, WebSocketProtocol, WebSocketTransport,
};

const TRANSPORT_SLOT_COUNT: usize = TransportKind::ALL.len();

pub use json_rpc_2_transport::{H3_ALPN, QUIC_ALPN, WS_QUIC_ALPN};

/// The transports over which a [`MultiTransportServer`] is exposed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransportKind {
    #[cfg(feature = "tcp")]
    Tcp,
    #[cfg(feature = "udp")]
    Udp,
    #[cfg(feature = "rtp")]
    Rtp,
    #[cfg(feature = "rudp")]
    Rudp,
    #[cfg(feature = "srt")]
    Srt,
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    UdpLite,
    #[cfg(feature = "mqtt")]
    Mqtt,
    #[cfg(feature = "coap")]
    Coap,
    #[cfg(feature = "amqp")]
    Amqp,
    #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
    Dccp,
    #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
    Sctp,
    #[cfg(feature = "quic")]
    Quic,
    #[cfg(feature = "http1")]
    Http1,
    #[cfg(feature = "http3")]
    Http3,
    #[cfg(all(feature = "websocket", feature = "tcp"))]
    WebSocketTcp,
    #[cfg(all(feature = "websocket", feature = "udp"))]
    WebSocketUdp,
    #[cfg(all(feature = "websocket", feature = "rtp"))]
    WebSocketRtp,
    #[cfg(all(feature = "websocket", feature = "rudp"))]
    WebSocketRudp,
    #[cfg(all(feature = "websocket", feature = "srt"))]
    WebSocketSrt,
    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    WebSocketMqtt,
    #[cfg(all(feature = "websocket", feature = "coap"))]
    WebSocketCoap,
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    WebSocketAmqp,
    #[cfg(all(
        feature = "websocket",
        feature = "udplite",
        any(target_os = "linux", target_os = "android")
    ))]
    WebSocketUdpLite,
    #[cfg(all(
        feature = "websocket",
        feature = "dccp",
        any(target_os = "linux", target_os = "android")
    ))]
    WebSocketDccp,
    #[cfg(all(
        feature = "websocket",
        feature = "sctp",
        any(target_os = "linux", target_os = "android")
    ))]
    WebSocketSctp,
    #[cfg(all(feature = "websocket", feature = "quic"))]
    WebSocketQuic,
    #[cfg(all(feature = "websocket", feature = "http1"))]
    WebSocketHttp1,
    #[cfg(all(feature = "websocket", feature = "http3"))]
    WebSocketHttp3,
}

impl TransportKind {
    /// Whether this transport connects to an external message broker rather
    /// than opening a local listener.
    pub fn is_broker(self) -> bool {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt => true,
            #[cfg(feature = "amqp")]
            Self::Amqp => true,
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            Self::WebSocketMqtt => true,
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            Self::WebSocketAmqp => true,
            _ => false,
        }
    }

    pub const ALL: &[TransportKind] = &[
        #[cfg(feature = "tcp")]
        TransportKind::Tcp,
        #[cfg(feature = "udp")]
        TransportKind::Udp,
        #[cfg(feature = "rtp")]
        TransportKind::Rtp,
        #[cfg(feature = "rudp")]
        TransportKind::Rudp,
        #[cfg(feature = "srt")]
        TransportKind::Srt,
        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        TransportKind::UdpLite,
        #[cfg(feature = "mqtt")]
        TransportKind::Mqtt,
        #[cfg(feature = "coap")]
        TransportKind::Coap,
        #[cfg(feature = "amqp")]
        TransportKind::Amqp,
        #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
        TransportKind::Dccp,
        #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
        TransportKind::Sctp,
        #[cfg(feature = "quic")]
        TransportKind::Quic,
        #[cfg(feature = "http1")]
        TransportKind::Http1,
        #[cfg(feature = "http3")]
        TransportKind::Http3,
        #[cfg(all(feature = "websocket", feature = "tcp"))]
        TransportKind::WebSocketTcp,
        #[cfg(all(feature = "websocket", feature = "udp"))]
        TransportKind::WebSocketUdp,
        #[cfg(all(feature = "websocket", feature = "rtp"))]
        TransportKind::WebSocketRtp,
        #[cfg(all(feature = "websocket", feature = "rudp"))]
        TransportKind::WebSocketRudp,
        #[cfg(all(feature = "websocket", feature = "srt"))]
        TransportKind::WebSocketSrt,
        #[cfg(all(feature = "websocket", feature = "mqtt"))]
        TransportKind::WebSocketMqtt,
        #[cfg(all(feature = "websocket", feature = "coap"))]
        TransportKind::WebSocketCoap,
        #[cfg(all(feature = "websocket", feature = "amqp"))]
        TransportKind::WebSocketAmqp,
        #[cfg(all(
            feature = "websocket",
            feature = "udplite",
            any(target_os = "linux", target_os = "android")
        ))]
        TransportKind::WebSocketUdpLite,
        #[cfg(all(
            feature = "websocket",
            feature = "dccp",
            any(target_os = "linux", target_os = "android")
        ))]
        TransportKind::WebSocketDccp,
        #[cfg(all(
            feature = "websocket",
            feature = "sctp",
            any(target_os = "linux", target_os = "android")
        ))]
        TransportKind::WebSocketSctp,
        #[cfg(all(feature = "websocket", feature = "quic"))]
        TransportKind::WebSocketQuic,
        #[cfg(all(feature = "websocket", feature = "http1"))]
        TransportKind::WebSocketHttp1,
        #[cfg(all(feature = "websocket", feature = "http3"))]
        TransportKind::WebSocketHttp3,
    ];

    /// Short identifier: tcp, udp, rtp, rudp, srt, udplite, dccp, sctp, quic, http1, http3, ws-tcp, ws-udp, ws-rtp, ws-rudp, ws-srt, ws-udplite, ws-dccp, ws-sctp, ws-quic, ws-http1, ws-http3.
    pub fn id(self) -> &'static str {
        match self {
            #[cfg(feature = "tcp")]
            TransportKind::Tcp => "tcp",
            #[cfg(feature = "udp")]
            TransportKind::Udp => "udp",
            #[cfg(feature = "rtp")]
            TransportKind::Rtp => "rtp",
            #[cfg(feature = "rudp")]
            TransportKind::Rudp => "rudp",
            #[cfg(feature = "srt")]
            TransportKind::Srt => "srt",
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            TransportKind::UdpLite => "udplite",
            #[cfg(feature = "mqtt")]
            TransportKind::Mqtt => "mqtt",
            #[cfg(feature = "coap")]
            TransportKind::Coap => "coap",
            #[cfg(feature = "amqp")]
            TransportKind::Amqp => "amqp",
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Dccp => "dccp",
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Sctp => "sctp",
            #[cfg(feature = "quic")]
            TransportKind::Quic => "quic",
            #[cfg(feature = "http1")]
            TransportKind::Http1 => "http1",
            #[cfg(feature = "http3")]
            TransportKind::Http3 => "http3",
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            TransportKind::WebSocketTcp => "ws-tcp",
            #[cfg(all(feature = "websocket", feature = "udp"))]
            TransportKind::WebSocketUdp => "ws-udp",
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            TransportKind::WebSocketRtp => "ws-rtp",
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            TransportKind::WebSocketRudp => "ws-rudp",
            #[cfg(all(feature = "websocket", feature = "srt"))]
            TransportKind::WebSocketSrt => "ws-srt",
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            TransportKind::WebSocketMqtt => "ws-mqtt",
            #[cfg(all(feature = "websocket", feature = "coap"))]
            TransportKind::WebSocketCoap => "ws-coap",
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            TransportKind::WebSocketAmqp => "ws-amqp",
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketUdpLite => "ws-udplite",
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketDccp => "ws-dccp",
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketSctp => "ws-sctp",
            #[cfg(all(feature = "websocket", feature = "quic"))]
            TransportKind::WebSocketQuic => "ws-quic",
            #[cfg(all(feature = "websocket", feature = "http1"))]
            TransportKind::WebSocketHttp1 => "ws-http1",
            #[cfg(all(feature = "websocket", feature = "http3"))]
            TransportKind::WebSocketHttp3 => "ws-http3",
        }
    }

    /// Human-readable name.
    pub fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "tcp")]
            TransportKind::Tcp => "TCP",
            #[cfg(feature = "udp")]
            TransportKind::Udp => "UDP",
            #[cfg(feature = "rtp")]
            TransportKind::Rtp => "RTP/RTCP",
            #[cfg(feature = "rudp")]
            TransportKind::Rudp => "RUDP",
            #[cfg(feature = "srt")]
            TransportKind::Srt => "SRT",
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            TransportKind::UdpLite => "UDP-Lite",
            #[cfg(feature = "mqtt")]
            TransportKind::Mqtt => "MQTT",
            #[cfg(feature = "coap")]
            TransportKind::Coap => "CoAP",
            #[cfg(feature = "amqp")]
            TransportKind::Amqp => "AMQP",
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Dccp => "DCCP",
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Sctp => "SCTP",
            #[cfg(feature = "quic")]
            TransportKind::Quic => "QUIC",
            #[cfg(feature = "http1")]
            TransportKind::Http1 => "HTTP/1.1",
            #[cfg(feature = "http3")]
            TransportKind::Http3 => "HTTP/3",
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            TransportKind::WebSocketTcp => "WebSocket/TCP",
            #[cfg(all(feature = "websocket", feature = "udp"))]
            TransportKind::WebSocketUdp => "WebSocket/UDP",
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            TransportKind::WebSocketRtp => "WebSocket/RTP",
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            TransportKind::WebSocketRudp => "WebSocket/RUDP",
            #[cfg(all(feature = "websocket", feature = "srt"))]
            TransportKind::WebSocketSrt => "WebSocket/SRT",
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            TransportKind::WebSocketMqtt => "WebSocket/MQTT",
            #[cfg(all(feature = "websocket", feature = "coap"))]
            TransportKind::WebSocketCoap => "WebSocket/CoAP",
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            TransportKind::WebSocketAmqp => "WebSocket/AMQP",
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketUdpLite => "WebSocket/UDP-Lite",
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketDccp => "WebSocket/DCCP",
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketSctp => "WebSocket/SCTP",
            #[cfg(all(feature = "websocket", feature = "quic"))]
            TransportKind::WebSocketQuic => "WebSocket/QUIC",
            #[cfg(all(feature = "websocket", feature = "http1"))]
            TransportKind::WebSocketHttp1 => "WebSocket/HTTP/1.1",
            #[cfg(all(feature = "websocket", feature = "http3"))]
            TransportKind::WebSocketHttp3 => "WebSocket/HTTP/3",
        }
    }

    /// The transport with identifier `id` (see [`TransportKind::id`]).
    pub fn from_id(id: &str) -> Option<TransportKind> {
        Self::ALL.iter().copied().find(|kind| kind.id() == id)
    }

    /// Whether the transport binds to a UDP-family socket.
    pub fn over_udp(self) -> bool {
        self.socket().is_udp()
    }

    fn index(self) -> usize {
        self as usize
    }
}

impl TransportCapabilities for TransportKind {
    fn capabilities(&self) -> TransportCapabilitySet {
        match self {
            #[cfg(feature = "tcp")]
            TransportKind::Tcp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(feature = "udp")]
            TransportKind::Udp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "rtp")]
            TransportKind::Rtp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "rudp")]
            TransportKind::Rudp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "srt")]
            TransportKind::Srt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            TransportKind::UdpLite => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "mqtt")]
            TransportKind::Mqtt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(feature = "coap")]
            TransportKind::Coap => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "amqp")]
            TransportKind::Amqp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Dccp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Dccp,
                supports_ping: true,
            },
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Sctp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Sctp,
                supports_ping: true,
            },
            #[cfg(feature = "quic")]
            TransportKind::Quic => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(feature = "http1")]
            TransportKind::Http1 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(feature = "http3")]
            TransportKind::Http3 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            TransportKind::WebSocketTcp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "udp"))]
            TransportKind::WebSocketUdp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            TransportKind::WebSocketRtp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            TransportKind::WebSocketRudp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "srt"))]
            TransportKind::WebSocketSrt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            TransportKind::WebSocketMqtt => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "coap"))]
            TransportKind::WebSocketCoap => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            TransportKind::WebSocketAmqp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketUdpLite => TransportCapabilitySet {
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
            TransportKind::WebSocketDccp => TransportCapabilitySet {
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
            TransportKind::WebSocketSctp => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Sctp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "quic"))]
            TransportKind::WebSocketQuic => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "http1"))]
            TransportKind::WebSocketHttp1 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Serialized,
                security: TransportSecurity::Plain,
                socket: TransportSocket::Tcp,
                supports_ping: true,
            },
            #[cfg(all(feature = "websocket", feature = "http3"))]
            TransportKind::WebSocketHttp3 => TransportCapabilitySet {
                multiplexing: TransportMultiplexing::Multiplexed,
                security: TransportSecurity::Tls,
                socket: TransportSocket::Udp,
                supports_ping: true,
            },
            #[cfg(not(any(
                feature = "tcp",
                feature = "udp",
                feature = "rtp",
                feature = "rudp",
                feature = "srt",
                all(feature = "udplite", any(target_os = "linux", target_os = "android")),
                feature = "mqtt",
                feature = "coap",
                feature = "amqp",
                all(feature = "dccp", any(target_os = "linux", target_os = "android")),
                all(feature = "sctp", any(target_os = "linux", target_os = "android")),
                feature = "quic",
                feature = "http1",
                feature = "http3"
            )))]
            _ => unreachable!("no runtime transport feature is enabled"),
        }
    }
}

/// Certificate chain and private key of the QUIC-based transports.
#[cfg(any(feature = "quic", feature = "http3"))]
struct Identity {
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

/// One JSON-RPC protocol exposed through local listeners and external broker
/// services. Only local listeners are replicated over bind addresses.
pub struct MultiTransportServer {
    protocol: Arc<dyn JsonRpcProtocol>,
    bind: Vec<IpAddr>,
    ports: [Option<u16>; TRANSPORT_SLOT_COUNT],
    #[cfg(any(feature = "quic", feature = "http3"))]
    identity: Option<Identity>,
    limits: ServerLimits,
    shutdown: ServerShutdownPolicy,
    tcp_tuning: TcpTuningConfig,
    udp_reliability: UdpReliabilityConfig,
    udp_lite_config: UdpLiteConfig,
    srt_config: Option<SrtConfig>,
    #[cfg(feature = "mqtt")]
    mqtt_config: Option<MqttConfig>,
    #[cfg(feature = "amqp")]
    amqp_config: Option<AmqpConfig>,
    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    ws_mqtt_config: Option<MqttConfig>,
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    ws_amqp_config: Option<AmqpConfig>,
}

impl MultiTransportServer {
    pub fn builder(
        protocol: Arc<dyn JsonRpcProtocol>,
    ) -> MultiTransportServerBuilder<builder::MissingBind, builder::NoTransport> {
        MultiTransportServerBuilder::new(protocol)
    }

    pub fn new(protocol: Arc<dyn JsonRpcProtocol>) -> Self {
        Self {
            protocol,
            bind: Vec::new(),
            ports: [None; TRANSPORT_SLOT_COUNT],
            #[cfg(any(feature = "quic", feature = "http3"))]
            identity: None,
            limits: ServerLimits::default(),
            shutdown: ServerShutdownPolicy::default(),
            tcp_tuning: TcpTuningConfig::default(),
            udp_reliability: UdpReliabilityConfig::default(),
            udp_lite_config: UdpLiteConfig::default(),
            srt_config: None,
            #[cfg(feature = "mqtt")]
            mqtt_config: None,
            #[cfg(feature = "amqp")]
            amqp_config: None,
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            ws_mqtt_config: None,
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            ws_amqp_config: None,
        }
    }

    /// Local addresses to listen on. Required for local transports, ignored
    /// by external broker services, which connect once per configured kind.
    pub fn set_bind(&mut self, addresses: impl IntoIterator<Item = IpAddr>) -> &mut Self {
        self.bind = addresses.into_iter().collect();
        self
    }

    /// Local port of `kind`; port 0 lets the OS choose a free port.
    /// For MQTT/AMQP this instead selects an external broker on localhost.
    /// Broker port 0 is invalid; no broker or private wire server is created.
    pub fn set_port(&mut self, kind: TransportKind, port: u16) -> &mut Self {
        self.ports[kind.index()] = Some(port);
        let _address = SocketAddr::from(([127, 0, 0, 1], port));
        match kind {
            #[cfg(feature = "mqtt")]
            TransportKind::Mqtt => self.mqtt_config = Some(MqttConfig::for_address(_address)),
            #[cfg(feature = "amqp")]
            TransportKind::Amqp => self.amqp_config = Some(AmqpConfig::for_address(_address)),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            TransportKind::WebSocketMqtt => {
                let mut config = MqttConfig::for_address(_address);
                config.request_topic = "json-rpc-2/ws/requests".into();
                self.ws_mqtt_config = Some(config);
            }
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            TransportKind::WebSocketAmqp => {
                let mut config = AmqpConfig::for_address(_address);
                config.request_queue = "json-rpc-2.ws.requests".into();
                self.ws_amqp_config = Some(config);
            }
            _ => {}
        }
        self
    }

    /// Do not start `kind`.
    pub fn clear_port(&mut self, kind: TransportKind) -> &mut Self {
        self.ports[kind.index()] = None;
        match kind {
            #[cfg(feature = "mqtt")]
            TransportKind::Mqtt => self.mqtt_config = None,
            #[cfg(feature = "amqp")]
            TransportKind::Amqp => self.amqp_config = None,
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            TransportKind::WebSocketMqtt => self.ws_mqtt_config = None,
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            TransportKind::WebSocketAmqp => self.ws_amqp_config = None,
            _ => {}
        }
        self
    }

    /// Subscribe once to an external MQTT broker, regardless of bind addresses.
    /// The config carries the authenticated broker URL and service request topic.
    #[cfg(feature = "mqtt")]
    pub fn set_mqtt_config(&mut self, config: MqttConfig) -> &mut Self {
        self.ports[TransportKind::Mqtt.index()] = None;
        self.mqtt_config = Some(config);
        self
    }

    #[cfg(feature = "mqtt")]
    pub fn mqtt_config(&self) -> Option<&MqttConfig> {
        self.mqtt_config.as_ref()
    }

    /// Consume once from an external AMQP broker's service queue.
    /// Credentials and virtual host belong in the config's broker URL.
    #[cfg(feature = "amqp")]
    pub fn set_amqp_config(&mut self, config: AmqpConfig) -> &mut Self {
        self.ports[TransportKind::Amqp.index()] = None;
        self.amqp_config = Some(config);
        self
    }

    #[cfg(feature = "amqp")]
    pub fn amqp_config(&self) -> Option<&AmqpConfig> {
        self.amqp_config.as_ref()
    }

    /// MQTT service for WebSocket application frames; use a topic distinct
    /// from the raw JSON-RPC service when enabling both.
    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    pub fn set_ws_mqtt_config(&mut self, config: MqttConfig) -> &mut Self {
        self.ports[TransportKind::WebSocketMqtt.index()] = None;
        self.ws_mqtt_config = Some(config);
        self
    }

    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    pub fn ws_mqtt_config(&self) -> Option<&MqttConfig> {
        self.ws_mqtt_config.as_ref()
    }

    /// AMQP service for WebSocket application frames; use a queue distinct
    /// from the raw JSON-RPC service when enabling both.
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    pub fn set_ws_amqp_config(&mut self, config: AmqpConfig) -> &mut Self {
        self.ports[TransportKind::WebSocketAmqp.index()] = None;
        self.ws_amqp_config = Some(config);
        self
    }

    #[cfg(all(feature = "websocket", feature = "amqp"))]
    pub fn ws_amqp_config(&self) -> Option<&AmqpConfig> {
        self.ws_amqp_config.as_ref()
    }

    /// TCP port of the TCP transport (newline-delimited JSON).
    #[cfg(feature = "tcp")]
    pub fn set_tcp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Tcp, port)
    }

    /// UDP port of the reliable datagram transport.
    #[cfg(feature = "udp")]
    pub fn set_udp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Udp, port)
    }

    /// UDP port of the RTP/RTCP datagram transport.
    #[cfg(feature = "rtp")]
    pub fn set_rtp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Rtp, port)
    }

    /// UDP port of the reliable RUDP datagram transport.
    #[cfg(feature = "rudp")]
    pub fn set_rudp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Rudp, port)
    }

    /// UDP port owned by an encrypted standard libsrt listener.
    #[cfg(feature = "srt")]
    pub fn set_srt_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Srt, port)
    }

    /// UDP-Lite port of the partially checksummed datagram transport.
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    pub fn set_udplite_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::UdpLite, port)
    }

    /// External MQTT 3.1.1 broker port on localhost, not a local listener.
    /// Use `set_mqtt_config` for credentials and a service-specific topic.
    #[cfg(feature = "mqtt")]
    pub fn set_mqtt_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Mqtt, port)
    }

    /// UDP port of the CoAP-style constrained application transport.
    #[cfg(feature = "coap")]
    pub fn set_coap_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Coap, port)
    }

    /// External AMQP 0-9-1 broker port on localhost, not a local listener.
    /// Use `set_amqp_config` for credentials, virtual host and service queue.
    #[cfg(feature = "amqp")]
    pub fn set_amqp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Amqp, port)
    }

    /// DCCP port of the datagram transport with congestion control.
    #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
    pub fn set_dccp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Dccp, port)
    }

    /// SCTP port of the message-oriented multi-stream transport.
    #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
    pub fn set_sctp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Sctp, port)
    }

    /// UDP port of the raw QUIC transport.
    #[cfg(feature = "quic")]
    pub fn set_quic_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Quic, port)
    }

    /// TCP port of the HTTP/1.1 transport.
    #[cfg(feature = "http1")]
    pub fn set_http1_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Http1, port)
    }

    /// UDP port of the HTTP/3 transport.
    #[cfg(feature = "http3")]
    pub fn set_http3_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::Http3, port)
    }

    /// TCP port of the WebSocket over HTTP/1.1 transport.
    #[cfg(all(feature = "websocket", feature = "http1"))]
    pub fn set_ws_http1_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketHttp1, port)
    }

    /// TCP port of the dedicated WebSocket over TCP transport.
    #[cfg(all(feature = "websocket", feature = "tcp"))]
    pub fn set_ws_tcp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketTcp, port)
    }

    /// UDP port of the dedicated WebSocket over reliable UDP transport.
    #[cfg(all(feature = "websocket", feature = "udp"))]
    pub fn set_ws_udp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketUdp, port)
    }

    /// UDP port of the dedicated WebSocket over RTP/RTCP transport.
    #[cfg(all(feature = "websocket", feature = "rtp"))]
    pub fn set_ws_rtp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketRtp, port)
    }

    /// UDP port of the dedicated WebSocket over RUDP transport.
    #[cfg(all(feature = "websocket", feature = "rudp"))]
    pub fn set_ws_rudp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketRudp, port)
    }

    /// UDP port owned by libsrt for WebSocket application frames.
    #[cfg(all(feature = "websocket", feature = "srt"))]
    pub fn set_ws_srt_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketSrt, port)
    }

    /// External localhost MQTT broker for WebSocket application frames.
    /// Uses the `json-rpc-2/ws/requests` topic.
    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    pub fn set_ws_mqtt_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketMqtt, port)
    }

    /// UDP port of the dedicated WebSocket over CoAP-style transport.
    #[cfg(all(feature = "websocket", feature = "coap"))]
    pub fn set_ws_coap_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketCoap, port)
    }

    /// External localhost AMQP broker for WebSocket application frames.
    /// Uses the `json-rpc-2.ws.requests` queue.
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    pub fn set_ws_amqp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketAmqp, port)
    }

    /// UDP-Lite port of the dedicated WebSocket over UDP-Lite transport.
    #[cfg(all(
        feature = "websocket",
        feature = "udplite",
        any(target_os = "linux", target_os = "android")
    ))]
    pub fn set_ws_udplite_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketUdpLite, port)
    }

    /// DCCP port of the dedicated WebSocket over DCCP transport.
    #[cfg(all(
        feature = "websocket",
        feature = "dccp",
        any(target_os = "linux", target_os = "android")
    ))]
    pub fn set_ws_dccp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketDccp, port)
    }

    /// SCTP port of the dedicated WebSocket over SCTP transport.
    #[cfg(all(
        feature = "websocket",
        feature = "sctp",
        any(target_os = "linux", target_os = "android")
    ))]
    pub fn set_ws_sctp_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketSctp, port)
    }

    /// UDP port of the dedicated WebSocket over QUIC transport.
    #[cfg(all(feature = "websocket", feature = "quic"))]
    pub fn set_ws_quic_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketQuic, port)
    }

    /// UDP port of the WebSocket over HTTP/3 transport.
    #[cfg(all(feature = "websocket", feature = "http3"))]
    pub fn set_ws_http3_port(&mut self, port: u16) -> &mut Self {
        self.set_port(TransportKind::WebSocketHttp3, port)
    }

    /// TLS certificate chain and private key of QUIC, HTTP/3 and WebSocket
    /// over HTTP/3. Required when one of them has a port.
    pub fn set_tls(
        &mut self,
        certificates: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> &mut Self {
        #[cfg(any(feature = "quic", feature = "http3"))]
        {
            self.identity = Some(Identity { certificates, key });
        }
        #[cfg(not(any(feature = "quic", feature = "http3")))]
        {
            let _ = (certificates, key);
        }
        self
    }

    pub fn set_server_limits(&mut self, limits: ServerLimits) -> &mut Self {
        self.limits = limits;
        self
    }

    pub fn set_transport_limits(&mut self, limits: ServerLimits) -> &mut Self {
        self.set_server_limits(limits)
    }

    pub fn set_shutdown_policy(&mut self, shutdown: ServerShutdownPolicy) -> &mut Self {
        self.shutdown = shutdown;
        self
    }

    pub fn set_tcp_tuning(&mut self, tuning: TcpTuningConfig) -> &mut Self {
        self.tcp_tuning = tuning;
        self
    }

    pub fn set_udp_reliability(&mut self, config: UdpReliabilityConfig) -> &mut Self {
        self.udp_reliability = config;
        self
    }

    pub fn set_udplite_config(&mut self, config: UdpLiteConfig) -> &mut Self {
        self.udp_lite_config = config;
        self
    }

    pub fn set_srt_config(&mut self, config: SrtConfig) -> &mut Self {
        self.srt_config = Some(config);
        self
    }

    pub fn bind(&self) -> &[IpAddr] {
        &self.bind
    }

    pub fn server_limits(&self) -> &ServerLimits {
        &self.limits
    }

    pub fn transport_limits(&self) -> &ServerLimits {
        self.server_limits()
    }

    pub fn shutdown_policy(&self) -> &ServerShutdownPolicy {
        &self.shutdown
    }

    pub fn tcp_tuning(&self) -> &TcpTuningConfig {
        &self.tcp_tuning
    }

    pub fn udp_reliability(&self) -> &UdpReliabilityConfig {
        &self.udp_reliability
    }

    pub fn udplite_config(&self) -> &UdpLiteConfig {
        &self.udp_lite_config
    }

    pub fn srt_config(&self) -> Option<&SrtConfig> {
        self.srt_config.as_ref()
    }

    /// Configured local port, or the localhost external broker convenience
    /// port. Explicit broker configs enable a service without setting a port.
    pub fn port(&self, kind: TransportKind) -> Option<u16> {
        self.ports[kind.index()]
    }

    /// Enabled local listeners and external broker services, in slot order.
    pub fn transports(&self) -> impl Iterator<Item = TransportKind> + '_ {
        TransportKind::ALL
            .iter()
            .copied()
            .filter(|kind| match kind {
                #[cfg(feature = "mqtt")]
                TransportKind::Mqtt => self.mqtt_config.is_some(),
                #[cfg(feature = "amqp")]
                TransportKind::Amqp => self.amqp_config.is_some(),
                #[cfg(all(feature = "websocket", feature = "mqtt"))]
                TransportKind::WebSocketMqtt => self.ws_mqtt_config.is_some(),
                #[cfg(all(feature = "websocket", feature = "amqp"))]
                TransportKind::WebSocketAmqp => self.ws_amqp_config.is_some(),
                _ => self.port(*kind).is_some(),
            })
    }

    /// Check the configuration: bind addresses, at least one transport, TLS
    /// for the QUIC-based ones, and no two transports of the same protocol
    /// (same socket family) on the same non-zero port.
    pub fn validate(&self) -> io::Result<()> {
        let invalid = |message: String| Err(io::Error::new(io::ErrorKind::InvalidInput, message));
        let transports: Vec<TransportKind> = self.transports().collect();
        if self.bind.is_empty() && transports.iter().any(|kind| !kind.is_broker()) {
            return invalid("no bind address".into());
        }
        if transports.is_empty() {
            return invalid("no transport configured".into());
        }
        #[cfg(all(feature = "mqtt", feature = "websocket"))]
        if let (Some(raw), Some(ws)) = (&self.mqtt_config, &self.ws_mqtt_config)
            && raw.broker_url == ws.broker_url
            && raw.request_topic == ws.request_topic
        {
            return invalid(
                "raw and WebSocket MQTT services require distinct request topics".into(),
            );
        }
        #[cfg(all(feature = "amqp", feature = "websocket"))]
        if let (Some(raw), Some(ws)) = (&self.amqp_config, &self.ws_amqp_config)
            && raw.broker_url == ws.broker_url
            && raw.request_queue == ws.request_queue
        {
            return invalid(
                "raw and WebSocket AMQP services require distinct request queues".into(),
            );
        }
        #[cfg(any(feature = "quic", feature = "http3"))]
        if self.identity.is_none()
            && let Some(kind) = transports.iter().find(|kind| kind.security().is_tls())
        {
            return invalid(format!("{} needs a TLS certificate and key", kind.name()));
        }
        #[cfg(all(feature = "srt", feature = "websocket"))]
        if self.srt_config.is_none()
            && let Some(kind) = transports
                .iter()
                .find(|kind| matches!(kind, TransportKind::Srt | TransportKind::WebSocketSrt))
        {
            return invalid(format!(
                "{} needs an SRT passphrase/configuration",
                kind.name()
            ));
        }
        #[cfg(all(feature = "srt", not(feature = "websocket")))]
        if self.srt_config.is_none()
            && let Some(kind) = transports
                .iter()
                .find(|kind| matches!(kind, TransportKind::Srt))
        {
            return invalid(format!(
                "{} needs an SRT passphrase/configuration",
                kind.name()
            ));
        }
        if let Some(config) = &self.srt_config {
            config
                .validate()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        self.udp_lite_config
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        for (i, first) in transports.iter().enumerate() {
            if first.is_broker() {
                if self.port(*first) == Some(0) {
                    return invalid(format!(
                        "{} requires a nonzero external broker port",
                        first.name()
                    ));
                }
                continue;
            }
            for second in &transports[i + 1..] {
                if second.is_broker() {
                    continue;
                }
                let port = self.port(*first);
                if first.socket() == second.socket()
                    && port != Some(0)
                    && port == self.port(*second)
                {
                    return invalid(format!(
                        "{} and {} use the same {} port {}",
                        first.name(),
                        second.name(),
                        if first.id().contains("dccp") {
                            "DCCP"
                        } else if first.id().contains("sctp") {
                            "SCTP"
                        } else if first.socket().is_udp() {
                            "UDP"
                        } else {
                            "TCP"
                        },
                        port.unwrap_or_default()
                    ));
                }
            }
        }
        Ok(())
    }

    /// Start local listeners on every bind address and broker consumers once
    /// per configured kind. If startup fails, already started services close.
    pub async fn start(&self) -> io::Result<RunningServer> {
        self.validate()?;
        let runtime = Arc::new(TransportRuntime::new(
            self.limits.clone(),
            self.shutdown.clone(),
            self.tcp_tuning.clone(),
            self.udp_reliability.clone(),
        ));
        #[cfg(feature = "websocket")]
        let websocket: Arc<dyn WebSocketProtocol> =
            Arc::new(JsonRpcWebSocket::new(self.protocol.clone()));
        let mut running = RunningServer {
            listeners: Vec::new(),
            brokers: Vec::new(),
            runtime: Arc::clone(&runtime),
        };
        #[cfg(any(feature = "mqtt", feature = "amqp"))]
        for kind in self.transports().filter(|kind| kind.is_broker()) {
            let transport = match self
                .connect_broker(
                    kind,
                    #[cfg(feature = "websocket")]
                    &websocket,
                    &runtime,
                )
                .await
            {
                Ok(transport) => transport,
                Err(error) => {
                    if let Err(close_error) = running.close().await {
                        tracing::error!(error = %close_error, "failed to close partially started server");
                    }
                    return Err(io::Error::new(
                        error.kind(),
                        format!("{} broker startup failed: {error}", kind.name()),
                    ));
                }
            };
            running.brokers.push((kind, transport));
        }
        for &ip in &self.bind {
            for kind in self.transports().filter(|kind| !kind.is_broker()) {
                let address = SocketAddr::new(ip, self.port(kind).unwrap_or_default());
                match self
                    .listen(
                        kind,
                        address,
                        #[cfg(feature = "websocket")]
                        &websocket,
                        &runtime,
                    )
                    .await
                {
                    Ok(listener) => running.listeners.push(listener),
                    Err(error) => {
                        if let Err(close_error) = running.close().await {
                            tracing::error!(error = %close_error, "failed to close partially started server");
                        }
                        return Err(io::Error::new(
                            error.kind(),
                            format!("{} on {address}: {error}", kind.name()),
                        ));
                    }
                }
            }
        }
        Ok(running)
    }

    #[cfg(any(feature = "mqtt", feature = "amqp"))]
    async fn connect_broker(
        &self,
        kind: TransportKind,
        #[cfg(feature = "websocket")] websocket: &Arc<dyn WebSocketProtocol>,
        runtime: &Arc<TransportRuntime>,
    ) -> io::Result<RunningTransport> {
        let missing =
            || io::Error::new(io::ErrorKind::InvalidInput, "missing broker configuration");
        match kind {
            #[cfg(feature = "mqtt")]
            TransportKind::Mqtt => Ok(MqttTransport::connect(
                self.mqtt_config.clone().ok_or_else(missing)?,
            )
            .await?
            .with_runtime(Arc::clone(runtime))
            .start(self.protocol.clone())),
            #[cfg(feature = "amqp")]
            TransportKind::Amqp => Ok(AmqpTransport::connect(
                self.amqp_config.clone().ok_or_else(missing)?,
            )
            .await?
            .with_runtime(Arc::clone(runtime))
            .start(self.protocol.clone())),
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            TransportKind::WebSocketMqtt => Ok(MqttWebSocketTransport::connect(
                self.ws_mqtt_config.clone().ok_or_else(missing)?,
            )
            .await?
            .with_runtime(Arc::clone(runtime))
            .start(websocket.clone())),
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            TransportKind::WebSocketAmqp => Ok(AmqpWebSocketTransport::connect(
                self.ws_amqp_config.clone().ok_or_else(missing)?,
            )
            .await?
            .with_runtime(Arc::clone(runtime))
            .start(websocket.clone())),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a broker transport",
            )),
        }
    }

    async fn listen(
        &self,
        kind: TransportKind,
        address: SocketAddr,
        #[cfg(feature = "websocket")] websocket: &Arc<dyn WebSocketProtocol>,
        runtime: &Arc<TransportRuntime>,
    ) -> io::Result<Listener> {
        let protocol = self.protocol.clone();
        let (address, transport) = match kind {
            #[cfg(feature = "tcp")]
            TransportKind::Tcp => {
                let listener = bind_tcp(address, &runtime.tcp_tuning)?;
                (
                    listener.local_addr()?,
                    TcpTransport::new(listener)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(feature = "http1")]
            TransportKind::Http1 => {
                let listener = bind_tcp(address, &runtime.tcp_tuning)?;
                (
                    listener.local_addr()?,
                    Http1Transport::new(listener)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(feature = "mqtt")]
            TransportKind::Mqtt => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "MQTT uses an external broker, not a listener",
                ));
            }
            #[cfg(feature = "amqp")]
            TransportKind::Amqp => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AMQP uses an external broker, not a listener",
                ));
            }
            #[cfg(feature = "coap")]
            TransportKind::Coap => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    CoapTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(all(feature = "websocket", feature = "tcp"))]
            TransportKind::WebSocketTcp => {
                let listener = bind_tcp(address, &runtime.tcp_tuning)?;
                (
                    listener.local_addr()?,
                    TcpWebSocketTransport::new(listener)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(feature = "websocket", feature = "mqtt"))]
            TransportKind::WebSocketMqtt => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WebSocket MQTT uses an external broker, not a listener",
                ));
            }
            #[cfg(all(feature = "websocket", feature = "amqp"))]
            TransportKind::WebSocketAmqp => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WebSocket AMQP uses an external broker, not a listener",
                ));
            }
            #[cfg(all(feature = "websocket", feature = "coap"))]
            TransportKind::WebSocketCoap => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    CoapWebSocketTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(feature = "websocket", feature = "rudp"))]
            TransportKind::WebSocketRudp => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    RudpWebSocketTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(feature = "websocket", feature = "srt"))]
            TransportKind::WebSocketSrt => {
                let transport = SrtWebSocketTransport::bind(
                    address,
                    self.srt_config.clone().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "missing SRT configuration")
                    })?,
                )
                .await?;
                let local_addr = transport.local_addr()?;
                (
                    local_addr,
                    transport
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(
                feature = "websocket",
                feature = "udplite",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketUdpLite => {
                let socket = bind_udplite(address, &self.udp_lite_config)?;
                (
                    socket.local_addr()?,
                    UdpLiteWebSocketTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(feature = "websocket", feature = "udp"))]
            TransportKind::WebSocketUdp => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    UdpWebSocketTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(feature = "websocket", feature = "rtp"))]
            TransportKind::WebSocketRtp => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    RtpWebSocketTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Dccp => {
                let transport = DccpTransport::bind(address)?.with_runtime(Arc::clone(runtime));
                let local_addr = transport.local_addr();
                (local_addr, transport.start(protocol))
            }
            #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
            TransportKind::Sctp => {
                let transport = SctpTransport::bind(address)?.with_runtime(Arc::clone(runtime));
                let local_addr = transport.local_addr();
                (local_addr, transport.start(protocol))
            }
            #[cfg(all(
                feature = "websocket",
                feature = "dccp",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketDccp => {
                let transport =
                    DccpWebSocketTransport::bind(address)?.with_runtime(Arc::clone(runtime));
                let local_addr = transport.local_addr();
                (local_addr, transport.start(websocket.clone()))
            }
            #[cfg(all(
                feature = "websocket",
                feature = "sctp",
                any(target_os = "linux", target_os = "android")
            ))]
            TransportKind::WebSocketSctp => {
                let transport =
                    SctpWebSocketTransport::bind(address)?.with_runtime(Arc::clone(runtime));
                let local_addr = transport.local_addr();
                (local_addr, transport.start(websocket.clone()))
            }
            #[cfg(all(feature = "websocket", feature = "http1"))]
            TransportKind::WebSocketHttp1 => {
                let listener = bind_tcp(address, &runtime.tcp_tuning)?;
                (
                    listener.local_addr()?,
                    Http1WebSocketTransport::new(listener)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(feature = "udp")]
            TransportKind::Udp => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    UdpTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(feature = "rtp")]
            TransportKind::Rtp => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    RtpTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(feature = "rudp")]
            TransportKind::Rudp => {
                let socket = bind_udp(address)?;
                (
                    socket.local_addr()?,
                    RudpTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(feature = "srt")]
            TransportKind::Srt => {
                let transport = SrtTransport::bind(
                    address,
                    self.srt_config.clone().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "missing SRT configuration")
                    })?,
                )
                .await?;
                let local_addr = transport.local_addr()?;
                (
                    local_addr,
                    transport.with_runtime(Arc::clone(runtime)).start(protocol),
                )
            }
            #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
            TransportKind::UdpLite => {
                let socket = bind_udplite(address, &self.udp_lite_config)?;
                (
                    socket.local_addr()?,
                    UdpLiteTransport::new(tokio::net::UdpSocket::from_std(socket)?)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(feature = "quic")]
            TransportKind::Quic => {
                let endpoint = self.quic_endpoint(address, QUIC_ALPN)?;
                (
                    endpoint.local_addr()?,
                    QuicTransport::new(endpoint)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(feature = "http3")]
            TransportKind::Http3 => {
                let endpoint = self.quic_endpoint(address, H3_ALPN)?;
                (
                    endpoint.local_addr()?,
                    Http3Transport::new(endpoint)
                        .with_runtime(Arc::clone(runtime))
                        .start(protocol),
                )
            }
            #[cfg(all(feature = "websocket", feature = "quic"))]
            TransportKind::WebSocketQuic => {
                let endpoint = self.quic_endpoint(address, WS_QUIC_ALPN)?;
                (
                    endpoint.local_addr()?,
                    QuicWebSocketTransport::new(endpoint)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
            #[cfg(all(feature = "websocket", feature = "http3"))]
            TransportKind::WebSocketHttp3 => {
                let endpoint = self.quic_endpoint(address, H3_ALPN)?;
                (
                    endpoint.local_addr()?,
                    Http3WebSocketTransport::new(endpoint)
                        .with_runtime(Arc::clone(runtime))
                        .start(websocket.clone()),
                )
            }
        };
        Ok(Listener {
            kind,
            address,
            transport,
        })
    }

    /// QUIC server endpoint (TLS 1.3) bound to `address`.
    #[cfg(any(feature = "quic", feature = "http3"))]
    fn quic_endpoint(&self, address: SocketAddr, alpn: &[u8]) -> io::Result<quinn::Endpoint> {
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no TLS certificate"))?;
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(identity.certificates.clone(), identity.key.clone_key())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        tls.alpn_protocols = vec![alpn.to_vec()];
        let crypto = QuicServerConfig::try_from(tls).map_err(io::Error::other)?;
        let config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        let runtime = quinn::default_runtime()
            .ok_or_else(|| io::Error::other("no async runtime for QUIC"))?;
        quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(config),
            bind_udp(address)?,
            runtime,
        )
    }
}

/// A transport of a running server.
struct Listener {
    kind: TransportKind,
    address: SocketAddr,
    transport: RunningTransport,
}

/// Handle of a started [`MultiTransportServer`].
pub struct RunningServer {
    listeners: Vec<Listener>,
    brokers: Vec<(TransportKind, RunningTransport)>,
    runtime: Arc<TransportRuntime>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HealthSnapshot {
    pub runtime: RuntimeMetricsSnapshot,
    pub server: super::ServerMetricsSnapshot,
}

#[derive(Clone)]
pub struct HealthHandle {
    server: Arc<RpcServer>,
    runtime: Arc<TransportRuntime>,
}

impl RunningServer {
    /// External broker subscriptions/consumers. These do not own local
    /// listening sockets and are intentionally absent from `listeners()`.
    pub fn broker_transports(&self) -> impl Iterator<Item = TransportKind> + '_ {
        self.brokers.iter().map(|(kind, _)| *kind)
    }

    /// Every running transport with its actual local address.
    pub fn listeners(&self) -> impl Iterator<Item = (TransportKind, SocketAddr)> + '_ {
        self.listeners
            .iter()
            .map(|listener| (listener.kind, listener.address))
    }

    /// Local address of `kind` on the bind address `ip`.
    pub fn address(&self, kind: TransportKind, ip: IpAddr) -> Option<SocketAddr> {
        self.listeners()
            .find(|(running, address)| *running == kind && address.ip() == ip)
            .map(|(_, address)| address)
    }

    pub fn runtime_metrics(&self) -> RuntimeMetricsSnapshot {
        self.runtime.metrics.snapshot()
    }

    pub fn health_handle(&self, server: Arc<RpcServer>) -> HealthHandle {
        HealthHandle {
            server,
            runtime: Arc::clone(&self.runtime),
        }
    }

    /// Gracefully close every transport (see [`RunningTransport::close`]) and
    /// wait until all have stopped; the first error is returned.
    pub async fn close(self) -> io::Result<()> {
        let mut result = Ok(());
        for listener in self.listeners {
            if let Err(error) = listener.transport.close().await
                && result.is_ok()
            {
                result = Err(error);
            }
        }
        for (_, transport) in self.brokers {
            if let Err(error) = transport.close().await
                && result.is_ok()
            {
                result = Err(error);
            }
        }
        result
    }
}

impl HealthHandle {
    pub fn snapshot(&self) -> HealthSnapshot {
        HealthSnapshot {
            runtime: self.runtime.metrics.snapshot(),
            server: self.server.metrics(),
        }
    }

    pub fn json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.snapshot())
    }

    pub fn prometheus(&self) -> String {
        let snapshot = self.snapshot();
        format!(
            concat!(
                "# TYPE json_rpc_2_server_active_requests gauge\n",
                "json_rpc_2_server_active_requests {}\n",
                "# TYPE json_rpc_2_server_rpc_calls_total counter\n",
                "json_rpc_2_server_rpc_calls_total {}\n",
                "# TYPE json_rpc_2_server_rpc_errors_total counter\n",
                "json_rpc_2_server_rpc_errors_total {}\n",
                "# TYPE json_rpc_2_runtime_active_sessions gauge\n",
                "json_rpc_2_runtime_active_sessions {}\n",
                "# TYPE json_rpc_2_runtime_active_streams gauge\n",
                "json_rpc_2_runtime_active_streams {}\n",
                "# TYPE json_rpc_2_runtime_reconnect_attempts counter\n",
                "json_rpc_2_runtime_reconnect_attempts {}\n",
            ),
            snapshot.server.active_requests,
            snapshot.server.rpc_calls_total,
            snapshot.server.rpc_errors_total,
            snapshot.runtime.active_sessions,
            snapshot.runtime.active_streams,
            snapshot.runtime.reconnect_attempts,
        )
    }

    #[cfg(feature = "http1")]
    pub fn router(&self) -> Router {
        let handle = self.clone();
        Router::new()
            .route("/healthz", get(healthz))
            .route("/metrics.json", get(metrics_json))
            .route("/metrics", get(metrics_prometheus))
            .with_state(handle)
    }
}

#[cfg(feature = "http1")]
async fn healthz(
    axum::extract::State(handle): axum::extract::State<HealthHandle>,
) -> Json<HealthSnapshot> {
    Json(handle.snapshot())
}

#[cfg(feature = "http1")]
async fn metrics_json(
    axum::extract::State(handle): axum::extract::State<HealthHandle>,
) -> Json<HealthSnapshot> {
    Json(handle.snapshot())
}

#[cfg(feature = "http1")]
async fn metrics_prometheus(
    axum::extract::State(handle): axum::extract::State<HealthHandle>,
) -> impl axum::response::IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        handle.prometheus(),
    )
}

/// A socket bound to `address`. An IPv6 socket accepts IPv6 only
/// (`IPV6_V6ONLY`), so `0.0.0.0` and `::` can be bound side by side on every OS.
#[cfg(any(
    feature = "tcp",
    feature = "http1",
    feature = "udp",
    feature = "rtp",
    feature = "rudp",
    feature = "quic",
    feature = "http3",
    feature = "websocket",
    feature = "coap"
))]
fn bind_socket(
    address: SocketAddr,
    kind: socket2::Type,
    _tcp_tuning: &TcpTuningConfig,
) -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(socket2::Domain::for_address(address), kind, None)?;
    if address.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    #[cfg(unix)]
    if kind == socket2::Type::STREAM {
        // Like `TcpListener::bind`: restart without waiting for TIME_WAIT.
        socket.set_reuse_address(true)?;
    }
    #[cfg(any(
        feature = "tcp",
        feature = "http1",
        all(feature = "websocket", feature = "tcp"),
        all(feature = "websocket", feature = "http1")
    ))]
    if kind == socket2::Type::STREAM {
        super::apply_tcp_listener_tuning(&socket, _tcp_tuning)?;
    }
    socket.bind(&address.into())?;
    Ok(socket)
}

#[cfg(any(
    feature = "tcp",
    feature = "http1",
    all(feature = "websocket", feature = "tcp"),
    all(feature = "websocket", feature = "http1")
))]
fn bind_tcp(address: SocketAddr, tcp_tuning: &TcpTuningConfig) -> io::Result<TcpListener> {
    let socket = bind_socket(address, socket2::Type::STREAM, tcp_tuning)?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    TcpListener::from_std(socket.into())
}

#[cfg(any(
    feature = "udp",
    feature = "rtp",
    feature = "rudp",
    feature = "quic",
    feature = "http3",
    feature = "websocket",
    feature = "coap"
))]
fn bind_udp(address: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let socket = bind_socket(address, socket2::Type::DGRAM, &TcpTuningConfig::default())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
fn bind_udplite(address: SocketAddr, config: &UdpLiteConfig) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::from(libc::IPPROTO_UDPLITE)),
    )?;
    if address.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    json_rpc_2_transport::apply_udp_lite_coverage(&socket, config)?;
    socket.bind(&address.into())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

mod builder {
    use super::{
        CertificateDer, IpAddr, JsonRpcProtocol, MultiTransportServer, PrivateKeyDer,
        RunningServer, ServerLimits, ServerShutdownPolicy, SrtConfig, TransportKind, UdpLiteConfig,
    };
    use std::{io, marker::PhantomData, sync::Arc};

    pub struct MissingBind;
    pub struct HasBind;
    pub struct NoTransport;
    pub struct HasTransport;

    pub struct Builder<BindState, TransportState> {
        inner: MultiTransportServer,
        bind_state: PhantomData<BindState>,
        transport_state: PhantomData<TransportState>,
    }

    impl Builder<MissingBind, NoTransport> {
        pub fn new(protocol: Arc<dyn JsonRpcProtocol>) -> Self {
            Self {
                inner: MultiTransportServer::new(protocol),
                bind_state: PhantomData,
                transport_state: PhantomData,
            }
        }
    }

    impl<BindState, TransportState> Builder<BindState, TransportState> {
        pub fn set_bind(
            mut self,
            addresses: impl IntoIterator<Item = IpAddr>,
        ) -> Builder<HasBind, TransportState> {
            self.inner.set_bind(addresses);
            Builder {
                inner: self.inner,
                bind_state: PhantomData,
                transport_state: PhantomData,
            }
        }
    }

    impl<BindState, TransportState> Builder<BindState, TransportState> {
        /// Configure a broker-only service without requiring local bind IPs.
        #[cfg(feature = "mqtt")]
        pub fn set_mqtt_config(
            mut self,
            config: super::MqttConfig,
        ) -> Builder<HasBind, HasTransport> {
            self.inner.set_mqtt_config(config);
            Builder {
                inner: self.inner,
                bind_state: PhantomData,
                transport_state: PhantomData,
            }
        }

        #[cfg(feature = "amqp")]
        pub fn set_amqp_config(
            mut self,
            config: super::AmqpConfig,
        ) -> Builder<HasBind, HasTransport> {
            self.inner.set_amqp_config(config);
            Builder {
                inner: self.inner,
                bind_state: PhantomData,
                transport_state: PhantomData,
            }
        }

        #[cfg(all(feature = "websocket", feature = "mqtt"))]
        pub fn set_ws_mqtt_config(
            mut self,
            config: super::MqttConfig,
        ) -> Builder<HasBind, HasTransport> {
            self.inner.set_ws_mqtt_config(config);
            Builder {
                inner: self.inner,
                bind_state: PhantomData,
                transport_state: PhantomData,
            }
        }

        #[cfg(all(feature = "websocket", feature = "amqp"))]
        pub fn set_ws_amqp_config(
            mut self,
            config: super::AmqpConfig,
        ) -> Builder<HasBind, HasTransport> {
            self.inner.set_ws_amqp_config(config);
            Builder {
                inner: self.inner,
                bind_state: PhantomData,
                transport_state: PhantomData,
            }
        }

        pub fn set_tls(
            mut self,
            certificates: Vec<CertificateDer<'static>>,
            key: PrivateKeyDer<'static>,
        ) -> Self {
            self.inner.set_tls(certificates, key);
            self
        }

        pub fn set_server_limits(mut self, limits: ServerLimits) -> Self {
            self.inner.set_server_limits(limits);
            self
        }

        pub fn set_transport_limits(self, limits: ServerLimits) -> Self {
            self.set_server_limits(limits)
        }

        pub fn set_shutdown_policy(mut self, shutdown: ServerShutdownPolicy) -> Self {
            self.inner.set_shutdown_policy(shutdown);
            self
        }

        pub fn set_srt_config(mut self, config: SrtConfig) -> Self {
            self.inner.set_srt_config(config);
            self
        }

        pub fn set_udplite_config(mut self, config: UdpLiteConfig) -> Self {
            self.inner.set_udplite_config(config);
            self
        }

        fn with_transport(mut self, kind: TransportKind, port: u16) -> Self {
            self.inner.set_port(kind, port);
            self
        }
    }

    impl<BindState> Builder<BindState, NoTransport> {
        pub fn set_port(self, kind: TransportKind, port: u16) -> Builder<BindState, HasTransport> {
            Builder {
                inner: self.with_transport(kind, port).inner,
                bind_state: PhantomData,
                transport_state: PhantomData,
            }
        }

        #[cfg(feature = "tcp")]
        pub fn set_tcp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Tcp, port)
        }

        #[cfg(feature = "udp")]
        pub fn set_udp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Udp, port)
        }

        #[cfg(feature = "rtp")]
        pub fn set_rtp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Rtp, port)
        }

        #[cfg(feature = "rudp")]
        pub fn set_rudp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Rudp, port)
        }

        #[cfg(feature = "srt")]
        pub fn set_srt_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Srt, port)
        }

        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        pub fn set_udplite_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::UdpLite, port)
        }

        #[cfg(feature = "mqtt")]
        pub fn set_mqtt_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Mqtt, port)
        }

        #[cfg(feature = "coap")]
        pub fn set_coap_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Coap, port)
        }

        #[cfg(feature = "amqp")]
        pub fn set_amqp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Amqp, port)
        }

        #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
        pub fn set_dccp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Dccp, port)
        }

        #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
        pub fn set_sctp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Sctp, port)
        }

        #[cfg(feature = "quic")]
        pub fn set_quic_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Quic, port)
        }

        #[cfg(feature = "http1")]
        pub fn set_http1_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Http1, port)
        }

        #[cfg(feature = "http3")]
        pub fn set_http3_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::Http3, port)
        }

        #[cfg(all(feature = "websocket", feature = "tcp"))]
        pub fn set_ws_tcp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketTcp, port)
        }

        #[cfg(all(feature = "websocket", feature = "udp"))]
        pub fn set_ws_udp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketUdp, port)
        }

        #[cfg(all(feature = "websocket", feature = "rtp"))]
        pub fn set_ws_rtp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketRtp, port)
        }

        #[cfg(all(feature = "websocket", feature = "rudp"))]
        pub fn set_ws_rudp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketRudp, port)
        }

        #[cfg(all(feature = "websocket", feature = "srt"))]
        pub fn set_ws_srt_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketSrt, port)
        }

        #[cfg(all(feature = "websocket", feature = "mqtt"))]
        pub fn set_ws_mqtt_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketMqtt, port)
        }

        #[cfg(all(feature = "websocket", feature = "coap"))]
        pub fn set_ws_coap_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketCoap, port)
        }

        #[cfg(all(feature = "websocket", feature = "amqp"))]
        pub fn set_ws_amqp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketAmqp, port)
        }

        #[cfg(all(
            feature = "websocket",
            feature = "udplite",
            any(target_os = "linux", target_os = "android")
        ))]
        pub fn set_ws_udplite_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketUdpLite, port)
        }

        #[cfg(all(
            feature = "websocket",
            feature = "dccp",
            any(target_os = "linux", target_os = "android")
        ))]
        pub fn set_ws_dccp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketDccp, port)
        }

        #[cfg(all(
            feature = "websocket",
            feature = "sctp",
            any(target_os = "linux", target_os = "android")
        ))]
        pub fn set_ws_sctp_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketSctp, port)
        }

        #[cfg(all(feature = "websocket", feature = "quic"))]
        pub fn set_ws_quic_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketQuic, port)
        }

        #[cfg(all(feature = "websocket", feature = "http1"))]
        pub fn set_ws_http1_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketHttp1, port)
        }

        #[cfg(all(feature = "websocket", feature = "http3"))]
        pub fn set_ws_http3_port(self, port: u16) -> Builder<BindState, HasTransport> {
            self.set_port(TransportKind::WebSocketHttp3, port)
        }
    }

    impl<BindState> Builder<BindState, HasTransport> {
        pub fn set_port(mut self, kind: TransportKind, port: u16) -> Self {
            self.inner.set_port(kind, port);
            self
        }

        #[cfg(feature = "tcp")]
        pub fn set_tcp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Tcp, port)
        }

        #[cfg(feature = "udp")]
        pub fn set_udp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Udp, port)
        }

        #[cfg(feature = "rtp")]
        pub fn set_rtp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Rtp, port)
        }

        #[cfg(feature = "rudp")]
        pub fn set_rudp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Rudp, port)
        }

        #[cfg(feature = "srt")]
        pub fn set_srt_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Srt, port)
        }

        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        pub fn set_udplite_port(self, port: u16) -> Self {
            self.set_port(TransportKind::UdpLite, port)
        }

        #[cfg(feature = "mqtt")]
        pub fn set_mqtt_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Mqtt, port)
        }

        #[cfg(feature = "coap")]
        pub fn set_coap_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Coap, port)
        }

        #[cfg(feature = "amqp")]
        pub fn set_amqp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Amqp, port)
        }

        #[cfg(all(feature = "dccp", any(target_os = "linux", target_os = "android")))]
        pub fn set_dccp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Dccp, port)
        }

        #[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
        pub fn set_sctp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Sctp, port)
        }

        #[cfg(feature = "quic")]
        pub fn set_quic_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Quic, port)
        }

        #[cfg(feature = "http1")]
        pub fn set_http1_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Http1, port)
        }

        #[cfg(feature = "http3")]
        pub fn set_http3_port(self, port: u16) -> Self {
            self.set_port(TransportKind::Http3, port)
        }

        #[cfg(all(feature = "websocket", feature = "tcp"))]
        pub fn set_ws_tcp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketTcp, port)
        }

        #[cfg(all(feature = "websocket", feature = "udp"))]
        pub fn set_ws_udp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketUdp, port)
        }

        #[cfg(all(feature = "websocket", feature = "rtp"))]
        pub fn set_ws_rtp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketRtp, port)
        }

        #[cfg(all(feature = "websocket", feature = "rudp"))]
        pub fn set_ws_rudp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketRudp, port)
        }

        #[cfg(all(feature = "websocket", feature = "srt"))]
        pub fn set_ws_srt_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketSrt, port)
        }

        #[cfg(all(feature = "websocket", feature = "mqtt"))]
        pub fn set_ws_mqtt_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketMqtt, port)
        }

        #[cfg(all(feature = "websocket", feature = "coap"))]
        pub fn set_ws_coap_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketCoap, port)
        }

        #[cfg(all(feature = "websocket", feature = "amqp"))]
        pub fn set_ws_amqp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketAmqp, port)
        }

        #[cfg(all(
            feature = "websocket",
            feature = "udplite",
            any(target_os = "linux", target_os = "android")
        ))]
        pub fn set_ws_udplite_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketUdpLite, port)
        }

        #[cfg(all(
            feature = "websocket",
            feature = "dccp",
            any(target_os = "linux", target_os = "android")
        ))]
        pub fn set_ws_dccp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketDccp, port)
        }

        #[cfg(all(
            feature = "websocket",
            feature = "sctp",
            any(target_os = "linux", target_os = "android")
        ))]
        pub fn set_ws_sctp_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketSctp, port)
        }

        #[cfg(all(feature = "websocket", feature = "quic"))]
        pub fn set_ws_quic_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketQuic, port)
        }

        #[cfg(all(feature = "websocket", feature = "http1"))]
        pub fn set_ws_http1_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketHttp1, port)
        }

        #[cfg(all(feature = "websocket", feature = "http3"))]
        pub fn set_ws_http3_port(self, port: u16) -> Self {
            self.set_port(TransportKind::WebSocketHttp3, port)
        }
    }

    impl Builder<HasBind, HasTransport> {
        pub fn build(self) -> MultiTransportServer {
            self.inner
        }

        pub async fn start(self) -> io::Result<RunningServer> {
            self.inner.start().await
        }
    }

    pub use Builder as MultiTransportServerBuilder;
}

pub use builder::{HasBind, HasTransport, MissingBind, MultiTransportServerBuilder, NoTransport};
