//! Shared JSON-RPC 2.0 wire types and protocol constants.

mod amqp;
mod coap;
mod mqtt;
mod rtp;
mod rudp;
mod srt;
mod udp_lite;
mod udp_reliable;
mod websocket;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use amqp::{AmqpFrame, AmqpMessageKind, decode_amqp_frame, encode_amqp_frame};
pub use coap::{CoapMessage, CoapMessageType, decode_coap_message, encode_coap_message};
pub use mqtt::{MqttPacket, MqttPacketKind, decode_mqtt_packet, encode_mqtt_packet};
pub use rtp::{
    RtcpControlPacket, RtcpControlPacketKind, RtpPacket, RtpPacketKind, decode_rtcp_control_packet,
    decode_rtp_packet, encode_rtcp_control_packet, encode_rtp_packet,
};
pub use rudp::{RudpPacket, RudpPacketKind, decode_rudp_packet, encode_rudp_packet};
pub use srt::{SrtConfig, SrtCrypto, SrtPacket, SrtPacketKind};
pub use udp_lite::UdpLiteConfig;
pub use udp_reliable::{
    ReliableUdpPacket, ReliableUdpPacketKind, UdpReliabilityConfig, decode_udp_packet,
    encode_udp_packet,
};
pub use websocket::{
    WebSocketFrame, WebSocketFrameKind, WebSocketFuture, WebSocketMessage, WebSocketProtocol,
    WebSocketUdpFrame, WebSocketUdpFrameKind, decode_websocket_frame, decode_websocket_message,
    decode_websocket_udp_frame, encode_websocket_close_frame, encode_websocket_frame,
    encode_websocket_message, encode_websocket_udp_frame, websocket_message_bytes,
};

pub const VERSION: &str = "2.0";
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const CONTROL_PATH: &str = "/.well-known/json-rpc-2/ping";
pub const CONTROL_PING: &[u8] = b"#json-rpc-2-control-ping";
pub const CONTROL_PONG: &[u8] = b"#json-rpc-2-control-pong";
pub const QUIC_ALPN: &[u8] = b"json-rpc-2";
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

/// TCP congestion control profile used by TCP-based transports.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
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

/// JSON-RPC request identifier. The protocol allows strings, numbers, and null.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum RpcId {
    String(String),
    Number(serde_json::Number),
    Null,
}

impl RpcId {
    pub fn number(value: u64) -> Self {
        Self::Number(value.into())
    }
}

/// Request object. Omit `id` to create a notification.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<RpcId>,
}

impl RpcRequest {
    pub fn call(method: impl Into<String>, params: Option<Value>, id: RpcId) -> Self {
        Self {
            jsonrpc: VERSION.to_owned(),
            method: method.into(),
            params,
            id: Some(id),
        }
    }

    pub fn notification(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: VERSION.to_owned(),
            method: method.into(),
            params,
            id: None,
        }
    }
}

/// JSON-RPC error object.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn parse_error() -> Self {
        Self::new(-32700, "Parse error")
    }

    pub fn invalid_request() -> Self {
        Self::new(-32600, "Invalid Request")
    }

    pub fn method_not_found() -> Self {
        Self::new(-32601, "Method not found")
    }

    pub fn invalid_params() -> Self {
        Self::new(-32602, "Invalid params")
    }

    pub fn internal_error() -> Self {
        Self::new(-32603, "Internal error")
    }
}

/// A successful JSON-RPC response.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RpcSuccess {
    pub jsonrpc: String,
    pub result: Value,
    pub id: RpcId,
}

/// A failed JSON-RPC response.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RpcFailure {
    pub jsonrpc: String,
    pub error: RpcError,
    pub id: RpcId,
}
