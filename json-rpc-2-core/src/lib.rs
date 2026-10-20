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
