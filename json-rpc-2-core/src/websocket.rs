use std::{future::Future, pin::Pin};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

const WS_FRAME_MAGIC: [u8; 4] = *b"WSU2";
const WS_FRAME_VERSION: u8 = 1;
const WS_FRAME_HEADER_LEN: usize = 10;

/// A transport-neutral WebSocket application message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WebSocketMessage {
    Text(String),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
}

/// Future returned while a WebSocket data message is handled.
pub type WebSocketFuture<'a> = Pin<Box<dyn Future<Output = Option<WebSocketMessage>> + Send + 'a>>;

/// Protocol carried over an established WebSocket session.
pub trait WebSocketProtocol: Send + Sync + 'static {
    fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a>;
}

/// WebSocket-style frame kinds carried inside datagram-style transports.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WebSocketFrameKind {
    Text = 1,
    Binary = 2,
    Close = 3,
    Ping = 4,
    Pong = 5,
}

impl WebSocketFrameKind {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Text),
            2 => Some(Self::Binary),
            3 => Some(Self::Close),
            4 => Some(Self::Ping),
            5 => Some(Self::Pong),
            _ => None,
        }
    }
}

/// One WebSocket-style frame carried inside a transport-specific envelope.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WebSocketFrame {
    pub kind: WebSocketFrameKind,
    pub payload: Vec<u8>,
}

impl WebSocketFrame {
    pub fn text(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: WebSocketFrameKind::Text,
            payload: payload.into(),
        }
    }

    pub fn binary(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: WebSocketFrameKind::Binary,
            payload: payload.into(),
        }
    }

    pub fn close() -> Self {
        Self {
            kind: WebSocketFrameKind::Close,
            payload: Vec::new(),
        }
    }

    pub fn ping(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: WebSocketFrameKind::Ping,
            payload: payload.into(),
        }
    }

    pub fn pong(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: WebSocketFrameKind::Pong,
            payload: payload.into(),
        }
    }
}

pub fn encode_websocket_frame(frame: &WebSocketFrame) -> Result<Vec<u8>, String> {
    validate_control_frame(frame)?;
    let payload_len = u32::try_from(frame.payload.len())
        .map_err(|_| "WebSocket frame payload exceeds the protocol limit".to_owned())?;
    let mut bytes = Vec::with_capacity(WS_FRAME_HEADER_LEN + frame.payload.len());
    bytes.extend_from_slice(&WS_FRAME_MAGIC);
    bytes.push(WS_FRAME_VERSION);
    bytes.push(frame.kind as u8);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&frame.payload);
    Ok(bytes)
}

pub fn decode_websocket_frame(packet: &[u8]) -> Result<WebSocketFrame, String> {
    if packet.len() < WS_FRAME_HEADER_LEN {
        return Err("WebSocket frame too short".to_owned());
    }
    if packet[..4] != WS_FRAME_MAGIC {
        return Err("invalid WebSocket frame magic".to_owned());
    }
    if packet[4] != WS_FRAME_VERSION {
        return Err(format!("unsupported WebSocket frame version {}", packet[4]));
    }
    let kind = WebSocketFrameKind::from_byte(packet[5])
        .ok_or_else(|| format!("unknown WebSocket frame kind {}", packet[5]))?;
    let payload_len =
        u32::from_be_bytes(packet[6..10].try_into().expect("slice len checked")) as usize;
    let remaining = packet.len() - WS_FRAME_HEADER_LEN;
    if payload_len != remaining {
        return Err(format!(
            "WebSocket payload length mismatch (header {payload_len}, actual {remaining})"
        ));
    }
    let frame = WebSocketFrame {
        kind,
        payload: packet[WS_FRAME_HEADER_LEN..].to_vec(),
    };
    validate_control_frame(&frame)?;
    Ok(frame)
}

fn validate_control_frame(frame: &WebSocketFrame) -> Result<(), String> {
    if matches!(
        frame.kind,
        WebSocketFrameKind::Close | WebSocketFrameKind::Ping | WebSocketFrameKind::Pong
    ) && (frame.payload.len() > 125
        || (frame.kind == WebSocketFrameKind::Close && frame.payload.len() == 1))
    {
        return Err("invalid WebSocket control frame payload length".to_owned());
    }
    Ok(())
}

pub fn encode_websocket_message(message: &WebSocketMessage) -> Result<Vec<u8>, String> {
    let frame = match message {
        WebSocketMessage::Text(text) => WebSocketFrame::text(text.as_bytes()),
        WebSocketMessage::Binary(bytes) => WebSocketFrame::binary(bytes.to_vec()),
        WebSocketMessage::Ping(bytes) => WebSocketFrame::ping(bytes.to_vec()),
        WebSocketMessage::Pong(bytes) => WebSocketFrame::pong(bytes.to_vec()),
    };
    encode_websocket_frame(&frame)
}

pub fn encode_websocket_close_frame() -> Result<Vec<u8>, String> {
    encode_websocket_frame(&WebSocketFrame::close())
}

pub fn decode_websocket_message(packet: &[u8]) -> Result<Option<WebSocketMessage>, String> {
    let frame = decode_websocket_frame(packet)?;
    match frame.kind {
        WebSocketFrameKind::Text => String::from_utf8(frame.payload)
            .map(|text| Some(WebSocketMessage::Text(text)))
            .map_err(|error| format!("invalid WebSocket text payload: {error}")),
        WebSocketFrameKind::Binary => {
            Ok(Some(WebSocketMessage::Binary(Bytes::from(frame.payload))))
        }
        WebSocketFrameKind::Close => Ok(None),
        WebSocketFrameKind::Ping => Ok(Some(WebSocketMessage::Ping(Bytes::from(frame.payload)))),
        WebSocketFrameKind::Pong => Ok(Some(WebSocketMessage::Pong(Bytes::from(frame.payload)))),
    }
}

pub fn websocket_message_bytes(message: &WebSocketMessage) -> &[u8] {
    match message {
        WebSocketMessage::Text(text) => text.as_bytes(),
        WebSocketMessage::Binary(bytes) => bytes.as_ref(),
        WebSocketMessage::Ping(bytes) | WebSocketMessage::Pong(bytes) => bytes.as_ref(),
    }
}

pub type WebSocketUdpFrame = WebSocketFrame;
pub type WebSocketUdpFrameKind = WebSocketFrameKind;

pub fn encode_websocket_udp_frame(frame: &WebSocketUdpFrame) -> Result<Vec<u8>, String> {
    encode_websocket_frame(frame)
}

pub fn decode_websocket_udp_frame(packet: &[u8]) -> Result<WebSocketUdpFrame, String> {
    decode_websocket_frame(packet)
}

#[cfg(test)]
mod tests;
