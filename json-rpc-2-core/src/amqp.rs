use serde::{Deserialize, Serialize};

const AMQP_FRAME_END: u8 = 0xce;
const AMQP_FRAME_TYPE_BODY: u8 = 3;
const AMQP_FRAME_TYPE_HEARTBEAT: u8 = 8;
const AMQP_DEFAULT_CHANNEL: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum AmqpMessageKind {
    Request = 1,
    Response = 2,
    Notification = 3,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AmqpFrame {
    pub channel: u16,
    pub kind: Option<AmqpMessageKind>,
    pub correlation_id: u64,
    pub payload: Vec<u8>,
}

impl AmqpFrame {
    pub fn request(correlation_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel: AMQP_DEFAULT_CHANNEL,
            kind: Some(AmqpMessageKind::Request),
            correlation_id,
            payload: payload.into(),
        }
    }

    pub fn response(correlation_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel: AMQP_DEFAULT_CHANNEL,
            kind: Some(AmqpMessageKind::Response),
            correlation_id,
            payload: payload.into(),
        }
    }

    pub fn notification(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            channel: AMQP_DEFAULT_CHANNEL,
            kind: Some(AmqpMessageKind::Notification),
            correlation_id: 0,
            payload: payload.into(),
        }
    }

    pub fn heartbeat() -> Self {
        Self {
            channel: 0,
            kind: None,
            correlation_id: 0,
            payload: Vec::new(),
        }
    }
}

pub fn encode_amqp_frame(frame: &AmqpFrame) -> Result<Vec<u8>, String> {
    let (frame_type, payload) = match frame.kind {
        Some(kind) => {
            let mut payload = Vec::with_capacity(9 + frame.payload.len());
            payload.push(kind as u8);
            payload.extend_from_slice(&frame.correlation_id.to_be_bytes());
            payload.extend_from_slice(&frame.payload);
            (AMQP_FRAME_TYPE_BODY, payload)
        }
        None => {
            if !frame.payload.is_empty() || frame.correlation_id != 0 {
                return Err("AMQP heartbeat frames must not carry payload".to_owned());
            }
            (AMQP_FRAME_TYPE_HEARTBEAT, Vec::new())
        }
    };
    let mut bytes = Vec::with_capacity(8 + payload.len());
    bytes.push(frame_type);
    bytes.extend_from_slice(&frame.channel.to_be_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&payload);
    bytes.push(AMQP_FRAME_END);
    Ok(bytes)
}

pub fn decode_amqp_frame(packet: &[u8]) -> Result<AmqpFrame, String> {
    if packet.len() < 8 {
        return Err("AMQP frame too short".to_owned());
    }
    if packet[packet.len() - 1] != AMQP_FRAME_END {
        return Err("invalid AMQP frame end marker".to_owned());
    }
    let frame_type = packet[0];
    let channel = u16::from_be_bytes([packet[1], packet[2]]);
    let payload_len = u32::from_be_bytes([packet[3], packet[4], packet[5], packet[6]]) as usize;
    let payload = &packet[7..packet.len() - 1];
    if payload.len() != payload_len {
        return Err(format!(
            "AMQP payload length mismatch (header {payload_len}, actual {})",
            payload.len()
        ));
    }
    match frame_type {
        AMQP_FRAME_TYPE_BODY => {
            if payload.len() < 9 {
                return Err("AMQP body frame too short".to_owned());
            }
            let kind = match payload[0] {
                1 => AmqpMessageKind::Request,
                2 => AmqpMessageKind::Response,
                3 => AmqpMessageKind::Notification,
                other => return Err(format!("unknown AMQP message kind {other}")),
            };
            Ok(AmqpFrame {
                channel,
                kind: Some(kind),
                correlation_id: u64::from_be_bytes(
                    payload[1..9].try_into().expect("slice len checked"),
                ),
                payload: payload[9..].to_vec(),
            })
        }
        AMQP_FRAME_TYPE_HEARTBEAT => Ok(AmqpFrame::heartbeat()),
        other => Err(format!("unsupported AMQP frame type {other}")),
    }
}

#[cfg(test)]
mod tests;
