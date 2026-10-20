use serde::{Deserialize, Serialize};

const COAP_VERSION: u8 = 1;
const COAP_PAYLOAD_MARKER: u8 = 0xff;
const COAP_CODE_EMPTY: u8 = 0;
const COAP_CODE_POST: u8 = 2;
const COAP_CODE_CONTENT: u8 = 69;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CoapMessageType {
    Confirmable = 0,
    NonConfirmable = 1,
    Acknowledgement = 2,
    Reset = 3,
}

impl CoapMessageType {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Confirmable),
            1 => Some(Self::NonConfirmable),
            2 => Some(Self::Acknowledgement),
            3 => Some(Self::Reset),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CoapMessage {
    pub ty: CoapMessageType,
    pub code: u8,
    pub message_id: u16,
    pub token: Vec<u8>,
    pub payload: Vec<u8>,
}

impl CoapMessage {
    pub fn request(message_id: u16, token: Vec<u8>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            ty: CoapMessageType::Confirmable,
            code: COAP_CODE_POST,
            message_id,
            token,
            payload: payload.into(),
        }
    }

    pub fn response(message_id: u16, token: Vec<u8>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            ty: CoapMessageType::Acknowledgement,
            code: COAP_CODE_CONTENT,
            message_id,
            token,
            payload: payload.into(),
        }
    }

    pub fn ack(message_id: u16, token: Vec<u8>) -> Self {
        Self {
            ty: CoapMessageType::Acknowledgement,
            code: COAP_CODE_EMPTY,
            message_id,
            token,
            payload: Vec::new(),
        }
    }

    pub fn ping(message_id: u16) -> Self {
        Self {
            ty: CoapMessageType::Confirmable,
            code: COAP_CODE_EMPTY,
            message_id,
            token: Vec::new(),
            payload: Vec::new(),
        }
    }

    pub fn pong(message_id: u16) -> Self {
        Self {
            ty: CoapMessageType::Acknowledgement,
            code: COAP_CODE_EMPTY,
            message_id,
            token: Vec::new(),
            payload: Vec::new(),
        }
    }
}

pub fn encode_coap_message(message: &CoapMessage) -> Result<Vec<u8>, String> {
    if message.token.len() > 8 {
        return Err("CoAP token must be at most 8 bytes".to_owned());
    }
    let mut bytes = Vec::with_capacity(4 + message.token.len() + message.payload.len() + 1);
    bytes.push((COAP_VERSION << 6) | ((message.ty as u8) << 4) | message.token.len() as u8);
    bytes.push(message.code);
    bytes.extend_from_slice(&message.message_id.to_be_bytes());
    bytes.extend_from_slice(&message.token);
    if !message.payload.is_empty() {
        bytes.push(COAP_PAYLOAD_MARKER);
        bytes.extend_from_slice(&message.payload);
    }
    Ok(bytes)
}

pub fn decode_coap_message(packet: &[u8]) -> Result<CoapMessage, String> {
    if packet.len() < 4 {
        return Err("CoAP packet too short".to_owned());
    }
    let version = packet[0] >> 6;
    if version != COAP_VERSION {
        return Err(format!("unsupported CoAP version {version}"));
    }
    let ty = CoapMessageType::from_byte((packet[0] >> 4) & 0x03)
        .ok_or_else(|| format!("unknown CoAP message type {}", (packet[0] >> 4) & 0x03))?;
    let token_len = (packet[0] & 0x0f) as usize;
    if packet.len() < 4 + token_len {
        return Err("CoAP packet truncated before token".to_owned());
    }
    let message_id = u16::from_be_bytes([packet[2], packet[3]]);
    let token = packet[4..4 + token_len].to_vec();
    let payload = if packet.len() == 4 + token_len {
        Vec::new()
    } else {
        let marker_index = 4 + token_len;
        if packet[marker_index] != COAP_PAYLOAD_MARKER {
            return Err("CoAP payload marker missing".to_owned());
        }
        packet[marker_index + 1..].to_vec()
    };
    Ok(CoapMessage {
        ty,
        code: packet[1],
        message_id,
        token,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coap_roundtrip() {
        let message = CoapMessage::request(9, vec![1, 2, 3, 4], b"hello".to_vec());
        let encoded = encode_coap_message(&message).unwrap();
        assert_eq!(decode_coap_message(&encoded).unwrap(), message);
    }
}
