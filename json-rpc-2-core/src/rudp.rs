use serde::{Deserialize, Serialize};

const RUDP_MAGIC: [u8; 4] = *b"RUDP";
const RUDP_VERSION: u8 = 1;
const RUDP_HEADER_LEN: usize = 24;
const RUDP_MAX_PAYLOAD: usize = 65_507 - RUDP_HEADER_LEN;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RudpPacketKind {
    Request = 1,
    Response = 2,
    Ack = 3,
    Ping = 4,
    Pong = 5,
}

impl RudpPacketKind {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Request),
            2 => Some(Self::Response),
            3 => Some(Self::Ack),
            4 => Some(Self::Ping),
            5 => Some(Self::Pong),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RudpPacket {
    pub sequence_number: u32,
    pub packet_id: u64,
    pub kind: RudpPacketKind,
    pub payload: Vec<u8>,
}

impl RudpPacket {
    pub fn request(sequence_number: u32, packet_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: RudpPacketKind::Request,
            payload: payload.into(),
        }
    }

    pub fn response(sequence_number: u32, packet_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: RudpPacketKind::Response,
            payload: payload.into(),
        }
    }

    pub fn ack(sequence_number: u32, packet_id: u64) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: RudpPacketKind::Ack,
            payload: Vec::new(),
        }
    }

    pub fn ping(sequence_number: u32, packet_id: u64) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: RudpPacketKind::Ping,
            payload: Vec::new(),
        }
    }

    pub fn pong(sequence_number: u32, packet_id: u64) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: RudpPacketKind::Pong,
            payload: Vec::new(),
        }
    }
}

pub fn encode_rudp_packet(packet: &RudpPacket) -> Result<Vec<u8>, String> {
    if packet.payload.len() > RUDP_MAX_PAYLOAD {
        return Err(format!(
            "RUDP payload exceeds transport limit ({} > {})",
            packet.payload.len(),
            RUDP_MAX_PAYLOAD
        ));
    }
    let mut bytes = Vec::with_capacity(RUDP_HEADER_LEN + packet.payload.len());
    bytes.extend_from_slice(&RUDP_MAGIC);
    bytes.push(RUDP_VERSION);
    bytes.push(packet.kind as u8);
    bytes.extend_from_slice(&packet.sequence_number.to_be_bytes());
    bytes.extend_from_slice(&packet.packet_id.to_be_bytes());
    bytes.extend_from_slice(&(packet.payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&0u16.to_be_bytes());
    bytes.extend_from_slice(&packet.payload);
    Ok(bytes)
}

/// Validate technical framing without allocating or inspecting business data.
pub fn inspect_rudp_packet(packet: &[u8]) -> Result<(RudpPacketKind, &[u8]), String> {
    if packet.len() < RUDP_HEADER_LEN {
        return Err("RUDP packet too short".to_owned());
    }
    if packet[..4] != RUDP_MAGIC {
        return Err("invalid RUDP transport magic".to_owned());
    }
    if packet[4] != RUDP_VERSION {
        return Err(format!("unsupported RUDP transport version {}", packet[4]));
    }
    let kind = RudpPacketKind::from_byte(packet[5])
        .ok_or_else(|| format!("unknown RUDP packet kind {}", packet[5]))?;
    let payload_len =
        u32::from_be_bytes(packet[18..22].try_into().expect("slice len checked")) as usize;
    let remaining = packet.len() - RUDP_HEADER_LEN;
    if payload_len != remaining {
        return Err(format!(
            "RUDP payload length mismatch (header {payload_len}, actual {remaining})"
        ));
    }
    Ok((kind, &packet[RUDP_HEADER_LEN..]))
}

pub fn decode_rudp_packet(packet: &[u8]) -> Result<RudpPacket, String> {
    let (kind, payload) = inspect_rudp_packet(packet)?;
    Ok(RudpPacket {
        sequence_number: u32::from_be_bytes(packet[6..10].try_into().expect("validated header")),
        packet_id: u64::from_be_bytes(packet[10..18].try_into().expect("validated header")),
        kind,
        payload: payload.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rudp_roundtrip_preserves_fields() {
        let packet = RudpPacket::request(7, 99, b"hello".to_vec());
        let encoded = encode_rudp_packet(&packet).unwrap();
        assert_eq!(decode_rudp_packet(&encoded).unwrap(), packet);
    }
}
