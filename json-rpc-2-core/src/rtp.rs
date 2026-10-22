use serde::{Deserialize, Serialize};

const RTP_VERSION: u8 = 2;
const RTP_HEADER_LEN: usize = 12;
const RTP_PAYLOAD_TYPE_JSON_RPC: u8 = 96;
const RTP_METADATA_LEN: usize = 9;
const RTP_MAX_PAYLOAD: usize = 65_507 - RTP_HEADER_LEN;

const RTCP_PACKET_TYPE_APP: u8 = 204;
const RTCP_NAME: [u8; 4] = *b"JRP2";
const RTCP_PACKET_LEN: usize = 20;

/// Packet kind carried inside the custom RTP payload.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RtpPacketKind {
    Request = 1,
    Response = 2,
}

impl RtpPacketKind {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Request),
            2 => Some(Self::Response),
            _ => None,
        }
    }
}

/// One JSON-RPC message carried inside an RTP packet.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RtpPacket {
    pub sequence_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub kind: RtpPacketKind,
    /// Correlates request/response pairs and RTCP control traffic.
    pub message_id: u64,
    pub payload: Vec<u8>,
}

/// Minimal RTCP APP control packet used for ACK / ping / pong.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RtcpControlPacketKind {
    Ack = 1,
    Ping = 2,
    Pong = 3,
}

impl RtcpControlPacketKind {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Ack),
            2 => Some(Self::Ping),
            3 => Some(Self::Pong),
            _ => None,
        }
    }
}

/// RTCP-side control message multiplexed with RTP on the same UDP socket.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RtcpControlPacket {
    pub kind: RtcpControlPacketKind,
    pub ssrc: u32,
    pub message_id: u64,
}

pub fn encode_rtp_packet(packet: &RtpPacket) -> Result<Vec<u8>, String> {
    let payload_len = RTP_METADATA_LEN + packet.payload.len();
    if payload_len > RTP_MAX_PAYLOAD {
        return Err(format!(
            "RTP payload exceeds transport limit ({} > {})",
            payload_len, RTP_MAX_PAYLOAD
        ));
    }

    let mut bytes = Vec::with_capacity(RTP_HEADER_LEN + payload_len);
    bytes.push(RTP_VERSION << 6);
    bytes.push(RTP_PAYLOAD_TYPE_JSON_RPC);
    bytes.extend_from_slice(&packet.sequence_number.to_be_bytes());
    bytes.extend_from_slice(&packet.timestamp.to_be_bytes());
    bytes.extend_from_slice(&packet.ssrc.to_be_bytes());
    bytes.push(packet.kind as u8);
    bytes.extend_from_slice(&packet.message_id.to_be_bytes());
    bytes.extend_from_slice(&packet.payload);
    Ok(bytes)
}

/// Validate technical framing without allocating or inspecting business data.
pub fn inspect_rtp_packet(packet: &[u8]) -> Result<(RtpPacketKind, &[u8]), String> {
    if packet.len() < RTP_HEADER_LEN + RTP_METADATA_LEN {
        return Err("RTP packet too short".to_owned());
    }
    if packet[0] >> 6 != RTP_VERSION {
        return Err(format!("unsupported RTP version {}", packet[0] >> 6));
    }
    if packet[1] != RTP_PAYLOAD_TYPE_JSON_RPC {
        return Err(format!("unexpected RTP payload type {}", packet[1]));
    }
    let kind = RtpPacketKind::from_byte(packet[RTP_HEADER_LEN])
        .ok_or_else(|| format!("unknown RTP packet kind {}", packet[RTP_HEADER_LEN]))?;
    Ok((kind, &packet[RTP_HEADER_LEN + RTP_METADATA_LEN..]))
}

pub fn decode_rtp_packet(packet: &[u8]) -> Result<RtpPacket, String> {
    let (kind, payload) = inspect_rtp_packet(packet)?;
    Ok(RtpPacket {
        sequence_number: u16::from_be_bytes(packet[2..4].try_into().expect("slice len checked")),
        timestamp: u32::from_be_bytes(packet[4..8].try_into().expect("slice len checked")),
        ssrc: u32::from_be_bytes(packet[8..12].try_into().expect("slice len checked")),
        kind,
        message_id: u64::from_be_bytes(packet[13..21].try_into().expect("slice len checked")),
        payload: payload.to_vec(),
    })
}

pub fn encode_rtcp_control_packet(packet: &RtcpControlPacket) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::with_capacity(RTCP_PACKET_LEN);
    bytes.push((RTP_VERSION << 6) | (packet.kind as u8 & 0x1f));
    bytes.push(RTCP_PACKET_TYPE_APP);
    bytes.extend_from_slice(&4u16.to_be_bytes());
    bytes.extend_from_slice(&packet.ssrc.to_be_bytes());
    bytes.extend_from_slice(&RTCP_NAME);
    bytes.extend_from_slice(&packet.message_id.to_be_bytes());
    Ok(bytes)
}

pub fn decode_rtcp_control_packet(packet: &[u8]) -> Result<RtcpControlPacket, String> {
    if packet.len() != RTCP_PACKET_LEN {
        return Err(format!(
            "RTCP control packet must be exactly {RTCP_PACKET_LEN} bytes"
        ));
    }
    if packet[0] >> 6 != RTP_VERSION {
        return Err(format!("unsupported RTCP version {}", packet[0] >> 6));
    }
    if packet[1] != RTCP_PACKET_TYPE_APP {
        return Err(format!("unexpected RTCP packet type {}", packet[1]));
    }
    let length = u16::from_be_bytes(packet[2..4].try_into().expect("slice len checked"));
    if length != 4 {
        return Err(format!("unexpected RTCP APP length {}", length));
    }
    if packet[8..12] != RTCP_NAME {
        return Err("invalid RTCP APP name".to_owned());
    }
    let kind = RtcpControlPacketKind::from_byte(packet[0] & 0x1f)
        .ok_or_else(|| format!("unknown RTCP control kind {}", packet[0] & 0x1f))?;
    Ok(RtcpControlPacket {
        kind,
        ssrc: u32::from_be_bytes(packet[4..8].try_into().expect("slice len checked")),
        message_id: u64::from_be_bytes(packet[12..20].try_into().expect("slice len checked")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rtp_roundtrip_preserves_header_and_payload() {
        let packet = RtpPacket {
            sequence_number: 7,
            timestamp: 42,
            ssrc: 99,
            kind: RtpPacketKind::Request,
            message_id: 123,
            payload: br#"{"jsonrpc":"2.0"}"#.to_vec(),
        };
        let encoded = encode_rtp_packet(&packet).unwrap();
        assert_eq!(decode_rtp_packet(&encoded).unwrap(), packet);
    }

    #[test]
    fn rtcp_roundtrip_preserves_control_fields() {
        let packet = RtcpControlPacket {
            kind: RtcpControlPacketKind::Ping,
            ssrc: 11,
            message_id: 88,
        };
        let encoded = encode_rtcp_control_packet(&packet).unwrap();
        assert_eq!(decode_rtcp_control_packet(&encoded).unwrap(), packet);
    }
}
