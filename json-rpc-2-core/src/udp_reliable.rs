use std::time::Duration;

use serde::{Deserialize, Serialize};

const UDP_MAGIC: [u8; 4] = *b"JRP2";
const UDP_VERSION: u8 = 1;
const UDP_HEADER_LEN: usize = 20;
const UDP_MAX_PAYLOAD: usize = 65_507 - UDP_HEADER_LEN;

/// Reliability policy for the custom UDP transport.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct UdpReliabilityConfig {
    /// Delay between retransmission attempts while waiting for ACK/response.
    pub retransmit_interval: Duration,
    /// Number of retransmits after the initial send.
    pub max_retransmits: u32,
    /// Upper bound for one UDP packet, including transport header.
    pub max_packet_size: usize,
    /// Maximum number of cached request entries per peer for deduplication.
    pub dedup_cache_capacity: usize,
    /// Time-to-live of deduplication cache entries on the server.
    pub dedup_cache_ttl: Duration,
}

impl Default for UdpReliabilityConfig {
    fn default() -> Self {
        Self {
            retransmit_interval: Duration::from_millis(50),
            max_retransmits: 5,
            max_packet_size: 4096,
            dedup_cache_capacity: 4096,
            dedup_cache_ttl: Duration::from_secs(30),
        }
    }
}

/// Packet kinds carried by the custom reliable UDP transport.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ReliableUdpPacketKind {
    Request = 1,
    Response = 2,
    Ack = 3,
    Ping = 4,
    Pong = 5,
}

impl ReliableUdpPacketKind {
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

/// One complete reliable-UDP transport packet.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReliableUdpPacket {
    /// Sequence/correlation id. For `Request`/`Ping` it is the outbound
    /// sequence number; for `Ack`/`Response`/`Pong` it references the packet
    /// being acknowledged or answered.
    pub packet_id: u64,
    pub kind: ReliableUdpPacketKind,
    pub payload: Vec<u8>,
}

impl ReliableUdpPacket {
    pub fn request(packet_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            packet_id,
            kind: ReliableUdpPacketKind::Request,
            payload: payload.into(),
        }
    }

    pub fn response(packet_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            packet_id,
            kind: ReliableUdpPacketKind::Response,
            payload: payload.into(),
        }
    }

    pub fn ack(packet_id: u64) -> Self {
        Self {
            packet_id,
            kind: ReliableUdpPacketKind::Ack,
            payload: Vec::new(),
        }
    }

    pub fn ping(packet_id: u64) -> Self {
        Self {
            packet_id,
            kind: ReliableUdpPacketKind::Ping,
            payload: Vec::new(),
        }
    }

    pub fn pong(packet_id: u64) -> Self {
        Self {
            packet_id,
            kind: ReliableUdpPacketKind::Pong,
            payload: Vec::new(),
        }
    }
}

pub fn encode_udp_packet(packet: &ReliableUdpPacket) -> Result<Vec<u8>, String> {
    if packet.payload.len() > UDP_MAX_PAYLOAD {
        return Err(format!(
            "UDP payload exceeds transport limit ({} > {})",
            packet.payload.len(),
            UDP_MAX_PAYLOAD
        ));
    }

    let mut bytes = Vec::with_capacity(UDP_HEADER_LEN + packet.payload.len());
    bytes.extend_from_slice(&UDP_MAGIC);
    bytes.push(UDP_VERSION);
    bytes.push(packet.kind as u8);
    bytes.extend_from_slice(&0u16.to_be_bytes());
    bytes.extend_from_slice(&packet.packet_id.to_be_bytes());
    bytes.extend_from_slice(&(packet.payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&packet.payload);
    Ok(bytes)
}

/// Validate technical framing without allocating or inspecting business data.
pub fn inspect_udp_packet(packet: &[u8]) -> Result<(ReliableUdpPacketKind, &[u8]), String> {
    if packet.len() < UDP_HEADER_LEN {
        return Err("UDP packet too short".to_owned());
    }
    if packet[..4] != UDP_MAGIC {
        return Err("invalid UDP transport magic".to_owned());
    }
    if packet[4] != UDP_VERSION {
        return Err(format!("unsupported UDP transport version {}", packet[4]));
    }
    let kind = ReliableUdpPacketKind::from_byte(packet[5])
        .ok_or_else(|| format!("unknown UDP packet kind {}", packet[5]))?;
    let payload_len =
        u32::from_be_bytes(packet[16..20].try_into().expect("slice len checked")) as usize;
    let remaining = packet.len() - UDP_HEADER_LEN;
    if payload_len != remaining {
        return Err(format!(
            "UDP payload length mismatch (header {payload_len}, actual {remaining})"
        ));
    }
    Ok((kind, &packet[UDP_HEADER_LEN..]))
}

pub fn decode_udp_packet(packet: &[u8]) -> Result<ReliableUdpPacket, String> {
    let (kind, payload) = inspect_udp_packet(packet)?;
    Ok(ReliableUdpPacket {
        packet_id: u64::from_be_bytes(packet[8..16].try_into().expect("validated header")),
        kind,
        payload: payload.to_vec(),
    })
}
