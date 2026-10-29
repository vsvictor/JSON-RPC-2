//! Native SRT configuration and historical JSRT codec compatibility utilities.
//!
//! Only [`SrtConfig`] configures the current SRT transport. [`SrtCrypto`],
//! [`SrtPacket`] and [`SrtPacketKind`] describe the obsolete private JSRT
//! format, not SRT. They remain solely for decoding historical data and legacy
//! codec tests; they are unsupported for live transport use. Current adapters
//! use libsrt's handshake, message delivery and passphrase encryption instead.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SRT_MAGIC: [u8; 4] = *b"JSRT";
const SRT_VERSION: u8 = 1;
const SRT_HEADER_LEN: usize = 22;
const SRT_MAX_PAYLOAD: usize = 65_507 - SRT_HEADER_LEN;

/// Encryption configuration for native libsrt caller/listener connections.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct SrtConfig {
    /// Shared passphrase, containing 10 through 79 UTF-8 bytes.
    /// Empty disables encryption in libsrt itself but is not supported by this adapter.
    pub passphrase: String,
}

impl SrtConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(10..=79).contains(&self.passphrase.len()) {
            return Err("libsrt passphrase must contain 10..=79 bytes".to_owned());
        }
        Ok(())
    }
}

/// **Legacy JSRT only.** Not a standard SRT packet type; unsupported by current adapters.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SrtPacketKind {
    Request = 1,
    Response = 2,
    Ack = 3,
    Ping = 4,
    Pong = 5,
}

impl SrtPacketKind {
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

/// **Legacy JSRT only.** Private packet format, not accepted by native SRT adapters.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SrtPacket {
    pub sequence_number: u32,
    pub packet_id: u64,
    pub kind: SrtPacketKind,
    pub payload: Vec<u8>,
}

impl SrtPacket {
    pub fn request(sequence_number: u32, packet_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: SrtPacketKind::Request,
            payload: payload.into(),
        }
    }

    pub fn response(sequence_number: u32, packet_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: SrtPacketKind::Response,
            payload: payload.into(),
        }
    }

    pub fn ack(sequence_number: u32, packet_id: u64) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: SrtPacketKind::Ack,
            payload: Vec::new(),
        }
    }

    pub fn ping(sequence_number: u32, packet_id: u64) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: SrtPacketKind::Ping,
            payload: Vec::new(),
        }
    }

    pub fn pong(sequence_number: u32, packet_id: u64) -> Self {
        Self {
            sequence_number,
            packet_id,
            kind: SrtPacketKind::Pong,
            payload: Vec::new(),
        }
    }
}

/// **Legacy JSRT only; unsupported for live transport use.**
///
/// This SHA-256/AES-GCM codec is not libsrt's key management or encryption.
/// It is retained under its historical name for legacy codec compatibility;
/// current SRT adapters never instantiate it.
#[derive(Clone)]
pub struct SrtCrypto {
    cipher: Aes256Gcm,
}

impl SrtCrypto {
    pub fn new(config: &SrtConfig) -> Result<Self, String> {
        config.validate()?;
        let key = Sha256::digest(config.passphrase.as_bytes());
        Ok(Self {
            cipher: Aes256Gcm::new_from_slice(&key).map_err(|error| error.to_string())?,
        })
    }

    pub fn encode_packet(&self, packet: &SrtPacket) -> Result<Vec<u8>, String> {
        if packet.payload.len() > SRT_MAX_PAYLOAD {
            return Err(format!(
                "legacy JSRT payload exceeds codec limit ({} > {})",
                packet.payload.len(),
                SRT_MAX_PAYLOAD
            ));
        }
        let mut header = Vec::with_capacity(SRT_HEADER_LEN);
        header.extend_from_slice(&SRT_MAGIC);
        header.push(SRT_VERSION);
        header.push(packet.kind as u8);
        header.extend_from_slice(&packet.sequence_number.to_be_bytes());
        header.extend_from_slice(&packet.packet_id.to_be_bytes());
        header.extend_from_slice(&0u32.to_be_bytes());
        let nonce = packet_nonce(packet.sequence_number, packet.packet_id);
        let encrypted = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &packet.payload,
                    aad: &header[..18],
                },
            )
            .map_err(|error| error.to_string())?;
        header[18..22].copy_from_slice(&(encrypted.len() as u32).to_be_bytes());
        let mut bytes = header;
        bytes.extend_from_slice(&encrypted);
        Ok(bytes)
    }

    pub fn decode_packet(&self, packet: &[u8]) -> Result<SrtPacket, String> {
        if packet.len() < SRT_HEADER_LEN {
            return Err("legacy JSRT packet too short".to_owned());
        }
        if packet[..4] != SRT_MAGIC {
            return Err("invalid legacy JSRT codec magic".to_owned());
        }
        if packet[4] != SRT_VERSION {
            return Err(format!(
                "unsupported legacy JSRT codec version {}",
                packet[4]
            ));
        }
        let kind = SrtPacketKind::from_byte(packet[5])
            .ok_or_else(|| format!("unknown legacy JSRT packet kind {}", packet[5]))?;
        let sequence_number =
            u32::from_be_bytes(packet[6..10].try_into().expect("slice len checked"));
        let packet_id = u64::from_be_bytes(packet[10..18].try_into().expect("slice len checked"));
        let payload_len =
            u32::from_be_bytes(packet[18..22].try_into().expect("slice len checked")) as usize;
        let remaining = packet.len() - SRT_HEADER_LEN;
        if payload_len != remaining {
            return Err(format!(
                "legacy JSRT payload length mismatch (header {payload_len}, actual {remaining})"
            ));
        }
        let nonce = packet_nonce(sequence_number, packet_id);
        let decrypted = self
            .cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &packet[SRT_HEADER_LEN..],
                    aad: &packet[..18],
                },
            )
            .map_err(|error| error.to_string())?;
        Ok(SrtPacket {
            sequence_number,
            packet_id,
            kind,
            payload: decrypted,
        })
    }
}

fn packet_nonce(sequence_number: u32, packet_id: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[..4].copy_from_slice(&sequence_number.to_be_bytes());
    nonce[4..12].copy_from_slice(&packet_id.to_be_bytes());
    nonce
}

#[cfg(test)]
mod tests;
