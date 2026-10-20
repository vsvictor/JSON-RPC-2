use serde::{Deserialize, Serialize};

const MQTT_PACKET_TYPE_PUBLISH: u8 = 3;
const MQTT_PACKET_TYPE_PUBACK: u8 = 4;
const MQTT_PACKET_TYPE_PINGREQ: u8 = 12;
const MQTT_PACKET_TYPE_PINGRESP: u8 = 13;
const MQTT_TOPIC_REQUEST: &str = "json-rpc/2/request";
const MQTT_TOPIC_RESPONSE: &str = "json-rpc/2/response";
const MQTT_TOPIC_NOTIFICATION: &str = "json-rpc/2/notification";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum MqttPacketKind {
    Request,
    Response,
    Notification,
    Ack,
    Ping,
    Pong,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MqttPacket {
    pub kind: MqttPacketKind,
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

impl MqttPacket {
    pub fn request(packet_id: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: MqttPacketKind::Request,
            packet_id: Some(packet_id),
            payload: payload.into(),
        }
    }

    pub fn response(packet_id: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: MqttPacketKind::Response,
            packet_id: Some(packet_id),
            payload: payload.into(),
        }
    }

    pub fn notification(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: MqttPacketKind::Notification,
            packet_id: None,
            payload: payload.into(),
        }
    }

    pub fn ack(packet_id: u16) -> Self {
        Self {
            kind: MqttPacketKind::Ack,
            packet_id: Some(packet_id),
            payload: Vec::new(),
        }
    }

    pub fn ping() -> Self {
        Self {
            kind: MqttPacketKind::Ping,
            packet_id: None,
            payload: Vec::new(),
        }
    }

    pub fn pong() -> Self {
        Self {
            kind: MqttPacketKind::Pong,
            packet_id: None,
            payload: Vec::new(),
        }
    }
}

pub fn encode_mqtt_packet(packet: &MqttPacket) -> Result<Vec<u8>, String> {
    let (header, body) = match packet.kind {
        MqttPacketKind::Request => (
            MQTT_PACKET_TYPE_PUBLISH << 4,
            encode_publish_body(
                MQTT_TOPIC_REQUEST,
                packet.packet_id.ok_or_else(|| "MQTT request requires packet_id".to_owned())?,
                &packet.payload,
            )?,
        ),
        MqttPacketKind::Response => (
            MQTT_PACKET_TYPE_PUBLISH << 4,
            encode_publish_body(
                MQTT_TOPIC_RESPONSE,
                packet
                    .packet_id
                    .ok_or_else(|| "MQTT response requires packet_id".to_owned())?,
                &packet.payload,
            )?,
        ),
        MqttPacketKind::Notification => (
            MQTT_PACKET_TYPE_PUBLISH << 4,
            encode_publish_body(MQTT_TOPIC_NOTIFICATION, 0, &packet.payload)?,
        ),
        MqttPacketKind::Ack => (
            MQTT_PACKET_TYPE_PUBACK << 4,
            packet
                .packet_id
                .ok_or_else(|| "MQTT ack requires packet_id".to_owned())?
                .to_be_bytes()
                .to_vec(),
        ),
        MqttPacketKind::Ping => (MQTT_PACKET_TYPE_PINGREQ << 4, Vec::new()),
        MqttPacketKind::Pong => (MQTT_PACKET_TYPE_PINGRESP << 4, Vec::new()),
    };
    let mut bytes = vec![header];
    encode_remaining_length(body.len(), &mut bytes)?;
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub fn decode_mqtt_packet(packet: &[u8]) -> Result<MqttPacket, String> {
    if packet.is_empty() {
        return Err("MQTT packet too short".to_owned());
    }
    let packet_type = packet[0] >> 4;
    let (remaining_len, consumed) = decode_remaining_length(&packet[1..])?;
    let body = &packet[1 + consumed..];
    if body.len() != remaining_len {
        return Err(format!(
            "MQTT remaining length mismatch (header {remaining_len}, actual {})",
            body.len()
        ));
    }
    match packet_type {
        MQTT_PACKET_TYPE_PUBLISH => decode_publish_packet(body),
        MQTT_PACKET_TYPE_PUBACK => {
            if body.len() != 2 {
                return Err("MQTT PUBACK must contain exactly one packet id".to_owned());
            }
            Ok(MqttPacket::ack(u16::from_be_bytes([body[0], body[1]])))
        }
        MQTT_PACKET_TYPE_PINGREQ => Ok(MqttPacket::ping()),
        MQTT_PACKET_TYPE_PINGRESP => Ok(MqttPacket::pong()),
        _ => Err(format!("unsupported MQTT packet type {packet_type}")),
    }
}

fn encode_publish_body(topic: &str, packet_id: u16, payload: &[u8]) -> Result<Vec<u8>, String> {
    if topic.len() > u16::MAX as usize {
        return Err("MQTT topic too long".to_owned());
    }
    let mut body = Vec::with_capacity(4 + topic.len() + payload.len());
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(payload);
    Ok(body)
}

fn decode_publish_packet(body: &[u8]) -> Result<MqttPacket, String> {
    if body.len() < 4 {
        return Err("MQTT PUBLISH body too short".to_owned());
    }
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + topic_len + 2 {
        return Err("MQTT PUBLISH body truncated".to_owned());
    }
    let topic = std::str::from_utf8(&body[2..2 + topic_len])
        .map_err(|error| format!("invalid MQTT topic utf-8: {error}"))?;
    let packet_id_offset = 2 + topic_len;
    let packet_id = u16::from_be_bytes([body[packet_id_offset], body[packet_id_offset + 1]]);
    let payload = body[packet_id_offset + 2..].to_vec();
    match topic {
        MQTT_TOPIC_REQUEST => Ok(MqttPacket::request(packet_id, payload)),
        MQTT_TOPIC_RESPONSE => Ok(MqttPacket::response(packet_id, payload)),
        MQTT_TOPIC_NOTIFICATION => Ok(MqttPacket::notification(payload)),
        _ => Err(format!("unsupported MQTT topic {topic}")),
    }
}

fn encode_remaining_length(mut len: usize, out: &mut Vec<u8>) -> Result<(), String> {
    if len > 268_435_455 {
        return Err("MQTT remaining length exceeds protocol maximum".to_owned());
    }
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    Ok(())
}

fn decode_remaining_length(bytes: &[u8]) -> Result<(usize, usize), String> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    for (index, byte) in bytes.iter().copied().enumerate().take(4) {
        value += ((byte & 0x7f) as usize) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        multiplier *= 128;
    }
    Err("invalid MQTT remaining length".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_publish_roundtrip() {
        let packet = MqttPacket::request(7, br#"{"jsonrpc":"2.0"}"#.to_vec());
        let encoded = encode_mqtt_packet(&packet).unwrap();
        assert_eq!(decode_mqtt_packet(&encoded).unwrap(), packet);
    }
}
