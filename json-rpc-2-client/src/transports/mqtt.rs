//! JSON-RPC over MQTT-style framed messages on a persistent TCP stream.

use std::{
    io,
    net::SocketAddr,
    sync::atomic::{AtomicU32, Ordering},
};

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use super::{closed, transport_error};
use crate::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, expects_response,
};
use json_rpc_2_core::{MqttPacket, TcpTuningConfig, decode_mqtt_packet, encode_mqtt_packet};

/// MQTT-inspired client transport carrying one JSON-RPC message per publish frame.
pub struct MqttClientTransport {
    address: SocketAddr,
    tuning: TcpTuningConfig,
    next_packet_id: AtomicU32,
    connection: Mutex<Option<TcpStream>>,
}

impl MqttClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_tuning(address, TcpTuningConfig::default()).await
    }

    pub async fn connect_with_tuning(
        address: SocketAddr,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            address,
            tuning: tuning.clone(),
            next_packet_id: AtomicU32::new(1),
            connection: Mutex::new(Some(Self::open(address, &tuning).await?)),
        })
    }

    async fn open(address: SocketAddr, tuning: &TcpTuningConfig) -> Result<TcpStream, TransportError> {
        super::connect_tcp_stream(address, tuning).await
    }

    fn next_packet_id(&self) -> u16 {
        self.next_packet_id.fetch_add(1, Ordering::Relaxed) as u16
    }
}

impl RpcClientTransport for MqttClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects = expects_response(&message);
            self.send_with_expectation(message, expects).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let packet = if expects_response {
                MqttPacket::request(self.next_packet_id(), message.to_vec())
            } else {
                MqttPacket::notification(message.to_vec())
            };
            let mut connection = self.connection.lock().await;
            let stream = connection.as_mut().ok_or_else(closed)?;
            write_packet(stream, &packet).await?;
            if !expects_response {
                return Ok(None);
            }
            loop {
                match read_packet(stream).await? {
                    MqttPacket {
                        kind: json_rpc_2_core::MqttPacketKind::Response,
                        payload,
                        ..
                    } => return Ok(Some(Bytes::from(payload))),
                    MqttPacket {
                        kind: json_rpc_2_core::MqttPacketKind::Ping,
                        ..
                    } => write_packet(stream, &MqttPacket::pong()).await?,
                    _ => continue,
                }
            }
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            let stream = connection.as_mut().ok_or_else(closed)?;
            write_packet(stream, &MqttPacket::ping()).await?;
            loop {
                match read_packet(stream).await? {
                    MqttPacket {
                        kind: json_rpc_2_core::MqttPacketKind::Pong,
                        ..
                    } => return Ok(()),
                    MqttPacket {
                        kind: json_rpc_2_core::MqttPacketKind::Ping,
                        ..
                    } => write_packet(stream, &MqttPacket::pong()).await?,
                    _ => continue,
                }
            }
        })
    }

    fn kind(&self) -> &'static str {
        "mqtt"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = Self::open(self.address, &self.tuning).await?;
            let previous = self.connection.lock().await.replace(replacement);
            if let Some(mut stream) = previous {
                let _ = stream.shutdown().await;
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let Some(mut stream) = self.connection.lock().await.take() else {
                return Ok(());
            };
            stream.shutdown().await.map_err(transport_error)
        })
    }
}

async fn write_packet(stream: &mut TcpStream, packet: &MqttPacket) -> Result<(), TransportError> {
    let encoded = encode_mqtt_packet(packet).map_err(TransportError::new)?;
    stream.write_all(&encoded).await.map_err(transport_error)
}

async fn read_packet(stream: &mut TcpStream) -> Result<MqttPacket, TransportError> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await.map_err(transport_error)?;
    let mut length_bytes = Vec::new();
    for _ in 0..4 {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await.map_err(transport_error)?;
        length_bytes.push(byte[0]);
        if byte[0] & 0x80 == 0 {
            break;
        }
    }
    let (remaining, consumed) = decode_remaining_length(&length_bytes)?;
    let mut body = vec![0u8; remaining];
    stream.read_exact(&mut body).await.map_err(transport_error)?;
    let mut packet = Vec::with_capacity(1 + consumed + remaining);
    packet.extend_from_slice(&first);
    packet.extend_from_slice(&length_bytes[..consumed]);
    packet.extend_from_slice(&body);
    decode_mqtt_packet(&packet).map_err(TransportError::new)
}

fn decode_remaining_length(bytes: &[u8]) -> Result<(usize, usize), TransportError> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        value += ((byte & 0x7f) as usize) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        multiplier *= 128;
    }
    Err(transport_error(io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid MQTT remaining length",
    )))
}
