//! JSON-RPC over MQTT-style framed messages.

use std::{io, sync::Arc, time::Instant};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::{
    CancellationToken, Transport, TransportFuture, TransportRuntime, apply_tcp_stream_tuning,
    log_control, log_json_rpc, log_transport_error, multiplex::StreamMultiplexor,
};
use crate::JsonRpcProtocol;
use json_rpc_2_core::{MqttPacket, MqttPacketKind, decode_mqtt_packet, encode_mqtt_packet};

/// MQTT-inspired transport using PUBLISH/PING packets as the carrier.
pub struct MqttTransport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

impl MqttTransport {
    pub fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl Transport for MqttTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.listener, protocol, shutdown, self.runtime))
    }
}

pub async fn serve(
    listener: TcpListener,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let connections = multiplexor.handle();
    let result = loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => break Err(error),
            },
            _ = shutdown.cancelled() => break Ok(()),
        };
        let protocol = protocol.clone();
        let shutdown = shutdown.clone();
        let runtime = runtime.clone();
        connections
            .spawn(async move {
                if let Err(error) = handle_connection(stream, protocol, shutdown, runtime).await {
                    log_transport_error("MQTT", "listener", "connection", &error);
                }
            })
            .await?;
    };
    drop(listener);
    shutdown.cancel();
    multiplexor.close().await;
    result
}

async fn handle_connection(
    mut stream: TcpStream,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    apply_tcp_stream_tuning(&stream, &runtime.tcp_tuning)?;
    let peer = stream.peer_addr()?;
    loop {
        let packet = tokio::select! {
            packet = read_packet(&mut stream) => packet,
            _ = shutdown.cancelled() => break,
        };
        let packet = match packet {
            Ok(packet) => packet,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        };
        match packet.kind {
            MqttPacketKind::Ping => {
                log_control("MQTT", peer, "ping", "pong");
                write_packet(&mut stream, &MqttPacket::pong()).await?;
            }
            MqttPacketKind::Request | MqttPacketKind::Notification => {
                let started = Instant::now();
                let response = protocol.handle_message(&packet.payload).await;
                log_json_rpc(
                    "MQTT",
                    peer,
                    &packet.payload,
                    response.as_deref(),
                    started.elapsed(),
                );
                if let (Some(response), Some(packet_id)) = (response, packet.packet_id) {
                    write_packet(&mut stream, &MqttPacket::response(packet_id, response.to_vec()))
                        .await?;
                }
            }
            _ => {}
        }
    }
    match stream.shutdown().await {
        Err(error) if error.kind() != io::ErrorKind::NotConnected => Err(error),
        _ => Ok(()),
    }
}

async fn write_packet(stream: &mut TcpStream, packet: &MqttPacket) -> io::Result<()> {
    let encoded = encode_mqtt_packet(packet).map_err(io::Error::other)?;
    stream.write_all(&encoded).await
}

async fn read_packet(stream: &mut TcpStream) -> io::Result<MqttPacket> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await?;
    let mut length_bytes = Vec::new();
    for _ in 0..4 {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        length_bytes.push(byte[0]);
        if byte[0] & 0x80 == 0 {
            break;
        }
    }
    let (remaining, consumed) =
        decode_remaining_length(&length_bytes).map_err(invalid_data)?;
    let mut body = vec![0u8; remaining];
    stream.read_exact(&mut body).await?;
    let mut packet = Vec::with_capacity(1 + consumed + remaining);
    packet.extend_from_slice(&first);
    packet.extend_from_slice(&length_bytes[..consumed]);
    packet.extend_from_slice(&body);
    decode_mqtt_packet(&packet).map_err(invalid_data)
}

fn invalid_data(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

pub(crate) fn decode_remaining_length(bytes: &[u8]) -> Result<(usize, usize), String> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        value += ((byte & 0x7f) as usize) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        multiplier *= 128;
    }
    Err("invalid MQTT remaining length".to_owned())
}
