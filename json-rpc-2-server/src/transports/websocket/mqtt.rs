//! WebSocket-style framing over MQTT-style messages.

use std::{io, sync::Arc, time::Instant};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::{
    CancellationToken, WebSocketProtocol, WebSocketTransport, WebSocketTransportFuture,
};
use crate::transports::{
    TransportRuntime, apply_tcp_stream_tuning, log_control, log_json_rpc, log_transport_error,
    multiplex::StreamMultiplexor,
};
use json_rpc_2_core::{MqttPacket, MqttPacketKind, decode_mqtt_packet, decode_websocket_message, encode_mqtt_packet, encode_websocket_message, websocket_message_bytes};

pub struct MqttWebSocketTransport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

impl MqttWebSocketTransport {
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

impl WebSocketTransport for MqttWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        Box::pin(serve(self.listener, protocol, shutdown, self.runtime))
    }
}

pub async fn serve(
    listener: TcpListener,
    protocol: Arc<dyn WebSocketProtocol>,
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
                    log_transport_error("WebSocket/MQTT", "listener", "connection", &error);
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
    protocol: Arc<dyn WebSocketProtocol>,
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
                log_control("WebSocket/MQTT", peer, "ping", "pong");
                write_packet(&mut stream, &MqttPacket::pong()).await?;
            }
            MqttPacketKind::Request | MqttPacketKind::Notification => {
                let Some(message) = decode_websocket_message(&packet.payload)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                else {
                    continue;
                };
                let request_bytes = websocket_message_bytes(&message).to_vec();
                let started = Instant::now();
                let response = protocol.handle_message(message).await;
                let response_bytes = response.as_ref().map(websocket_message_bytes).map(|bytes| bytes.to_vec());
                log_json_rpc("WebSocket/MQTT", peer, &request_bytes, response_bytes.as_deref(), started.elapsed());
                if let (Some(response), Some(packet_id)) = (response, packet.packet_id) {
                    let encoded = encode_websocket_message(&response).map_err(io::Error::other)?;
                    write_packet(&mut stream, &MqttPacket::response(packet_id, encoded)).await?;
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
    let (remaining, consumed) = super::super::mqtt::decode_remaining_length(&length_bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut body = vec![0u8; remaining];
    stream.read_exact(&mut body).await?;
    let mut packet = Vec::with_capacity(1 + consumed + remaining);
    packet.extend_from_slice(&first);
    packet.extend_from_slice(&length_bytes[..consumed]);
    packet.extend_from_slice(&body);
    decode_mqtt_packet(&packet).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
