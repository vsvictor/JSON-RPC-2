//! WebSocket-style framing over AMQP-style body frames.

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
use json_rpc_2_core::{AmqpFrame, AmqpMessageKind, decode_amqp_frame, decode_websocket_message, encode_amqp_frame, encode_websocket_message, websocket_message_bytes};

pub struct AmqpWebSocketTransport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

impl AmqpWebSocketTransport {
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

impl WebSocketTransport for AmqpWebSocketTransport {
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
                    log_transport_error("WebSocket/AMQP", "listener", "connection", &error);
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
        let frame = tokio::select! {
            frame = read_frame(&mut stream) => frame,
            _ = shutdown.cancelled() => break,
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        };
        match frame.kind {
            None => {
                log_control("WebSocket/AMQP", peer, "heartbeat", "heartbeat");
                write_frame(&mut stream, &AmqpFrame::heartbeat()).await?;
            }
            Some(AmqpMessageKind::Request) | Some(AmqpMessageKind::Notification) => {
                let Some(message) = decode_websocket_message(&frame.payload)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                else {
                    continue;
                };
                let request_bytes = websocket_message_bytes(&message).to_vec();
                let started = Instant::now();
                let response = protocol.handle_message(message).await;
                let response_bytes = response.as_ref().map(websocket_message_bytes).map(|bytes| bytes.to_vec());
                log_json_rpc("WebSocket/AMQP", peer, &request_bytes, response_bytes.as_deref(), started.elapsed());
                if let Some(response) = response {
                    let encoded = encode_websocket_message(&response).map_err(io::Error::other)?;
                    write_frame(&mut stream, &AmqpFrame::response(frame.correlation_id, encoded)).await?;
                }
            }
            Some(AmqpMessageKind::Response) => {}
        }
    }
    match stream.shutdown().await {
        Err(error) if error.kind() != io::ErrorKind::NotConnected => Err(error),
        _ => Ok(()),
    }
}

async fn write_frame(stream: &mut TcpStream, frame: &AmqpFrame) -> io::Result<()> {
    let encoded = encode_amqp_frame(frame).map_err(io::Error::other)?;
    stream.write_all(&encoded).await
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<AmqpFrame> {
    let mut header = [0u8; 7];
    stream.read_exact(&mut header).await?;
    let payload_len = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    let mut tail = vec![0u8; payload_len + 1];
    stream.read_exact(&mut tail).await?;
    let mut packet = header.to_vec();
    packet.extend_from_slice(&tail);
    decode_amqp_frame(&packet).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
