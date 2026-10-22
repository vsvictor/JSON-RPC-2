//! Dedicated WebSocket-over-QUIC adapter.

use std::{io, sync::Arc, time::Instant};

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
    WebSocketTransportFuture,
};
use futures_util::{SinkExt, StreamExt};
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    QUIC_NO_ERROR, TransportRuntime, log_control, log_json_rpc, log_transport_error,
};
use quinn::{Connection, Endpoint};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::protocol::{CloseFrame, Message, Role, WebSocketConfig, frame::coding::CloseCode},
};

const MAX_WS_MESSAGE_BYTES: usize = MAX_MESSAGE_BYTES;
const DUPLEX_BUFFER_BYTES: usize = 64 * 1024;

/// Dedicated WebSocket-over-QUIC transport without an HTTP/3 CONNECT layer.
pub struct QuicWebSocketTransport {
    endpoint: Endpoint,
    runtime: Arc<TransportRuntime>,
}

impl QuicWebSocketTransport {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl WebSocketTransport for QuicWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        Box::pin(serve(self.endpoint, protocol, shutdown, self.runtime))
    }
}

pub async fn serve(
    endpoint: Endpoint,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let connections = multiplexor.handle();
    loop {
        let incoming = tokio::select! {
            incoming = endpoint.accept() => match incoming {
                Some(incoming) => incoming,
                None => break,
            },
            _ = shutdown.cancelled() => break,
        };
        let protocol = protocol.clone();
        let shutdown = shutdown.clone();
        let runtime = runtime.clone();
        connections
            .spawn(async move {
                if let Ok(connection) = incoming.await {
                    let peer = connection.remote_address();
                    serve_connection(connection, peer, protocol, shutdown, runtime).await;
                }
            })
            .await?;
    }
    endpoint.set_server_config(None);
    shutdown.cancel();
    multiplexor.close().await;
    endpoint.wait_idle().await;
    Ok(())
}

async fn serve_connection(
    connection: Connection,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let sessions = multiplexor.handle();
    loop {
        let accepted = tokio::select! {
            accepted = connection.accept_bi() => accepted,
            _ = shutdown.cancelled() => break,
        };
        let Ok((send_stream, recv_stream)) = accepted else {
            break;
        };
        let protocol = protocol.clone();
        let shutdown = shutdown.clone();
        let runtime = runtime.clone();
        if sessions
            .spawn(async move {
                if let Err(error) =
                    handle_stream(send_stream, recv_stream, peer, protocol, shutdown, runtime).await
                {
                    log_transport_error("WebSocket/QUIC", peer, "stream", error.as_ref());
                }
            })
            .await
            .is_err()
        {
            break;
        }
    }
    multiplexor.close().await;
    connection.close(QUIC_NO_ERROR.into(), b"server shutdown");
}

async fn handle_stream(
    mut send_stream: quinn::SendStream,
    mut recv_stream: quinn::RecvStream,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_io, bridge_io) = tokio::io::duplex(DUPLEX_BUFFER_BYTES);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge_io);
    let mut incoming = tokio::spawn(async move {
        while let Some(chunk) = recv_stream.read_chunk(16 * 1024, true).await? {
            if bridge_writer.write_all(&chunk.bytes).await.is_err() {
                continue;
            }
        }
        let _ = bridge_writer.shutdown().await;
        Ok::<(), io::Error>(())
    });
    let outgoing = tokio::spawn(async move {
        let mut buffer = [0; 16 * 1024];
        loop {
            let count = bridge_reader.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            send_stream.write_all(&buffer[..count]).await?;
        }
        send_stream.finish()?;
        let _ = send_stream.stopped().await;
        Ok::<(), io::Error>(())
    });

    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WS_MESSAGE_BYTES));
    let websocket = WebSocketStream::from_raw_socket(ws_io, Role::Server, Some(config)).await;
    run_websocket_session(websocket, peer, protocol, shutdown, runtime.clone()).await;
    let _ = outgoing.await;
    if tokio::time::timeout(runtime.close_timeout(), &mut incoming)
        .await
        .is_err()
    {
        incoming.abort();
    }
    Ok(())
}

async fn run_websocket_session(
    mut websocket: WebSocketStream<DuplexStream>,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) {
    loop {
        let message = tokio::select! {
            message = websocket.next() => message,
            _ = shutdown.cancelled() => {
                close_session(
                    websocket,
                    CloseCode::Away,
                    "server shutdown",
                    runtime.close_timeout(),
                )
                .await;
                return;
            }
        };
        let message = match message {
            Some(Ok(message)) => message,
            None => return,
            Some(Err(error)) => {
                log_transport_error("WebSocket/QUIC", peer, "frame", &error);
                return;
            }
        };
        let request = match message {
            Message::Text(text) => {
                if text.len() > MAX_WS_MESSAGE_BYTES {
                    close_session(
                        websocket,
                        CloseCode::Size,
                        "message too large",
                        runtime.close_timeout(),
                    )
                    .await;
                    return;
                }
                WebSocketMessage::Text(text.to_string())
            }
            Message::Binary(data) => {
                if data.len() > MAX_WS_MESSAGE_BYTES {
                    close_session(
                        websocket,
                        CloseCode::Size,
                        "message too large",
                        runtime.close_timeout(),
                    )
                    .await;
                    return;
                }
                WebSocketMessage::Binary(data)
            }
            Message::Ping(payload) => {
                log_control("WebSocket/QUIC", peer, "ping", "pong");
                if websocket.send(Message::Pong(payload)).await.is_err() {
                    return;
                }
                continue;
            }
            Message::Pong(_) | Message::Close(_) | Message::Frame(_) => continue,
        };

        let request_bytes = super::message_bytes_for_log(&request);
        let started = Instant::now();
        if let Some(response) =
            super::handle_stateless_message(protocol.as_ref(), request, "ws-quic").await
        {
            let response = match response {
                WebSocketMessage::Text(text) => {
                    log_json_rpc(
                        "WebSocket/QUIC",
                        peer,
                        &request_bytes,
                        Some(text.as_bytes()),
                        started.elapsed(),
                    );
                    Message::Text(text.into())
                }
                WebSocketMessage::Binary(data) => {
                    log_json_rpc(
                        "WebSocket/QUIC",
                        peer,
                        &request_bytes,
                        Some(data.as_ref()),
                        started.elapsed(),
                    );
                    Message::Binary(data)
                }
            };
            if websocket.send(response).await.is_err() {
                return;
            }
        } else {
            log_json_rpc(
                "WebSocket/QUIC",
                peer,
                &request_bytes,
                None,
                started.elapsed(),
            );
        }
    }
}

async fn close_session(
    mut websocket: WebSocketStream<DuplexStream>,
    code: CloseCode,
    reason: &'static str,
    timeout: std::time::Duration,
) {
    let frame = CloseFrame {
        code,
        reason: reason.into(),
    };
    if websocket.close(Some(frame)).await.is_err() {
        return;
    }
    let _ = tokio::time::timeout(timeout, async {
        while let Some(Ok(_)) = websocket.next().await {}
    })
    .await;
}
