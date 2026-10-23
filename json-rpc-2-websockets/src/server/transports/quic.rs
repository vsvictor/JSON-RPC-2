//! Dedicated WebSocket-over-QUIC adapter.
//!
//! Persistent identity and native WebSocket push belong to each bidirectional
//! stream and remain unchanged when the QUIC connection migrates.

use std::{io, sync::Arc, time::Instant};

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketSessionGuard,
    WebSocketTransport, WebSocketTransportFuture,
};
use futures_util::{SinkExt, StreamExt};
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    QUIC_NO_ERROR, TransportRuntime, TransportSession, log_control, log_json_rpc,
    log_transport_error,
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

/// Serve an accepted connection; the caller owns endpoint admission and shutdown.
pub async fn serve_connection(
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
            bridge_writer.write_all(&chunk.bytes).await?;
        }
        let _ = bridge_writer.shutdown().await;
        Ok::<(), io::Error>(())
    });
    let mut outgoing = tokio::spawn(async move {
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
    for (task, operation) in [
        (&mut outgoing, "send-bridge"),
        (&mut incoming, "recv-bridge"),
    ] {
        match tokio::time::timeout(runtime.close_timeout(), &mut *task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => log_transport_error("WebSocket/QUIC", peer, operation, &error),
            Ok(Err(error)) => log_transport_error("WebSocket/QUIC", peer, operation, &error),
            Err(error) => {
                log_transport_error("WebSocket/QUIC", peer, operation, &error);
                task.abort();
                let _ = task.await;
            }
        }
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
    // A session belongs to this logical WebSocket stream, not its migratable peer address.
    let mut session = TransportSession::new("ws-quic", true);
    let mut guard = Some(WebSocketSessionGuard::new(
        protocol.clone(),
        session.context.clone(),
    ));
    loop {
        let message = tokio::select! {
            message = websocket.next() => message,
            payload = session.push_receiver.recv() => {
                let Some(payload) = payload else { return };
                if payload.len() > MAX_WS_MESSAGE_BYTES {
                    guard.take();
                    close_session(websocket, CloseCode::Size, "push message too large", runtime.close_timeout()).await;
                    return;
                }
                let message = match std::str::from_utf8(&payload) {
                        Ok(text) => Message::Text(text.to_owned().into()),
                        Err(_) => Message::Binary(payload),
                    };
                if let Err(error) = websocket.send(message).await {
                    log_transport_error("WebSocket/QUIC", peer, "push", &error);
                    return;
                }
                log_control("WebSocket/QUIC", peer, "server_push", "sent");
                continue;
            }
            _ = shutdown.cancelled() => {
                guard.take();
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
                    guard.take();
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
                    guard.take();
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
                if let Err(error) = websocket.send(Message::Pong(payload)).await {
                    log_transport_error("WebSocket/QUIC", peer, "pong", &error);
                    return;
                }
                continue;
            }
            Message::Close(_) => {
                guard.take();
                if let Err(error) = websocket.flush().await {
                    log_transport_error("WebSocket/QUIC", peer, "close reply", &error);
                }
                return;
            }
            Message::Pong(_) | Message::Frame(_) => continue,
        };

        let request_bytes = super::message_bytes_for_log(&request);
        let started = Instant::now();
        if let Some(response) = protocol
            .handle_message_with_context(request, &session.context)
            .await
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
                WebSocketMessage::Ping(data) => Message::Ping(data),
                WebSocketMessage::Pong(data) => Message::Pong(data),
            };
            if let Err(error) = websocket.send(response).await {
                log_transport_error("WebSocket/QUIC", peer, "response", &error);
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
    if let Err(error) = websocket.close(Some(frame)).await {
        log_transport_error("WebSocket/QUIC", "session", "close", &error);
        return;
    }
    if let Err(error) = tokio::time::timeout(timeout, async {
        while let Some(message) = websocket.next().await {
            match message {
                Ok(Message::Close(_)) => break,
                Err(error) => {
                    log_transport_error("WebSocket/QUIC", "session", "close reply", &error);
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    {
        log_transport_error("WebSocket/QUIC", "session", "close timeout", &error);
    }
}
