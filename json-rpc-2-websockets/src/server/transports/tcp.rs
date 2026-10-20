//! Dedicated WebSocket-over-TCP adapter.

use std::{io, sync::Arc, time::Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::protocol::{CloseFrame, Message, Role, WebSocketConfig, frame::coding::CloseCode},
};

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
    WebSocketTransportFuture,
};
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    TransportRuntime, apply_tcp_stream_tuning, log_control, log_json_rpc, log_transport_error,
};

const MAX_WS_MESSAGE_BYTES: usize = MAX_MESSAGE_BYTES;

/// Dedicated WebSocket-over-TCP transport without an HTTP upgrade layer.
pub struct TcpWebSocketTransport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

impl TcpWebSocketTransport {
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

impl WebSocketTransport for TcpWebSocketTransport {
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
                    log_transport_error("WebSocket/TCP", "listener", "connection", &error);
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
    stream: TcpStream,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    apply_tcp_stream_tuning(&stream, &runtime.tcp_tuning)?;
    let peer = stream.peer_addr()?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WS_MESSAGE_BYTES));
    let websocket = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
    run_websocket_session(websocket, peer, protocol, shutdown, runtime).await;
    Ok(())
}

async fn run_websocket_session(
    mut websocket: WebSocketStream<TcpStream>,
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
                log_transport_error("WebSocket/TCP", peer, "frame", &error);
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
                WebSocketMessage::Binary(Bytes::from(data.to_vec()))
            }
            Message::Ping(payload) => {
                log_control("WebSocket/TCP", peer, "ping", "pong");
                if websocket.send(Message::Pong(payload)).await.is_err() {
                    return;
                }
                continue;
            }
            Message::Pong(_) | Message::Close(_) | Message::Frame(_) => continue,
        };

        let request_bytes = match &request {
            WebSocketMessage::Text(text) => text.as_bytes().to_vec(),
            WebSocketMessage::Binary(data) => data.to_vec(),
        };
        let started = Instant::now();
        if let Some(response) = protocol.handle_message(request).await {
            let response = match response {
                WebSocketMessage::Text(text) => {
                    let response_bytes = text.as_bytes().to_vec();
                    log_json_rpc(
                        "WebSocket/TCP",
                        peer,
                        &request_bytes,
                        Some(&response_bytes),
                        started.elapsed(),
                    );
                    Message::Text(text.into())
                }
                WebSocketMessage::Binary(data) => {
                    log_json_rpc(
                        "WebSocket/TCP",
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
                "WebSocket/TCP",
                peer,
                &request_bytes,
                None,
                started.elapsed(),
            );
        }
    }
}

async fn close_session(
    mut websocket: WebSocketStream<TcpStream>,
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
