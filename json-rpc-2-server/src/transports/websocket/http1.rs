//! WebSocket-over-HTTP/1.1 Upgrade adapter.

use std::{io, sync::Arc, time::Instant};

use axum::{
    Router,
    body::Bytes,
    extract::{
        ConnectInfo, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    routing::get,
};
use futures_util::StreamExt;
use tokio::net::TcpListener;

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
    WebSocketTransportFuture,
};
use crate::{
    MAX_MESSAGE_BYTES,
    transports::{
        TransportRuntime, log_control, log_json_rpc, log_transport_error,
        multiplex::{StreamMultiplexor, StreamMultiplexorHandle},
    },
};

/// WebSocket transport established using the HTTP/1.1 Upgrade mechanism.
pub struct Http1WebSocketTransport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

impl Http1WebSocketTransport {
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

impl WebSocketTransport for Http1WebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        Box::pin(serve(self.listener, protocol, shutdown, self.runtime))
    }
}

#[derive(Clone)]
struct SessionState {
    protocol: Arc<dyn WebSocketProtocol>,
    sessions: StreamMultiplexorHandle,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
}

/// Build a WebSocket-over-HTTP/1.1 router, exposing `GET /ws`.
///
/// Sessions are closed with 1001 Going Away once `shutdown` is cancelled.
pub fn router(protocol: Arc<dyn WebSocketProtocol>, shutdown: CancellationToken) -> Router {
    let runtime = Arc::new(TransportRuntime::default());
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    router_with_sessions(protocol, multiplexor.handle(), shutdown, runtime)
}

fn router_with_sessions(
    protocol: Arc<dyn WebSocketProtocol>,
    sessions: StreamMultiplexorHandle,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> Router {
    Router::new()
        .route("/ws", get(websocket_upgrade))
        .with_state(SessionState {
            protocol,
            sessions,
            shutdown,
            runtime,
        })
}

/// Serve WebSocket connections established using HTTP/1.1 Upgrade.
///
/// Upgraded connections are no longer owned by the HTTP server, so their
/// sessions are tracked separately and awaited during shutdown.
pub async fn serve(
    listener: TcpListener,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let router = router_with_sessions(protocol, multiplexor.handle(), shutdown.clone(), runtime);
    let result = axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown.clone().cancelled_owned())
    .await;
    shutdown.cancel();
    multiplexor.close().await;
    result
}

async fn websocket_upgrade(
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    State(state): State<SessionState>,
    upgrade: WebSocketUpgrade,
) -> axum::response::Response {
    let sessions = state.sessions.clone();
    upgrade.on_upgrade(move |socket| async move {
        if let Err(error) = sessions
            .spawn(async move {
                websocket_session(socket, peer, state.protocol, state.shutdown, state.runtime)
                    .await;
            })
            .await
        {
            log_transport_error("WebSocket/HTTP/1.1", peer, "upgrade", &error);
        }
    })
}

async fn websocket_session(
    mut socket: WebSocket,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) {
    loop {
        let message = tokio::select! {
            message = socket.next() => message,
            _ = shutdown.cancelled() => {
                close_session(
                    socket,
                    close_code::AWAY,
                    "server shutdown",
                    runtime.close_timeout(),
                )
                .await;
                return;
            }
        };
        let Some(Ok(message)) = message else { return };
        let request = match message {
            Message::Text(text) => {
                if text.len() > MAX_MESSAGE_BYTES {
                    close_session(
                        socket,
                        close_code::SIZE,
                        "message too large",
                        runtime.close_timeout(),
                    )
                    .await;
                    return;
                }
                WebSocketMessage::Text(text.to_string())
            }
            Message::Binary(data) => {
                if data.len() > MAX_MESSAGE_BYTES {
                    close_session(
                        socket,
                        close_code::SIZE,
                        "message too large",
                        runtime.close_timeout(),
                    )
                    .await;
                    return;
                }
                WebSocketMessage::Binary(data)
            }
            Message::Ping(data) => {
                log_control("WebSocket/HTTP/1.1", peer, "ping", "pong");
                if socket.send(Message::Pong(data)).await.is_err() {
                    return;
                }
                continue;
            }
            Message::Pong(_) => continue,
            // The WebSocket library queues the Close reply; the next read
            // flushes it and ends the stream, completing the handshake.
            Message::Close(_) => continue,
        };
        let request_bytes = match &request {
            WebSocketMessage::Text(text) => text.as_bytes().to_vec(),
            WebSocketMessage::Binary(data) => data.to_vec(),
        };
        let started = Instant::now();
        let response = match protocol.handle_message(request).await {
            Some(WebSocketMessage::Text(text)) => {
                let response_bytes = text.as_bytes().to_vec();
                let response = Message::Text(text.into());
                log_json_rpc(
                    "WebSocket/HTTP/1.1",
                    peer,
                    &request_bytes,
                    Some(&response_bytes),
                    started.elapsed(),
                );
                response
            }
            Some(WebSocketMessage::Binary(data)) => {
                log_json_rpc(
                    "WebSocket/HTTP/1.1",
                    peer,
                    &request_bytes,
                    Some(data.as_ref()),
                    started.elapsed(),
                );
                Message::Binary(Bytes::from(data))
            }
            None => {
                log_json_rpc(
                    "WebSocket/HTTP/1.1",
                    peer,
                    &request_bytes,
                    None,
                    started.elapsed(),
                );
                continue;
            }
        };
        if socket.send(response).await.is_err() {
            return;
        }
    }
}

/// Server-initiated closing handshake: send Close, then wait (bounded) for
/// the client's Close reply.
async fn close_session(
    mut socket: WebSocket,
    code: u16,
    reason: &'static str,
    timeout: std::time::Duration,
) {
    let frame = CloseFrame {
        code,
        reason: reason.into(),
    };
    if socket.send(Message::Close(Some(frame))).await.is_err() {
        return;
    }
    let _ = tokio::time::timeout(timeout, async {
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    })
    .await;
}
