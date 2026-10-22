//! WebSocket-over-HTTP/1.1 Upgrade adapter.
//!
//! Each upgraded connection owns a persistent context and receives protocol push
//! on that same WebSocket. Close removes the context before flushing its reply.

use std::{io, sync::Arc, time::Instant};

use axum::{
    Router,
    extract::{
        ConnectInfo, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    routing::get,
};
use futures_util::StreamExt;
use tokio::net::TcpListener;
#[cfg(feature = "tls")]
use tokio::task::JoinSet;
#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketSessionGuard,
    WebSocketTransport, WebSocketTransportFuture,
};
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::multiplex::{StreamMultiplexor, StreamMultiplexorHandle};
use json_rpc_2_transport::{
    TransportRuntime, TransportSession, log_control, log_json_rpc, log_transport_error,
};

/// WebSocket transport established using the HTTP/1.1 Upgrade mechanism.
pub struct Http1WebSocketTransport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

#[cfg(feature = "tls")]
/// WebSocket-over-HTTP/1.1 Upgrade served over a verified TLS connection.
pub struct TlsWebSocketTransport {
    listener: TcpListener,
    tls: Arc<rustls::ServerConfig>,
    runtime: Arc<TransportRuntime>,
}

#[cfg(feature = "tls")]
impl TlsWebSocketTransport {
    pub fn new(listener: TcpListener, tls: Arc<rustls::ServerConfig>) -> Self {
        Self {
            listener,
            tls,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

#[cfg(feature = "tls")]
impl WebSocketTransport for TlsWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        Box::pin(Http1WebSocketTransport::serve_tls(
            self.listener,
            protocol,
            shutdown,
            self.runtime,
            self.tls,
        ))
    }
}

impl Http1WebSocketTransport {
    pub fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    #[cfg(feature = "tls")]
    async fn serve_tls(
        listener: TcpListener,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
        runtime: Arc<TransportRuntime>,
        tls: Arc<rustls::ServerConfig>,
    ) -> io::Result<()> {
        let multiplexor =
            StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
        let router = router_with_sessions(
            protocol,
            multiplexor.handle(),
            shutdown.clone(),
            Arc::clone(&runtime),
        );
        let acceptor = TlsAcceptor::from(tls);
        let mut connections = JoinSet::new();

        let result = loop {
            tokio::select! {
                _ = shutdown.cancelled() => break Ok(()),
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    let acceptor = acceptor.clone();
                    let router = router.clone();
                    connections.spawn(async move {
                        let stream = match acceptor.accept(stream).await {
                            Ok(stream) => stream,
                            Err(error) => {
                                log_transport_error("WebSocket/TLS", peer, "TLS handshake", &error);
                                return;
                            }
                        };
                        let service = tower::service_fn(move |mut request: hyper::Request<hyper::body::Incoming>| {
                            let router = router.clone();
                            request.extensions_mut().insert(ConnectInfo(peer));
                            async move {
                                use tower::ServiceExt;
                                router.oneshot(request.map(axum::body::Body::new)).await
                            }
                        });
                        let service = hyper_util::service::TowerToHyperService::new(service);
                        if let Err(error) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                            .with_upgrades()
                            .await
                        {
                            log_transport_error("WebSocket/TLS", peer, "HTTP/1.1 connection", &error);
                        }
                    });
                }
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(error) = result {
                        log_transport_error("WebSocket/TLS", "server", "connection task", &error);
                    }
                }
            }
        };

        shutdown.cancel();
        multiplexor.close().await;
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        result
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
    let mut session = TransportSession::new("ws-http1", true);
    let mut guard = Some(WebSocketSessionGuard::new(
        protocol.clone(),
        session.context.clone(),
    ));
    loop {
        let message = tokio::select! {
            message = socket.next() => message,
            payload = session.push_receiver.recv() => {
                let Some(payload) = payload else { return };
                if payload.len() > MAX_MESSAGE_BYTES {
                    guard.take();
                    close_session(socket, close_code::SIZE, "push message too large", runtime.close_timeout()).await;
                    return;
                }
                let message = if json_rpc_2_transport::gateway::is_gateway_frame(&payload) {
                    Message::Binary(payload)
                } else {
                    match std::str::from_utf8(&payload) {
                        Ok(text) => Message::Text(text.to_owned().into()),
                        Err(_) => Message::Binary(payload),
                    }
                };
                if let Err(error) = socket.send(message).await {
                    log_transport_error("WebSocket/HTTP/1.1", peer, "push", &error);
                    return;
                }
                log_control("WebSocket/HTTP/1.1", peer, "server_push", "sent");
                continue;
            }
            _ = shutdown.cancelled() => {
                guard.take();
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
        let message = match message {
            Some(Ok(message)) => message,
            None => return,
            Some(Err(error)) => {
                log_transport_error("WebSocket/HTTP/1.1", peer, "frame", &error);
                return;
            }
        };
        let request = match message {
            Message::Text(text) => {
                if text.len() > MAX_MESSAGE_BYTES {
                    guard.take();
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
                    guard.take();
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
                if let Err(error) = socket.send(Message::Pong(data)).await {
                    log_transport_error("WebSocket/HTTP/1.1", peer, "pong", &error);
                    return;
                }
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => {
                guard.take();
                if let Err(error) = futures_util::SinkExt::flush(&mut socket).await {
                    log_transport_error("WebSocket/HTTP/1.1", peer, "close reply", &error);
                }
                return;
            }
        };
        let request_bytes = super::message_bytes_for_log(&request);
        let started = Instant::now();
        let response = match protocol
            .handle_message_with_context(request, &session.context)
            .await
        {
            Some(WebSocketMessage::Text(text)) => {
                log_json_rpc(
                    "WebSocket/HTTP/1.1",
                    peer,
                    &request_bytes,
                    Some(text.as_bytes()),
                    started.elapsed(),
                );
                Message::Text(text.into())
            }
            Some(WebSocketMessage::Binary(data)) => {
                log_json_rpc(
                    "WebSocket/HTTP/1.1",
                    peer,
                    &request_bytes,
                    Some(data.as_ref()),
                    started.elapsed(),
                );
                Message::Binary(data)
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
        if let Err(error) = socket.send(response).await {
            log_transport_error("WebSocket/HTTP/1.1", peer, "response", &error);
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
    if let Err(error) = socket.send(Message::Close(Some(frame))).await {
        log_transport_error("WebSocket/HTTP/1.1", "session", "close", &error);
        return;
    }
    if let Err(error) = tokio::time::timeout(timeout, async {
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) => break,
                Err(error) => {
                    log_transport_error("WebSocket/HTTP/1.1", "session", "close reply", &error);
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    {
        log_transport_error("WebSocket/HTTP/1.1", "session", "close timeout", &error);
    }
}
