//! Dedicated WebSocket-over-TCP adapter.

use std::{io, sync::Arc, time::Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::protocol::{CloseFrame, Message, Role, WebSocketConfig, frame::coding::CloseCode},
};

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketSessionGuard,
    WebSocketTransport, WebSocketTransportFuture,
};
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    TransportRuntime, TransportSession, apply_tcp_stream_tuning, log_control, log_json_rpc,
    log_transport_error,
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
    run_websocket_session(websocket, peer, protocol, shutdown, runtime, "ws-tcp").await;
    Ok(())
}

/// Serve a raw WebSocket session on an already accepted stream without an
/// HTTP upgrade. The caller owns admission, cancellation, and task tracking.
pub async fn serve_established_stream<S>(
    stream: S,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    serve_established_stream_with_kind(stream, peer, protocol, shutdown, runtime, "ws-tcp").await;
}

/// Variant of [`serve_established_stream`] for secure raw WebSocket sessions.
pub async fn serve_established_stream_with_kind<S>(
    stream: S,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
    kind: &'static str,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WS_MESSAGE_BYTES));
    let websocket = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
    run_websocket_session(websocket, peer, protocol, shutdown, runtime, kind).await;
}

async fn run_websocket_session<S>(
    mut websocket: WebSocketStream<S>,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
    kind: &'static str,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = TransportSession::new(kind, true);
    let _session_guard = WebSocketSessionGuard::new(Arc::clone(&protocol), session.context.clone());
    let serving = async {
        loop {
            let event = tokio::select! {
                message = websocket.next() => SessionEvent::Frame(message),
                payload = session.push_receiver.recv() => SessionEvent::Push(payload),
                _ = shutdown.cancelled() => SessionEvent::Shutdown,
            };
            let message = match event {
                SessionEvent::Shutdown => {
                    close_session(
                        websocket,
                        CloseCode::Away,
                        "server shutdown",
                        runtime.close_timeout(),
                    )
                    .await;
                    return;
                }
                SessionEvent::Push(Some(payload)) => {
                    if payload.len() > MAX_WS_MESSAGE_BYTES {
                        close_session(
                            websocket,
                            CloseCode::Size,
                            "push message too large",
                            runtime.close_timeout(),
                        )
                        .await;
                        return;
                    }
                    let message = match std::str::from_utf8(&payload) {
                        Ok(text) => Message::Text(text.to_owned().into()),
                        Err(_) => Message::Binary(payload),
                    };
                    if websocket.send(message).await.is_err() {
                        return;
                    }
                    log_control("WebSocket/TCP", peer, "server_push", "sent");
                    continue;
                }
                SessionEvent::Push(None) => return,
                SessionEvent::Frame(message) => message,
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
                    WebSocketMessage::Binary(data)
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

            let request_bytes = super::message_bytes_for_log(&request);
            let started = Instant::now();
            if let Some(response) = protocol
                .handle_message_with_context(request, &session.context)
                .await
            {
                let response = match response {
                    WebSocketMessage::Text(text) => {
                        log_json_rpc(
                            "WebSocket/TCP",
                            peer,
                            &request_bytes,
                            Some(text.as_bytes()),
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
                    WebSocketMessage::Ping(data) => Message::Ping(data),
                    WebSocketMessage::Pong(data) => Message::Pong(data),
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
    };
    tokio::select! {
        biased;
        _ = protocol.session_cancelled(&session.context) => {},
        _ = serving => {},
    }
}

enum SessionEvent {
    Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Push(Option<Bytes>),
    Shutdown,
}

async fn close_session<S>(
    mut websocket: WebSocketStream<S>,
    code: CloseCode,
    reason: &'static str,
    timeout: std::time::Duration,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use json_rpc_2_core::WebSocketFuture;
    use json_rpc_2_transport::{CancellationToken, TransportContext, TransportRuntime};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::WebSocketConfig},
    };

    use super::{Role, WebSocketMessage, WebSocketProtocol, serve};

    struct SessionEcho {
        opened: tokio::sync::mpsc::UnboundedSender<TransportContext>,
    }

    impl WebSocketProtocol for SessionEcho {
        fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a> {
            Box::pin(async move { Some(message) })
        }

        fn handle_message_with_context<'a>(
            &'a self,
            message: WebSocketMessage,
            context: &'a TransportContext,
        ) -> WebSocketFuture<'a> {
            Box::pin(async move {
                match message {
                    WebSocketMessage::Text(text) => Some(WebSocketMessage::Text(format!(
                        "{}:{}:{text}",
                        context.transport, context.connection_id
                    ))),
                    message => Some(message),
                }
            })
        }

        fn session_opened(&self, context: &TransportContext) {
            let _ = self.opened.send(context.clone());
            if let Some(sender) = &context.push_sender {
                let _ = sender.send(Bytes::from_static(b"server-push"));
            }
        }
    }

    #[tokio::test]
    async fn websocket_tcp_preserves_session_context_and_delivers_push() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (opened, mut sessions) = tokio::sync::mpsc::unbounded_channel();
        let protocol = Arc::new(SessionEcho { opened });
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(serve(
            listener,
            protocol,
            shutdown.clone(),
            Arc::new(TransportRuntime::default()),
        ));

        let stream = TcpStream::connect(address).await.unwrap();
        let mut client: WebSocketStream<TcpStream> = WebSocketStream::from_raw_socket(
            stream,
            Role::Client,
            Some(WebSocketConfig::default()),
        )
        .await;
        let context = tokio::time::timeout(std::time::Duration::from_secs(2), sessions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.transport, "ws-tcp");
        let connection_id = context.connection_id;

        assert_eq!(
            client.next().await.unwrap().unwrap(),
            Message::Text("server-push".into())
        );
        client.send(Message::Text("request".into())).await.unwrap();
        assert_eq!(
            client.next().await.unwrap().unwrap(),
            Message::Text(format!("ws-tcp:{connection_id}:request").into())
        );

        drop(client);
        shutdown.cancel();
        server.await.unwrap().unwrap();
    }
}
