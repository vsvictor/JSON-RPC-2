//! WebSocket-over-HTTP/3 extended CONNECT adapter (RFC 9220).
//!
//! Each accepted CONNECT stream owns its persistent context and push channel.

use std::{io, sync::Arc, time::Instant};

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketSessionGuard,
    WebSocketTransport, WebSocketTransportFuture,
};
use axum::http::{HeaderValue, Method, Response, StatusCode, header};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::{Buf, Bytes};
use futures_util::{SinkExt, StreamExt};
use h3::{ext::Protocol, server::RequestResolver};
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    H3_NO_ERROR, TransportRuntime, TransportSession, log_control, log_json_rpc, log_transport_error,
};
use quinn::Endpoint;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        handshake::derive_accept_key,
        protocol::{CloseFrame, Message, Role, WebSocketConfig, frame::coding::CloseCode},
    },
};

const MAX_WS_MESSAGE_BYTES: usize = MAX_MESSAGE_BYTES;
const DUPLEX_BUFFER_BYTES: usize = 64 * 1024;

/// WebSocket transport established using HTTP/3 extended CONNECT.
pub struct Http3WebSocketTransport {
    endpoint: Endpoint,
    runtime: Arc<TransportRuntime>,
}

impl Http3WebSocketTransport {
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

impl WebSocketTransport for Http3WebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        Box::pin(serve(self.endpoint, protocol, shutdown, self.runtime))
    }
}

/// Serve WebSocket connections over HTTP/3 extended CONNECT.
///
/// The endpoint must use the HTTP/3 `h3` ALPN protocol. This server advertises
/// `SETTINGS_ENABLE_CONNECT_PROTOCOL` and accepts `:protocol = websocket`.
///
/// After `shutdown` is cancelled every session is closed with 1001 Going Away,
/// its CONNECT stream is finished, GOAWAY is sent and the connection is closed
/// with `H3_NO_ERROR`.
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
    connection: quinn::Connection,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) {
    let mut builder = h3::server::builder();
    builder.enable_extended_connect(true);
    let mut h3 = match builder
        .build(h3_quinn::Connection::new(connection.clone()))
        .await
    {
        Ok(connection) => connection,
        Err(error) => {
            log_transport_error("WebSocket/HTTP/3", peer, "setup", &error);
            return;
        }
    };

    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let sessions = multiplexor.handle();
    loop {
        let accepted = tokio::select! {
            accepted = h3.accept() => accepted,
            _ = shutdown.cancelled() => {
                let _ = h3.shutdown(0).await;
                break;
            }
        };
        // `None` or an error means the client closed the connection.
        let Ok(Some(resolver)) = accepted else { break };
        let protocol = protocol.clone();
        let shutdown = shutdown.clone();
        let runtime = runtime.clone();
        if sessions
            .spawn(async move {
                if let Err(error) =
                    handle_request(resolver, peer, protocol, shutdown, runtime).await
                {
                    log_transport_error("WebSocket/HTTP/3", peer, "request", error.as_ref());
                }
            })
            .await
            .is_err()
        {
            break;
        }
    }
    multiplexor.close().await;
    drop(h3);
    connection.close(H3_NO_ERROR.into(), b"");
}

async fn handle_request<C>(
    resolver: RequestResolver<C, Bytes>,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    C: h3::quic::Connection<Bytes>,
    C::BidiStream: h3::quic::BidiStream<Bytes> + Send + 'static,
    <C::BidiStream as h3::quic::BidiStream<Bytes>>::SendStream: Send + 'static,
    <C::BidiStream as h3::quic::BidiStream<Bytes>>::RecvStream: Send + 'static,
{
    let (request, stream) = resolver.resolve_request().await?;
    handle_resolved_request(request, stream, peer, protocol, shutdown, runtime).await
}

/// Serve an already resolved extended CONNECT on a shared HTTP/3 connection.
pub async fn handle_resolved_request<S>(
    request: http::Request<()>,
    mut stream: h3::server::RequestStream<S, Bytes>,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::BidiStream<Bytes> + Send + 'static,
    S::SendStream: Send + 'static,
    S::RecvStream: Send + 'static,
{
    let is_websocket_connect = request.method() == Method::CONNECT
        && request
            .extensions()
            .get::<Protocol>()
            .is_some_and(|protocol| protocol.as_str() == "websocket");
    if !is_websocket_connect {
        stream
            .send_response(
                Response::builder()
                    .status(StatusCode::NOT_IMPLEMENTED)
                    .body(())?,
            )
            .await?;
        stream.finish().await?;
        return Ok(());
    }

    let accept = derive_websocket_accept(
        request.headers().get(header::SEC_WEBSOCKET_VERSION),
        request.headers().get(header::SEC_WEBSOCKET_KEY),
    );
    let Some(accept) = accept else {
        send_status(&mut stream, StatusCode::BAD_REQUEST).await?;
        return Ok(());
    };
    stream
        .send_response(
            Response::builder()
                .status(StatusCode::OK)
                .header(header::SEC_WEBSOCKET_ACCEPT, accept)
                .body(())?,
        )
        .await?;

    let (send_stream, recv_stream) = stream.split();
    let (ws_io, bridge_io) = tokio::io::duplex(DUPLEX_BUFFER_BYTES);
    let (ws_reader, ws_writer) = tokio::io::split(bridge_io);
    let mut recv_task = tokio::spawn(async move {
        if let Err(error) = copy_h3_to_io(recv_stream, ws_writer).await {
            log_transport_error("WebSocket/HTTP/3", peer, "recv-bridge", error.as_ref());
        }
    });
    let mut send_task = tokio::spawn(async move {
        if let Err(error) = copy_io_to_h3(ws_reader, send_stream).await {
            log_transport_error("WebSocket/HTTP/3", peer, "send-bridge", error.as_ref());
        }
    });

    // The session drops its end of the duplex when it returns, so the send
    // bridge flushes the final Close frame, finishes the H3 stream and exits.
    // The peer then finishes its half; give it a bounded time to do so.
    run_websocket_session(ws_io, peer, protocol, shutdown, runtime.clone()).await;
    for (task, operation) in [
        (&mut send_task, "send-bridge"),
        (&mut recv_task, "recv-bridge"),
    ] {
        match tokio::time::timeout(runtime.close_timeout(), &mut *task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => log_transport_error("WebSocket/HTTP/3", peer, operation, &error),
            Err(error) => {
                log_transport_error("WebSocket/HTTP/3", peer, operation, &error);
                task.abort();
                let _ = task.await;
            }
        }
    }
    Ok(())
}

pub(crate) fn derive_websocket_accept(
    version: Option<&HeaderValue>,
    key: Option<&HeaderValue>,
) -> Option<HeaderValue> {
    if version.is_none_or(|version| version != "13") {
        return None;
    }
    let key = key?.as_bytes();
    if STANDARD.decode(key).ok()?.len() != 16 {
        return None;
    }
    HeaderValue::from_str(&derive_accept_key(key)).ok()
}

async fn send_status<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    status: StatusCode,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::SendStream<Bytes>,
{
    stream
        .send_response(Response::builder().status(status).body(())?)
        .await?;
    stream.finish().await?;
    Ok(())
}

async fn copy_h3_to_io<S>(
    mut stream: h3::server::RequestStream<S, Bytes>,
    mut writer: tokio::io::WriteHalf<DuplexStream>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::RecvStream + Send + 'static,
{
    while let Some(mut data) = stream.recv_data().await? {
        let bytes = data.copy_to_bytes(data.remaining());
        writer.write_all(&bytes).await?;
    }
    writer.shutdown().await?;
    Ok(())
}

async fn copy_io_to_h3<S>(
    mut reader: tokio::io::ReadHalf<DuplexStream>,
    mut stream: h3::server::RequestStream<S, Bytes>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::SendStream<Bytes> + Send + 'static,
{
    let mut buffer = [0; 16 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        stream
            .send_data(Bytes::copy_from_slice(&buffer[..read]))
            .await?;
    }
    stream.finish().await?;
    Ok(())
}

async fn run_websocket_session(
    stream: DuplexStream,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WS_MESSAGE_BYTES));
    let mut websocket = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
    let mut session = TransportSession::new("ws-http3", true);
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
                    log_transport_error("WebSocket/HTTP/3", peer, "push", &error);
                    return;
                }
                log_control("WebSocket/HTTP/3", peer, "server_push", "sent");
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
            // End of stream: the closing handshake is complete.
            None => return,
            Some(Err(error)) => {
                log_transport_error("WebSocket/HTTP/3", peer, "frame", &error);
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
                log_control("WebSocket/HTTP/3", peer, "ping", "pong");
                if let Err(error) = websocket.send(Message::Pong(payload)).await {
                    log_transport_error("WebSocket/HTTP/3", peer, "pong", &error);
                    return;
                }
                continue;
            }
            Message::Close(_) => {
                guard.take();
                if let Err(error) = websocket.flush().await {
                    log_transport_error("WebSocket/HTTP/3", peer, "close reply", &error);
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
                        "WebSocket/HTTP/3",
                        peer,
                        &request_bytes,
                        Some(text.as_bytes()),
                        started.elapsed(),
                    );
                    Message::Text(text.into())
                }
                WebSocketMessage::Binary(data) => {
                    log_json_rpc(
                        "WebSocket/HTTP/3",
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
                log_transport_error("WebSocket/HTTP/3", peer, "response", &error);
                return;
            }
        } else {
            log_json_rpc(
                "WebSocket/HTTP/3",
                peer,
                &request_bytes,
                None,
                started.elapsed(),
            );
        }
    }
}

/// Server-initiated closing handshake: send Close, then wait (bounded) until
/// the client's Close reply ends the stream.
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
        log_transport_error("WebSocket/HTTP/3", "session", "close", &error);
        return;
    }
    if let Err(error) = tokio::time::timeout(timeout, async {
        while let Some(message) = websocket.next().await {
            match message {
                Ok(Message::Close(_)) => break,
                Err(error) => {
                    log_transport_error("WebSocket/HTTP/3", "session", "close reply", &error);
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    {
        log_transport_error("WebSocket/HTTP/3", "session", "close timeout", &error);
    }
}
