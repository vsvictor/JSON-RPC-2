//! HTTP/3 server binding over a caller-configured QUIC endpoint.

use axum::http::{Method, Response, StatusCode, header};
use bytes::Buf;
use h3::server::RequestResolver;
use quinn::Endpoint;
use std::{io, sync::Arc, time::Instant};

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::JsonRpcProtocol;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    CONTROL_PATH, CancellationToken, H3_NO_ERROR, Transport, TransportFuture, TransportRuntime,
    log_control, log_json_rpc, log_transport_error,
};

/// HTTP/3 transport over an endpoint configured with the `h3` ALPN protocol.
pub struct Http3Transport {
    endpoint: Endpoint,
    runtime: Arc<TransportRuntime>,
}

impl Http3Transport {
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

impl Transport for Http3Transport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.endpoint, protocol, shutdown, self.runtime))
    }
}

/// Accept HTTP/3 connections on a QUIC endpoint configured with the `h3`
/// ALPN protocol by the caller.
///
/// After `shutdown` is cancelled every connection sends GOAWAY, finishes its
/// in-flight requests and closes with `H3_NO_ERROR`.
pub async fn serve(
    endpoint: Endpoint,
    protocol: Arc<dyn JsonRpcProtocol>,
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
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) {
    let mut h3 =
        match h3::server::Connection::new(h3_quinn::Connection::new(connection.clone())).await {
            Ok(h3) => h3,
            Err(_) => return,
        };
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let requests = multiplexor.handle();
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
        if requests
            .spawn(async move {
                if let Err(error) = handle_request(resolver, peer, protocol).await {
                    log_transport_error("HTTP/3", peer, "request", error.as_ref());
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
    resolver: RequestResolver<C, bytes::Bytes>,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn JsonRpcProtocol>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    C: h3::quic::Connection<bytes::Bytes>,
    C::BidiStream: h3::quic::BidiStream<bytes::Bytes>,
{
    let (request, stream) = resolver.resolve_request().await?;
    handle_resolved_request(request, stream, peer, protocol).await
}

/// Serve an already resolved request on a caller-owned HTTP/3 connection.
pub async fn handle_resolved_request<S>(
    request: axum::http::Request<()>,
    mut stream: h3::server::RequestStream<S, bytes::Bytes>,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn JsonRpcProtocol>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::BidiStream<bytes::Bytes>,
{
    let started = Instant::now();
    let mut body = Vec::new();

    while let Some(mut chunk) = stream.recv_data().await? {
        let chunk = chunk.copy_to_bytes(chunk.remaining());
        if body.len().saturating_add(chunk.len()) > MAX_MESSAGE_BYTES {
            let response = Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(())?;
            stream.send_response(response).await?;
            stream.finish().await?;
            return Ok(());
        }
        body.extend_from_slice(&chunk);
    }

    let (status, content_type, response_body) =
        if request.method() == Method::GET && request.uri().path() == CONTROL_PATH {
            log_control("HTTP/3", peer, "ping", "pong");
            (
                StatusCode::OK,
                Some("text/plain; charset=utf-8"),
                Some(json_rpc_2_transport::TransportMessage::from(
                    bytes::Bytes::from_static(b"pong"),
                )),
            )
        } else if request.method() != Method::POST {
            (StatusCode::METHOD_NOT_ALLOWED, None, None)
        } else {
            let binary = json_rpc_2_transport::gateway::is_gateway_frame(&body);
            let context = json_rpc_2_transport::TransportSession::stateless("http3");
            let body = bytes::Bytes::from(body);
            let request_log = body.clone();
            let response = protocol.handle_owned_message(body, &context).await;
            log_json_rpc(
                "HTTP/3",
                peer,
                &request_log,
                response.as_ref().map(|message| {
                    if message.header.is_empty() {
                        message.payload.as_ref()
                    } else {
                        message.header.as_ref()
                    }
                }),
                started.elapsed(),
            );
            match response {
                Some(response) => (
                    StatusCode::OK,
                    Some(if binary {
                        "application/octet-stream"
                    } else {
                        "application/json"
                    }),
                    Some(response),
                ),
                None => (StatusCode::NO_CONTENT, None, None),
            }
        };

    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    stream.send_response(builder.body(())?).await?;
    if let Some(response) = response_body {
        if !response.header.is_empty() {
            stream.send_data(response.header).await?;
        }
        if !response.payload.is_empty() {
            stream.send_data(response.payload).await?;
        }
    }
    stream.finish().await?;
    Ok(())
}
