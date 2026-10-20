//! HTTP/1.1 server binding.

use axum::{
    Router,
    body::Bytes,
    extract::{ConnectInfo, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::{io, sync::Arc, time::Instant};
use tokio::net::TcpListener;

use super::{
    CONTROL_PATH, CancellationToken, Transport, TransportFuture, TransportRuntime, log_control,
    log_json_rpc,
};
use crate::{JsonRpcProtocol, MAX_MESSAGE_BYTES};

/// HTTP/1.1 transport with JSON-RPC requests at `POST /`.
pub struct Http1Transport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

impl Http1Transport {
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

impl Transport for Http1Transport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve_http1(self.listener, protocol, shutdown, self.runtime))
    }
}

/// Build an HTTP/1.1 JSON-RPC router for embedding in an application.
pub fn router(protocol: Arc<dyn JsonRpcProtocol>) -> Router {
    router_with_runtime(protocol, Arc::new(TransportRuntime::default()))
}

pub fn router_with_runtime(
    protocol: Arc<dyn JsonRpcProtocol>,
    runtime: Arc<TransportRuntime>,
) -> Router {
    Router::new()
        .route("/", post(http_rpc))
        .route(CONTROL_PATH, get(control_ping))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_MESSAGE_BYTES))
        .with_state((protocol, runtime))
}

/// Serve the HTTP/1.1 router on a TCP listener.
///
/// After `shutdown` is cancelled no new connections are accepted, in-flight
/// requests are answered and keep-alive connections are closed.
pub async fn serve_http1(
    listener: TcpListener,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    axum::serve(
        listener,
        router_with_runtime(protocol, runtime)
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown.cancelled_owned())
    .await
}

async fn control_ping(ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>) -> impl IntoResponse {
    log_control("HTTP/1.1", peer, "ping", "pong");
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "pong",
    )
}

async fn http_rpc(
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    State((protocol, _runtime)): State<(Arc<dyn JsonRpcProtocol>, Arc<TransportRuntime>)>,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let response = protocol.handle_message(&body).await;
    log_json_rpc(
        "HTTP/1.1",
        peer,
        &body,
        response.as_deref(),
        started.elapsed(),
    );
    match response {
        Some(response) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            response,
        )
            .into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}
