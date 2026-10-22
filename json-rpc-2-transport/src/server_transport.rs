//! Transport-independent server contract.
//!
//! A transport only handles message framing and delivery; JSON-RPC semantics
//! are delegated to [`JsonRpcProtocol`], implemented once by the dispatcher in
//! `json-rpc-2-server`.

use std::{
    fmt::Display,
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use serde::Serialize;
use tokio::{sync::mpsc, task::JoinHandle};
pub use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{TcpCongestionControl, TcpTuningConfig};
use json_rpc_2_core::UdpReliabilityConfig;

/// QUIC application close code for a graceful raw QUIC shutdown.
pub const QUIC_NO_ERROR: u32 = 0;
/// HTTP/3 application close code for a graceful shutdown (RFC 9114 §8.1).
pub const H3_NO_ERROR: u32 = 0x100;

/// Future returned by the abstract JSON-RPC protocol contract.
pub type ProtocolFuture<'a> = Pin<Box<dyn Future<Output = Option<Bytes>> + Send + 'a>>;
pub type ProtocolMessageFuture<'a> =
    Pin<Box<dyn Future<Output = Option<crate::TransportMessage>> + Send + 'a>>;

/// Transport-independent JSON-RPC message processing contract.
///
/// Implementations receive one complete JSON-RPC message and return its
/// response bytes, or `None` when the request is a notification.
pub trait JsonRpcProtocol: Send + Sync + 'static {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a>;

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        _context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        self.handle_message(input)
    }

    fn session_opened(&self, _context: &TransportContext) {}

    fn session_closed(&self, _context: &TransportContext) {}

    /// Retain separate response envelope/payload buffers on supporting carriers.
    fn handle_transport_message<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolMessageFuture<'a> {
        Box::pin(async move {
            self.handle_message_with_context(input, context)
                .await
                .map(crate::TransportMessage::from)
        })
    }

    /// Transfer receive-buffer ownership, avoiding a copy into an async bus.
    fn handle_owned_message<'a>(
        &'a self,
        input: Bytes,
        context: &'a TransportContext,
    ) -> ProtocolMessageFuture<'a> {
        Box::pin(async move { self.handle_transport_message(&input, context).await })
    }
}

#[derive(Clone)]
pub struct TransportContext {
    pub transport: String,
    pub connection_id: String,
    pub push_sender: Option<mpsc::UnboundedSender<Bytes>>,
}

/// Dispatch an owned packet on carriers whose encoder requires contiguous bytes.
pub async fn dispatch_stateless(
    protocol: &dyn JsonRpcProtocol,
    payload: Bytes,
    transport: &str,
) -> Option<Bytes> {
    let context = TransportSession::stateless(transport);
    protocol
        .handle_owned_message(payload, &context)
        .await
        .map(crate::TransportMessage::into_bytes)
}

pub struct GatewayRequestMetadata {
    pub connection_id: String,
    pub transport: String,
    pub payload_length: Option<usize>,
}

impl GatewayRequestMetadata {
    pub fn parse(line: &str) -> io::Result<Option<Self>> {
        if line == "JRPC-GW/1" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gateway metadata requires connection ID and transport",
            ));
        }
        let Some(rest) = line.strip_prefix("JRPC-GW/1 ") else {
            return Ok(None);
        };
        let mut fields = rest.split_ascii_whitespace();
        let connection_id = fields.next();
        let transport = fields.next();
        let payload_length = fields
            .next()
            .map(|value| {
                value.parse::<usize>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "gateway metadata payload length must be a non-negative integer",
                    )
                })
            })
            .transpose()?;
        if fields.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid gateway metadata fields",
            ));
        }
        let (Some(connection_id), Some(transport)) = (connection_id, transport) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gateway metadata requires connection ID and transport",
            ));
        };
        if connection_id.len() > 128
            || connection_id.is_empty()
            || !connection_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || transport.is_empty()
            || !transport
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid gateway metadata value",
            ));
        }
        if payload_length.is_some_and(|length| length > json_rpc_2_core::MAX_MESSAGE_BYTES) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gateway metadata payload exceeds the maximum message size",
            ));
        }
        Ok(Some(Self {
            connection_id: connection_id.to_owned(),
            transport: transport.to_owned(),
            payload_length,
        }))
    }

    pub fn context(&self, push_sender: Option<mpsc::UnboundedSender<Bytes>>) -> TransportContext {
        TransportContext {
            transport: self.transport.clone(),
            connection_id: self.connection_id.clone(),
            push_sender,
        }
    }
}

#[cfg(test)]
mod gateway_metadata_tests {
    use super::GatewayRequestMetadata;

    #[test]
    fn parses_gateway_metadata_without_changing_the_payload_contract() {
        let metadata = GatewayRequestMetadata::parse(
            "JRPC-GW/1 550e8400-e29b-41d4-a716-446655440000 ws-tcp 25",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            metadata.connection_id,
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(metadata.transport, "ws-tcp");
        assert_eq!(metadata.payload_length, Some(25));
    }

    #[test]
    fn ignores_lines_without_the_gateway_metadata_prefix() {
        assert!(
            GatewayRequestMetadata::parse(r#"{"jsonrpc":"2.0","method":"echo","id":1}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_malformed_gateway_metadata() {
        for line in [
            "JRPC-GW/1",
            "JRPC-GW/1 connection-id",
            "JRPC-GW/1 connection-id tcp extra",
            "JRPC-GW/1 invalid_id tcp",
            "JRPC-GW/1 connection-id tcp not-a-length",
        ] {
            assert!(
                GatewayRequestMetadata::parse(line).is_err(),
                "accepted malformed metadata: {line}"
            );
        }
    }
}

pub struct TransportSession {
    pub context: TransportContext,
    pub push_receiver: mpsc::UnboundedReceiver<Bytes>,
}

impl TransportSession {
    pub fn new(transport: impl Into<String>, supports_push: bool) -> Self {
        let (push_sender, push_receiver) = mpsc::unbounded_channel();
        Self {
            context: TransportContext {
                transport: transport.into(),
                connection_id: Uuid::new_v4().to_string(),
                push_sender: supports_push.then_some(push_sender),
            },
            push_receiver,
        }
    }

    pub fn stateless(transport: impl Into<String>) -> TransportContext {
        TransportContext {
            transport: transport.into(),
            connection_id: Uuid::new_v4().to_string(),
            push_sender: None,
        }
    }
}

pub struct ProtocolSessionGuard {
    protocol: Arc<dyn JsonRpcProtocol>,
    context: TransportContext,
}

impl ProtocolSessionGuard {
    pub fn new(protocol: Arc<dyn JsonRpcProtocol>, context: TransportContext) -> Self {
        protocol.session_opened(&context);
        Self { protocol, context }
    }

    pub fn context(&self) -> &TransportContext {
        &self.context
    }
}

impl Drop for ProtocolSessionGuard {
    fn drop(&mut self) {
        self.protocol.session_closed(&self.context);
    }
}

/// Runtime limits for the server dispatcher and transport fan-out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerLimits {
    pub max_connections: usize,
    pub max_concurrent_requests: usize,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_connections: 4096,
            max_concurrent_requests: 1024,
        }
    }
}

/// Compatibility alias for the server-side limits type.
pub type TransportLimits = ServerLimits;

/// Graceful shutdown behavior shared by transport adapters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerShutdownPolicy {
    pub close_timeout: Duration,
}

impl Default for ServerShutdownPolicy {
    fn default() -> Self {
        Self {
            close_timeout: Duration::from_secs(5),
        }
    }
}

/// Future returned by a transport server.
pub type TransportFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

#[derive(Default)]
pub struct RuntimeMetrics {
    active_sessions: AtomicUsize,
    active_streams: AtomicUsize,
    reconnect_attempts: AtomicU64,
}

/// Observable transport/runtime health metrics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub active_sessions: usize,
    pub active_streams: usize,
    pub reconnect_attempts: u64,
}

impl RuntimeMetrics {
    pub fn session_started(&self) {
        self.sessions_started(1);
    }

    pub fn session_finished(&self) {
        self.sessions_finished(1);
    }

    pub fn sessions_started(&self, count: usize) {
        self.active_sessions.fetch_add(count, Ordering::Relaxed);
    }

    pub fn sessions_finished(&self, count: usize) {
        self.active_sessions.fetch_sub(count, Ordering::Relaxed);
    }

    pub fn stream_started(&self) {
        self.streams_started(1);
    }

    pub fn stream_finished(&self) {
        self.streams_finished(1);
    }

    pub fn streams_started(&self, count: usize) {
        self.active_streams.fetch_add(count, Ordering::Relaxed);
    }

    pub fn streams_finished(&self, count: usize) {
        self.active_streams.fetch_sub(count, Ordering::Relaxed);
    }

    pub fn reconnect_attempted(&self) {
        self.reconnect_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            active_streams: self.active_streams.load(Ordering::Relaxed),
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
        }
    }
}

/// Immutable transport runtime knobs shared by running server transports.
pub struct TransportRuntime {
    pub limits: ServerLimits,
    pub shutdown: ServerShutdownPolicy,
    pub tcp_tuning: TcpTuningConfig,
    pub udp_reliability: UdpReliabilityConfig,
    pub metrics: Arc<RuntimeMetrics>,
}

impl Default for TransportRuntime {
    fn default() -> Self {
        Self {
            limits: ServerLimits::default(),
            shutdown: ServerShutdownPolicy::default(),
            tcp_tuning: TcpTuningConfig::default(),
            udp_reliability: UdpReliabilityConfig::default(),
            metrics: Arc::new(RuntimeMetrics::default()),
        }
    }
}

impl TransportRuntime {
    pub fn new(
        limits: ServerLimits,
        shutdown: ServerShutdownPolicy,
        tcp_tuning: TcpTuningConfig,
        udp_reliability: UdpReliabilityConfig,
    ) -> Self {
        Self {
            limits,
            shutdown,
            tcp_tuning,
            udp_reliability,
            metrics: Arc::new(RuntimeMetrics::default()),
        }
    }

    pub fn close_timeout(&self) -> Duration {
        self.shutdown.close_timeout
    }
}

/// Abstract transport contract. A transport only handles message framing and
/// delivery; JSON-RPC semantics are delegated to `JsonRpcProtocol`.
pub trait Transport: Send + 'static {
    /// Serve until `shutdown` is cancelled, then close gracefully: stop
    /// accepting, finish in-flight messages, perform the carrier's closing
    /// handshake and release the listener or endpoint.
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture;

    /// Run the transport in a background task that can be closed later.
    fn start(self, protocol: Arc<dyn JsonRpcProtocol>) -> RunningTransport
    where
        Self: Sized,
    {
        RunningTransport::spawn(|shutdown| self.run(protocol, shutdown))
    }
}

/// Handle to a transport server running in the background.
pub struct RunningTransport {
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl RunningTransport {
    pub fn spawn(run: impl FnOnce(CancellationToken) -> TransportFuture) -> Self {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(shutdown.clone()));
        Self { shutdown, task }
    }

    /// Gracefully close the transport and wait until it has fully stopped.
    pub async fn close(self) -> io::Result<()> {
        self.shutdown.cancel();
        self.task.await.map_err(io::Error::other)?
    }
}

pub fn log_json_rpc(
    transport: &str,
    peer: impl Display,
    request: &[u8],
    response: Option<&[u8]>,
    latency: Duration,
) {
    debug!(
        transport,
        peer = %peer,
        request_bytes = request.len(),
        response_bytes = response.map_or(0, <[u8]>::len),
        latency_ms = latency.as_millis() as u64,
        notification = response.is_none(),
        "handled opaque message"
    );
}

pub fn log_control(transport: &str, peer: impl Display, action: &str, response: &str) {
    debug!(
        transport,
        peer = %peer,
        action,
        response,
        "handled control traffic"
    );
}

pub fn log_transport_error(
    transport: &str,
    peer: impl Display,
    stage: &'static str,
    error: &dyn std::fmt::Display,
) {
    warn!(transport, peer = %peer, stage, error = %error, "transport session failed");
}

pub fn apply_tcp_listener_tuning(
    socket: &socket2::Socket,
    tuning: &TcpTuningConfig,
) -> io::Result<()> {
    socket.set_tcp_nodelay(tuning.no_delay)?;
    apply_tcp_congestion_socket(socket, tuning.congestion_control)?;
    apply_tcp_fast_open_listener(socket, tuning)?;
    Ok(())
}

pub fn apply_tcp_stream_tuning(
    stream: &tokio::net::TcpStream,
    tuning: &TcpTuningConfig,
) -> io::Result<()> {
    let socket = socket2::SockRef::from(stream);
    socket.set_tcp_nodelay(tuning.no_delay)?;
    apply_tcp_congestion_sockref(&socket, tuning.congestion_control)
}

fn apply_tcp_congestion_socket(
    _socket: &socket2::Socket,
    congestion: TcpCongestionControl,
) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        apply_tcp_congestion_raw(_socket, congestion)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        if matches!(congestion, TcpCongestionControl::SystemDefault) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TCP congestion control overrides are only supported on linux/android in this crate",
            ))
        }
    }
}

fn apply_tcp_congestion_sockref(
    _socket: &socket2::SockRef<'_>,
    congestion: TcpCongestionControl,
) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        apply_tcp_congestion_raw(_socket, congestion)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        if matches!(congestion, TcpCongestionControl::SystemDefault) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TCP congestion control overrides are only supported on linux/android in this crate",
            ))
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn apply_tcp_congestion_raw(
    socket: &socket2::Socket,
    congestion: TcpCongestionControl,
) -> io::Result<()> {
    let Some(name) = congestion.as_socket_name() else {
        return Ok(());
    };
    use std::os::fd::AsRawFd;

    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CONGESTION,
            name.as_ptr().cast(),
            name.len() as libc::socklen_t,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn apply_tcp_fast_open_listener(
    socket: &socket2::Socket,
    tuning: &TcpTuningConfig,
) -> io::Result<()> {
    if !tuning.fast_open {
        return Ok(());
    }
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    {
        #[cfg(unix)]
        use std::os::fd::AsRawFd;

        let value: libc::c_int = if cfg!(any(target_os = "macos", target_os = "ios")) {
            1
        } else {
            tuning.fast_open_queue_len.min(i32::MAX as u32) as libc::c_int
        };
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_FASTOPEN,
                (&value as *const libc::c_int).cast(),
                std::mem::size_of_val(&value) as libc::socklen_t,
            )
        };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    )))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TCP Fast Open listeners are only supported on linux/android/macos/ios in this crate",
        ))
    }
}

/// Shared multiplexor for transports that fan out independent stream sessions.
pub mod multiplex {
    use std::{future::Future, io, sync::Arc};

    use tokio::sync::Semaphore;
    use tokio_util::task::TaskTracker;

    use super::RuntimeMetrics;

    /// Shared session multiplexer for stream-oriented transports.
    ///
    /// Each accepted connection or stream is spawned as an independent task
    /// while the semaphore keeps a bounded notion of concurrent in-flight
    /// sessions.
    pub struct StreamMultiplexor {
        sessions: TaskTracker,
        limit: Arc<Semaphore>,
        metrics: Arc<RuntimeMetrics>,
    }

    impl StreamMultiplexor {
        /// Create a multiplexor with an explicit maximum number of concurrent
        /// sessions.
        pub fn with_limit(max_sessions: usize, metrics: Arc<RuntimeMetrics>) -> Self {
            Self {
                sessions: TaskTracker::new(),
                limit: Arc::new(Semaphore::new(max_sessions)),
                metrics,
            }
        }

        /// Cloneable handle used by accept loops and upgrade callbacks.
        pub fn handle(&self) -> StreamMultiplexorHandle {
            StreamMultiplexorHandle {
                sessions: self.sessions.clone(),
                limit: self.limit.clone(),
                metrics: self.metrics.clone(),
            }
        }

        /// Stop accepting new work and wait until every tracked session ends.
        pub async fn close(self) {
            self.sessions.close();
            self.sessions.wait().await;
        }
    }

    /// Handle that dispatches accepted sessions into the shared multiplexor.
    #[derive(Clone)]
    pub struct StreamMultiplexorHandle {
        sessions: TaskTracker,
        limit: Arc<Semaphore>,
        metrics: Arc<RuntimeMetrics>,
    }

    impl StreamMultiplexorHandle {
        /// Spawn one session into the multiplexor.
        pub async fn spawn<F>(&self, session: F) -> io::Result<()>
        where
            F: Future<Output = ()> + Send + 'static,
        {
            let permit = self
                .limit
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| io::Error::other("session multiplexor closed"))?;
            self.metrics.session_started();
            self.metrics.stream_started();
            let metrics = self.metrics.clone();
            self.sessions.spawn(async move {
                let _permit = permit;
                session.await;
                metrics.stream_finished();
                metrics.session_finished();
            });
            Ok(())
        }
    }
}
