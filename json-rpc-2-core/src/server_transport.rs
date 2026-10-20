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
use serde_json::Value;
use tokio::task::JoinHandle;
pub use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::{TcpCongestionControl, TcpTuningConfig, UdpReliabilityConfig};

/// QUIC application close code for a graceful raw QUIC shutdown.
pub const QUIC_NO_ERROR: u32 = 0;
/// HTTP/3 application close code for a graceful shutdown (RFC 9114 §8.1).
pub const H3_NO_ERROR: u32 = 0x100;

/// Future returned by the abstract JSON-RPC protocol contract.
pub type ProtocolFuture<'a> = Pin<Box<dyn Future<Output = Option<Bytes>> + Send + 'a>>;

/// Transport-independent JSON-RPC message processing contract.
///
/// Implementations receive one complete JSON-RPC message and return its
/// response bytes, or `None` when the request is a notification.
pub trait JsonRpcProtocol: Send + Sync + 'static {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a>;
}

/// Runtime limits for the server dispatcher and transport fan-out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportLimits {
    pub max_connections: usize,
    pub max_concurrent_requests: usize,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_connections: 4096,
            max_concurrent_requests: 1024,
        }
    }
}

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
    pub(crate) active_sessions: AtomicUsize,
    pub(crate) active_streams: AtomicUsize,
    pub(crate) reconnect_attempts: AtomicU64,
}

/// Observable transport/runtime health metrics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub active_sessions: usize,
    pub active_streams: usize,
    pub reconnect_attempts: u64,
}

impl RuntimeMetrics {
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
    pub limits: TransportLimits,
    pub shutdown: ServerShutdownPolicy,
    pub tcp_tuning: TcpTuningConfig,
    pub udp_reliability: UdpReliabilityConfig,
    pub metrics: Arc<RuntimeMetrics>,
}

impl Default for TransportRuntime {
    fn default() -> Self {
        Self {
            limits: TransportLimits::default(),
            shutdown: ServerShutdownPolicy::default(),
            tcp_tuning: TcpTuningConfig::default(),
            udp_reliability: UdpReliabilityConfig::default(),
            metrics: Arc::new(RuntimeMetrics::default()),
        }
    }
}

impl TransportRuntime {
    pub fn new(
        limits: TransportLimits,
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
    let peer = peer.to_string();
    let (method, request_id) = extract_request_meta(request);
    let response_code = response.and_then(extract_response_code);
    debug!(
        transport,
        peer,
        method = method.as_deref().unwrap_or("<invalid>"),
        request_id = request_id.as_deref().unwrap_or("notification"),
        latency_ms = latency.as_millis() as u64,
        response_code = response_code.unwrap_or(0),
        notification = response.is_none(),
        "handled json-rpc message"
    );
    if trace_enabled() {
        trace!(transport, peer, request = %String::from_utf8_lossy(request), "json-rpc request payload");
        match response {
            Some(response) => {
                trace!(transport, peer, response = %String::from_utf8_lossy(response), "json-rpc response payload");
            }
            None => {
                trace!(transport, peer, "json-rpc notification without response");
            }
        }
    }
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

fn trace_enabled() -> bool {
    tracing::enabled!(tracing::Level::TRACE)
}

fn extract_request_meta(request: &[u8]) -> (Option<String>, Option<String>) {
    match serde_json::from_slice::<Value>(request) {
        Ok(Value::Object(object)) => (
            object
                .get("method")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            object.get("id").map(serialize_value),
        ),
        Ok(Value::Array(items)) => (
            Some(format!("batch[{}]", items.len())),
            Some(format!(
                "{} ids",
                items.iter().filter(|item| item.get("id").is_some()).count()
            )),
        ),
        _ => (None, None),
    }
}

fn extract_response_code(response: &[u8]) -> Option<i64> {
    match serde_json::from_slice::<Value>(response).ok()? {
        Value::Object(object) => object
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(Value::as_i64),
        Value::Array(items) => items.iter().find_map(|item| {
            item.get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_i64)
        }),
        _ => None,
    }
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
fn apply_tcp_congestion_raw<Fd>(socket: &Fd, congestion: TcpCongestionControl) -> io::Result<()>
where
    Fd: std::os::fd::AsFd,
{
    let Some(name) = congestion.as_socket_name() else {
        return Ok(());
    };
    use std::os::fd::{AsFd, AsRawFd};

    let result = unsafe {
        libc::setsockopt(
            socket.as_fd().as_raw_fd(),
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

fn apply_tcp_fast_open_listener(socket: &socket2::Socket, tuning: &TcpTuningConfig) -> io::Result<()> {
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
        return Ok(());
    }
    #[allow(unreachable_code)]
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "TCP Fast Open listeners are only supported on linux/android/macos/ios in this crate",
    ))
}

fn serialize_value(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<invalid>".to_owned())
}

/// Shared multiplexor for transports that fan out independent stream sessions.
pub mod multiplex {
    use std::{
        future::Future,
        io,
        sync::{Arc, atomic::Ordering},
    };

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
            self.metrics.active_sessions.fetch_add(1, Ordering::Relaxed);
            self.metrics.active_streams.fetch_add(1, Ordering::Relaxed);
            let metrics = self.metrics.clone();
            self.sessions.spawn(async move {
                let _permit = permit;
                session.await;
                metrics.active_streams.fetch_sub(1, Ordering::Relaxed);
                metrics.active_sessions.fetch_sub(1, Ordering::Relaxed);
            });
            Ok(())
        }
    }
}
