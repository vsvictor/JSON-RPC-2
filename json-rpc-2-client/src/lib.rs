//! JSON-RPC 2.0 client independent of any concrete network transport.

use bytes::Bytes;
pub use json_rpc_2_core::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

pub mod transports;

const DEFAULT_MAX_MISSED_PONGS: u32 = 3;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_OUTBOUND_QUEUE_CAPACITY: usize = 64;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;

/// Client-side call error.
#[derive(Debug)]
pub enum RpcClientError {
    Transport(TransportError),
    Serialization(serde_json::Error),
    InvalidResponse(String),
    InvalidConfiguration(String),
    MissingResponse,
    IdMismatch,
    Timeout { method: String, timeout: Duration },
    Cancelled { method: String },
    SessionClosed,
    Remote(RpcError),
}

impl std::fmt::Display for RpcClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "transport error: {error}"),
            Self::Serialization(error) => write!(f, "JSON serialization error: {error}"),
            Self::InvalidResponse(message) => write!(f, "invalid JSON-RPC response: {message}"),
            Self::InvalidConfiguration(message) => {
                write!(f, "invalid client configuration: {message}")
            }
            Self::MissingResponse => f.write_str("server did not return a response"),
            Self::IdMismatch => f.write_str("response ID does not match request ID"),
            Self::Timeout { method, timeout } => write!(
                f,
                "request `{method}` timed out after {} ms",
                timeout.as_millis()
            ),
            Self::Cancelled { method } => write!(f, "request `{method}` was cancelled"),
            Self::SessionClosed => f.write_str("client session is closed"),
            Self::Remote(error) => write!(f, "remote error {}: {}", error.code, error.message),
        }
    }
}

impl std::error::Error for RpcClientError {}

/// Policy for transport-level liveness checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientLivenessConfig {
    pub ping_frequency: Option<Duration>,
    pub max_missed_pongs: u32,
}

impl Default for ClientLivenessConfig {
    fn default() -> Self {
        Self {
            ping_frequency: None,
            max_missed_pongs: DEFAULT_MAX_MISSED_PONGS,
        }
    }
}

/// Recovery policy after transport-level liveness failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub restore_before_reconnect: bool,
    pub reconnect_cooldown: Duration,
    pub max_reconnect_backoff: Duration,
    pub reconnect_jitter: Duration,
    pub max_reconnect_elapsed: Option<Duration>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            restore_before_reconnect: true,
            reconnect_cooldown: Duration::ZERO,
            max_reconnect_backoff: Duration::ZERO,
            reconnect_jitter: Duration::ZERO,
            max_reconnect_elapsed: None,
        }
    }
}

/// Client-side transport/session limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportLimits {
    pub outbound_queue_capacity: usize,
    pub max_in_flight_requests: usize,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            outbound_queue_capacity: DEFAULT_OUTBOUND_QUEUE_CAPACITY,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
        }
    }
}

/// Immutable client policy/config snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcClientConfig {
    pub request_timeout: Duration,
    pub liveness: ClientLivenessConfig,
    pub reconnect: ReconnectPolicy,
    pub limits: TransportLimits,
}

impl Default for RpcClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            liveness: ClientLivenessConfig::default(),
            reconnect: ReconnectPolicy::default(),
            limits: TransportLimits::default(),
        }
    }
}

#[derive(Default)]
struct ClientMetrics {
    active_requests: AtomicUsize,
    outbound_queue_depth: AtomicUsize,
    rpc_calls_total: AtomicU64,
    rpc_errors_total: AtomicU64,
    request_timeouts_total: AtomicU64,
    cancelled_requests_total: AtomicU64,
    reconnect_attempts: AtomicU64,
    missed_pong_streak: AtomicU32,
}

/// Observable client metrics/health snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientMetricsSnapshot {
    pub active_requests: usize,
    pub outbound_queue_depth: usize,
    pub rpc_calls_total: u64,
    pub rpc_errors_total: u64,
    pub request_timeouts_total: u64,
    pub cancelled_requests_total: u64,
    pub reconnect_attempts: u64,
    pub missed_pong_streak: u32,
}

impl ClientMetrics {
    fn snapshot(&self) -> ClientMetricsSnapshot {
        ClientMetricsSnapshot {
            active_requests: self.active_requests.load(Ordering::Relaxed),
            outbound_queue_depth: self.outbound_queue_depth.load(Ordering::Relaxed),
            rpc_calls_total: self.rpc_calls_total.load(Ordering::Relaxed),
            rpc_errors_total: self.rpc_errors_total.load(Ordering::Relaxed),
            request_timeouts_total: self.request_timeouts_total.load(Ordering::Relaxed),
            cancelled_requests_total: self.cancelled_requests_total.load(Ordering::Relaxed),
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
            missed_pong_streak: self.missed_pong_streak.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
struct RequestContext {
    method: String,
    request_id: Option<RpcId>,
    expects_response: bool,
}

#[derive(Clone)]
struct RpcClientState<T> {
    transport: Arc<T>,
    config: Arc<RwLock<RpcClientConfig>>,
    session: RpcSessionHandle,
    next_id: Arc<AtomicU64>,
    ping_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    metrics: Arc<ClientMetrics>,
    closed: Arc<AtomicBool>,
}

/// A request to include in a JSON-RPC batch.
#[derive(Clone, Debug)]
pub enum BatchRequest {
    Call {
        method: String,
        params: Option<Value>,
    },
    Notification {
        method: String,
        params: Option<Value>,
    },
}

impl BatchRequest {
    pub fn call(method: impl Into<String>, params: Option<Value>) -> Self {
        Self::Call {
            method: method.into(),
            params,
        }
    }

    pub fn notification(method: impl Into<String>, params: Option<Value>) -> Self {
        Self::Notification {
            method: method.into(),
            params,
        }
    }
}

/// Result for one call in a batch, in the same order as the input calls.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchResponse {
    pub id: RpcId,
    pub result: Result<Value, RpcError>,
}

/// Cloneable client. Clones share the request ID sequence and transport.
pub struct RpcClient<T> {
    state: Arc<RpcClientState<T>>,
}

impl<T> Clone for RpcClient<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> RpcClient<T>
where
    T: RpcClientTransport,
{
    pub fn new(transport: T) -> Self {
        Self::new_with_config(transport, RpcClientConfig::default())
            .expect("default RpcClientConfig must be valid")
    }

    pub fn new_with_config(transport: T, config: RpcClientConfig) -> Result<Self, RpcClientError> {
        validate_client_config(&config)?;
        let transport = Arc::new(transport);
        let metrics = Arc::new(ClientMetrics::default());
        let session = RpcSessionHandle::spawn(
            Arc::clone(&transport),
            config.limits.clone(),
            Arc::clone(&metrics),
        );
        Ok(Self {
            state: Arc::new(RpcClientState {
                transport,
                config: Arc::new(RwLock::new(config)),
                session,
                next_id: Arc::new(AtomicU64::new(1)),
                ping_task: Arc::new(Mutex::new(None)),
                metrics,
                closed: Arc::new(AtomicBool::new(false)),
            }),
        })
    }

    /// Immutable snapshot of the current client policy.
    pub fn config(&self) -> RpcClientConfig {
        self.state
            .config
            .read()
            .expect("client config lock poisoned")
            .clone()
    }

    /// Observable health/metrics counters.
    pub fn metrics(&self) -> ClientMetricsSnapshot {
        self.state.metrics.snapshot()
    }

    /// Gracefully close the underlying transport and stop the shared client
    /// session. Shared by all clones.
    pub async fn close(&self) -> Result<(), RpcClientError> {
        if self.state.closed.swap(true, Ordering::SeqCst) {
            return self
                .state
                .transport
                .close()
                .await
                .map_err(RpcClientError::Transport);
        }
        self.stop_ping_task().await;
        self.state.session.shutdown().await;
        self.state
            .transport
            .close()
            .await
            .map_err(RpcClientError::Transport)
    }

    /// Replace the full policy/config atomically.
    pub async fn set_config(&self, config: RpcClientConfig) -> Result<(), RpcClientError> {
        validate_client_config(&config)?;
        {
            let mut current = self
                .state
                .config
                .write()
                .expect("client config lock poisoned");
            *current = config.clone();
        }
        self.state.session.set_limits(config.limits).await?;
        match config.liveness.ping_frequency {
            Some(frequency) => {
                self.set_ping_frequency(frequency.as_millis() as u64)
                    .await?
            }
            None => self.set_ping_frequency(0).await?,
        }
        Ok(())
    }

    /// Configure the timeout applied to each JSON-RPC request.
    pub fn set_request_timeout(&self, timeout: Duration) -> Result<(), RpcClientError> {
        if timeout.is_zero() {
            return Err(RpcClientError::InvalidConfiguration(
                "request_timeout must be greater than zero".to_owned(),
            ));
        }
        self.state
            .config
            .write()
            .expect("client config lock poisoned")
            .request_timeout = timeout;
        Ok(())
    }

    /// Configure the timeout applied to each JSON-RPC request in milliseconds.
    pub fn set_request_timeout_millis(&self, timeout_ms: u64) -> Result<(), RpcClientError> {
        self.set_request_timeout(Duration::from_millis(timeout_ms))
    }

    /// Configure transport liveness behavior.
    pub async fn set_liveness_config(
        &self,
        config: ClientLivenessConfig,
    ) -> Result<(), RpcClientError> {
        validate_liveness_config(&config)?;
        self.state
            .config
            .write()
            .expect("client config lock poisoned")
            .liveness = config.clone();
        match config.ping_frequency {
            Some(frequency) => self.set_ping_frequency(frequency.as_millis() as u64).await,
            None => self.set_ping_frequency(0).await,
        }
    }

    /// Configure transport reconnect behavior.
    pub fn set_reconnect_policy(&self, policy: ReconnectPolicy) -> Result<(), RpcClientError> {
        validate_reconnect_policy(&policy)?;
        self.state
            .config
            .write()
            .expect("client config lock poisoned")
            .reconnect = policy;
        Ok(())
    }

    /// Configure outbound queue and in-flight request limits.
    pub async fn set_transport_limits(
        &self,
        limits: TransportLimits,
    ) -> Result<(), RpcClientError> {
        validate_transport_limits(&limits)?;
        self.state
            .config
            .write()
            .expect("client config lock poisoned")
            .limits = limits.clone();
        self.state.session.set_limits(limits).await
    }

    /// Configure periodic transport-level pings in milliseconds.
    ///
    /// `frequency_ms = 0` disables periodic pings.
    pub async fn set_ping_frequency(&self, frequency_ms: u64) -> Result<(), RpcClientError> {
        {
            let mut config = self
                .state
                .config
                .write()
                .expect("client config lock poisoned");
            config.liveness.ping_frequency =
                (frequency_ms != 0).then_some(Duration::from_millis(frequency_ms));
        }

        let mut ping_task = self.state.ping_task.lock().await;
        if let Some(task) = ping_task.take() {
            task.abort();
        }
        if frequency_ms == 0 {
            return Ok(());
        }

        let state = Arc::clone(&self.state);
        *ping_task = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(frequency_ms));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval.tick().await;
            let mut reconnect_state = ReconnectBackoffState::default();
            let transport_kind = state.transport.kind();
            let peer = state
                .transport
                .peer()
                .unwrap_or_else(|| "unknown".to_owned());
            loop {
                interval.tick().await;
                match state.transport.transport_ping().await {
                    Ok(()) => {
                        reconnect_state.reset();
                        state.metrics.missed_pong_streak.store(0, Ordering::Relaxed);
                        trace!(transport = transport_kind, peer, "transport ping -> pong");
                    }
                    Err(error) => {
                        let liveness = state
                            .config
                            .read()
                            .expect("client config lock poisoned")
                            .liveness
                            .clone();
                        let missed_pongs = state
                            .metrics
                            .missed_pong_streak
                            .fetch_add(1, Ordering::Relaxed)
                            .saturating_add(1);
                        warn!(
                            transport = transport_kind,
                            peer,
                            missed_pongs,
                            error = %error,
                            "transport ping failed"
                        );
                        let threshold = liveness.max_missed_pongs.max(1);
                        if missed_pongs < threshold {
                            continue;
                        }
                        state
                            .metrics
                            .reconnect_attempts
                            .fetch_add(1, Ordering::Relaxed);
                        let reconnect = state
                            .config
                            .read()
                            .expect("client config lock poisoned")
                            .reconnect
                            .clone();
                        if reconnect.restore_before_reconnect {
                            info!(
                                transport = transport_kind,
                                peer,
                                missed_pongs,
                                "missed pong threshold reached, attempting restore"
                            );
                            match state.transport.restore().await {
                                Ok(()) => {
                                    reconnect_state.reset();
                                    state.metrics.missed_pong_streak.store(0, Ordering::Relaxed);
                                    continue;
                                }
                                Err(restore_error) => {
                                    warn!(
                                        transport = transport_kind,
                                        peer,
                                        error = %restore_error,
                                        "transport restore failed, reconnecting"
                                    );
                                }
                            }
                        }
                        let Some(backoff) = reconnect_state.next_delay(&reconnect) else {
                            warn!(
                                transport = transport_kind,
                                peer, "reconnect budget exhausted for current failure streak"
                            );
                            reconnect_state.reset();
                            continue;
                        };
                        if !backoff.is_zero() {
                            trace!(
                                transport = transport_kind,
                                peer,
                                delay_ms = backoff.as_millis() as u64,
                                "waiting before reconnect attempt"
                            );
                            tokio::time::sleep(backoff).await;
                        }
                        match state.transport.reconnect().await {
                            Ok(()) => {
                                reconnect_state.reset();
                                state.metrics.missed_pong_streak.store(0, Ordering::Relaxed);
                                info!(transport = transport_kind, peer, "transport reconnected");
                            }
                            Err(reconnect_error) => {
                                error!(
                                    transport = transport_kind,
                                    peer,
                                    error = %reconnect_error,
                                    "transport reconnect failed"
                                );
                            }
                        }
                    }
                }
            }
        }));
        Ok(())
    }

    /// Configure how many consecutive missed pong ticks trigger recovery.
    pub async fn set_max_missed_pongs(&self, count: u32) -> Result<(), RpcClientError> {
        if count == 0 {
            return Err(RpcClientError::InvalidConfiguration(
                "max_missed_pongs must be at least 1".to_owned(),
            ));
        }
        self.state
            .config
            .write()
            .expect("client config lock poisoned")
            .liveness
            .max_missed_pongs = count;
        Ok(())
    }

    /// Perform a transport-level liveness check that bypasses JSON-RPC.
    pub async fn transport_ping(&self) -> Result<(), RpcClientError> {
        self.state
            .transport
            .transport_ping()
            .await
            .map_err(RpcClientError::Transport)
    }

    /// Perform a call with serializable structured parameters and decode the
    /// returned result into a Rust type.
    pub async fn request<P, R>(
        &self,
        method: impl Into<String>,
        params: P,
    ) -> Result<R, RpcClientError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let params = serde_json::to_value(params).map_err(RpcClientError::Serialization)?;
        let result = self.request_value(method, Some(params)).await?;
        serde_json::from_value(result).map_err(RpcClientError::Serialization)
    }

    /// Perform a call and return its result as JSON.
    pub async fn request_value(
        &self,
        method: impl Into<String>,
        params: Option<Value>,
    ) -> Result<Value, RpcClientError> {
        let method = method.into();
        let id = self.next_id();
        let request = RpcRequest::call(method.clone(), params, id.clone());
        let payload = serde_json::to_vec(&request).map_err(RpcClientError::Serialization)?;
        let response = self
            .dispatch_raw(
                RequestContext {
                    method,
                    request_id: Some(id.clone()),
                    expects_response: true,
                },
                Bytes::from(payload),
                true,
            )
            .await?
            .wait()
            .await?
            .ok_or(RpcClientError::MissingResponse)?;
        let value: Value =
            serde_json::from_slice(&response).map_err(RpcClientError::Serialization)?;
        let (response_id, result) = parse_response(value)?;
        if response_id != id {
            return Err(RpcClientError::IdMismatch);
        }
        result.map_err(RpcClientError::Remote)
    }

    /// Perform a call without a `params` member.
    pub async fn request_without_params<R: DeserializeOwned>(
        &self,
        method: impl Into<String>,
    ) -> Result<R, RpcClientError> {
        let value = self.request_value(method, None).await?;
        serde_json::from_value(value).map_err(RpcClientError::Serialization)
    }

    /// Send a notification. JSON-RPC notifications have no response by
    /// definition, so any returned transport body is ignored.
    pub async fn notify(
        &self,
        method: impl Into<String>,
        params: Option<Value>,
    ) -> Result<(), RpcClientError> {
        let method = method.into();
        let request = RpcRequest::notification(method.clone(), params);
        let payload = serde_json::to_vec(&request).map_err(RpcClientError::Serialization)?;
        self.dispatch_raw(
            RequestContext {
                method,
                request_id: None,
                expects_response: false,
            },
            Bytes::from(payload),
            false,
        )
        .await?
        .wait()
        .await?;
        Ok(())
    }

    /// Send a batch and correlate each response by ID, independent of server
    /// response order. Notifications are omitted from the returned vector.
    pub async fn batch(
        &self,
        requests: Vec<BatchRequest>,
    ) -> Result<Vec<BatchResponse>, RpcClientError> {
        if requests.is_empty() {
            return Err(RpcClientError::InvalidResponse(
                "a batch must contain at least one request".to_owned(),
            ));
        }

        let mut wire = Vec::with_capacity(requests.len());
        let mut expected = Vec::new();
        for request in requests {
            match request {
                BatchRequest::Call { method, params } => {
                    let id = self.next_id();
                    expected.push(id.clone());
                    wire.push(
                        serde_json::to_value(RpcRequest::call(method, params, id))
                            .map_err(RpcClientError::Serialization)?,
                    );
                }
                BatchRequest::Notification { method, params } => {
                    wire.push(
                        serde_json::to_value(RpcRequest::notification(method, params))
                            .map_err(RpcClientError::Serialization)?,
                    );
                }
            }
        }

        let expects_response = !expected.is_empty();
        let payload = serde_json::to_vec(&wire).map_err(RpcClientError::Serialization)?;
        let response = self
            .dispatch_raw(
                RequestContext {
                    method: "batch".to_owned(),
                    request_id: None,
                    expects_response,
                },
                Bytes::from(payload),
                expects_response,
            )
            .await?
            .wait()
            .await?;

        if !expects_response {
            return Ok(Vec::new());
        }

        let response = response.ok_or(RpcClientError::MissingResponse)?;
        let values: Vec<Value> =
            serde_json::from_slice(&response).map_err(RpcClientError::Serialization)?;
        let expected_ids: HashSet<_> = expected.iter().cloned().collect();
        let mut responses = HashMap::with_capacity(values.len());
        for value in values {
            let (id, result) = parse_response(value)?;
            if !expected_ids.contains(&id) || responses.insert(id, result).is_some() {
                return Err(RpcClientError::InvalidResponse(
                    "batch response contains an unknown or duplicate ID".to_owned(),
                ));
            }
        }

        expected
            .into_iter()
            .map(|id| {
                responses
                    .remove(&id)
                    .map(|result| BatchResponse { id, result })
                    .ok_or(RpcClientError::MissingResponse)
            })
            .collect()
    }

    fn next_id(&self) -> RpcId {
        RpcId::number(self.state.next_id.fetch_add(1, Ordering::Relaxed))
    }

    async fn dispatch_raw(
        &self,
        context: RequestContext,
        payload: Bytes,
        expects_response: bool,
    ) -> Result<PendingResponse, RpcClientError> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(RpcClientError::SessionClosed);
        }
        let timeout = self
            .state
            .config
            .read()
            .expect("client config lock poisoned")
            .request_timeout;
        self.state
            .session
            .dispatch(context, payload, expects_response, timeout)
            .await
    }
}

impl<T> RpcClient<T> {
    async fn stop_ping_task(&self) {
        if let Some(task) = self.state.ping_task.lock().await.take() {
            task.abort();
        }
    }
}

#[derive(Default)]
struct ReconnectBackoffState {
    started_at: Option<Instant>,
    reconnect_attempts: u32,
}

impl ReconnectBackoffState {
    fn reset(&mut self) {
        self.started_at = None;
        self.reconnect_attempts = 0;
    }

    fn next_delay(&mut self, policy: &ReconnectPolicy) -> Option<Duration> {
        let now = Instant::now();
        let started_at = self.started_at.get_or_insert(now);
        if let Some(limit) = policy.max_reconnect_elapsed
            && now.duration_since(*started_at) > limit
        {
            return None;
        }

        let base = exponential_backoff(
            policy.reconnect_cooldown,
            policy.max_reconnect_backoff,
            self.reconnect_attempts,
        );
        self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
        Some(base.saturating_add(jitter(policy.reconnect_jitter)))
    }
}

fn exponential_backoff(initial: Duration, max: Duration, attempts: u32) -> Duration {
    if initial.is_zero() {
        return Duration::ZERO;
    }
    let factor = 1u32.checked_shl(attempts.min(20)).unwrap_or(u32::MAX) as u128;
    let nanos = initial.as_nanos().saturating_mul(factor);
    let bounded = if max.is_zero() {
        nanos
    } else {
        nanos.min(max.as_nanos())
    };
    duration_from_nanos(bounded)
}

fn jitter(max: Duration) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    let modulus = max.as_nanos().min(u64::MAX as u128) as u64;
    if modulus == 0 {
        return Duration::ZERO;
    }
    let sample = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(0)
        % (modulus.saturating_add(1));
    Duration::from_nanos(sample)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let secs = (nanos / 1_000_000_000).min(u64::MAX as u128) as u64;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;
    Duration::new(secs, subsec_nanos)
}

fn validate_liveness_config(config: &ClientLivenessConfig) -> Result<(), RpcClientError> {
    if config.max_missed_pongs == 0 {
        return Err(RpcClientError::InvalidConfiguration(
            "max_missed_pongs must be at least 1".to_owned(),
        ));
    }
    Ok(())
}

fn validate_transport_limits(limits: &TransportLimits) -> Result<(), RpcClientError> {
    if limits.outbound_queue_capacity == 0 {
        return Err(RpcClientError::InvalidConfiguration(
            "outbound_queue_capacity must be at least 1".to_owned(),
        ));
    }
    if limits.max_in_flight_requests == 0 {
        return Err(RpcClientError::InvalidConfiguration(
            "max_in_flight_requests must be at least 1".to_owned(),
        ));
    }
    Ok(())
}

fn validate_reconnect_policy(policy: &ReconnectPolicy) -> Result<(), RpcClientError> {
    if !policy.max_reconnect_backoff.is_zero()
        && !policy.reconnect_cooldown.is_zero()
        && policy.max_reconnect_backoff < policy.reconnect_cooldown
    {
        return Err(RpcClientError::InvalidConfiguration(
            "max_reconnect_backoff must be greater than or equal to reconnect_cooldown".to_owned(),
        ));
    }
    if matches!(policy.max_reconnect_elapsed, Some(limit) if limit.is_zero()) {
        return Err(RpcClientError::InvalidConfiguration(
            "max_reconnect_elapsed must be greater than zero when set".to_owned(),
        ));
    }
    Ok(())
}

fn validate_client_config(config: &RpcClientConfig) -> Result<(), RpcClientError> {
    if config.request_timeout.is_zero() {
        return Err(RpcClientError::InvalidConfiguration(
            "request_timeout must be greater than zero".to_owned(),
        ));
    }
    validate_liveness_config(&config.liveness)?;
    validate_transport_limits(&config.limits)?;
    validate_reconnect_policy(&config.reconnect)?;
    Ok(())
}

struct PendingResponse {
    token: u64,
    context: RequestContext,
    cancel: CancellationToken,
    session: mpsc::Sender<SessionCommand>,
    receiver: Option<oneshot::Receiver<Result<Option<Bytes>, RpcClientError>>>,
    finished: bool,
}

impl PendingResponse {
    async fn wait(mut self) -> Result<Option<Bytes>, RpcClientError> {
        let receiver = self
            .receiver
            .take()
            .expect("pending response receiver missing");
        let result = match receiver.await {
            Ok(result) => result,
            Err(_) => Err(RpcClientError::SessionClosed),
        };
        self.finished = true;
        result
    }
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.cancel.cancel();
        let _ = self
            .session
            .try_send(SessionCommand::Cancel { token: self.token });
        trace!(
            method = self.context.method,
            expects_response = self.context.expects_response,
            "client request future dropped"
        );
    }
}

#[derive(Clone)]
struct RpcSessionHandle {
    tx: mpsc::Sender<SessionCommand>,
    next_token: Arc<AtomicU64>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
    metrics: Arc<ClientMetrics>,
    outbound_queue_capacity: usize,
    max_in_flight_requests: Arc<AtomicUsize>,
}

impl RpcSessionHandle {
    fn spawn<T>(transport: Arc<T>, limits: TransportLimits, metrics: Arc<ClientMetrics>) -> Self
    where
        T: RpcClientTransport,
    {
        let (tx, rx) = mpsc::channel(limits.outbound_queue_capacity);
        let tx_clone = tx.clone();
        let task = tokio::spawn(run_session(
            transport,
            rx,
            tx_clone,
            Arc::clone(&metrics),
            Arc::new(Semaphore::new(limits.max_in_flight_requests)),
        ));
        Self {
            tx,
            next_token: Arc::new(AtomicU64::new(1)),
            task: Arc::new(Mutex::new(Some(task))),
            metrics,
            outbound_queue_capacity: limits.outbound_queue_capacity,
            max_in_flight_requests: Arc::new(AtomicUsize::new(limits.max_in_flight_requests)),
        }
    }

    async fn dispatch(
        &self,
        context: RequestContext,
        payload: Bytes,
        expects_response: bool,
        timeout: Duration,
    ) -> Result<PendingResponse, RpcClientError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cancel = CancellationToken::new();
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .outbound_queue_depth
            .fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(SessionCommand::Dispatch(DispatchCommand {
                token,
                context: context.clone(),
                payload,
                expects_response,
                timeout,
                cancel: cancel.clone(),
                response: response_tx,
            }))
            .await
            .map_err(|_| {
                self.metrics
                    .outbound_queue_depth
                    .fetch_sub(1, Ordering::Relaxed);
                RpcClientError::SessionClosed
            })?;
        Ok(PendingResponse {
            token,
            context,
            cancel,
            session: self.tx.clone(),
            receiver: Some(response_rx),
            finished: false,
        })
    }

    async fn set_limits(&self, limits: TransportLimits) -> Result<(), RpcClientError> {
        if limits.outbound_queue_capacity != self.outbound_queue_capacity {
            return Err(RpcClientError::InvalidConfiguration(
                "outbound_queue_capacity cannot be changed after the client session starts"
                    .to_owned(),
            ));
        }
        let current = self.max_in_flight_requests.load(Ordering::Relaxed);
        if limits.max_in_flight_requests < current {
            return Err(RpcClientError::InvalidConfiguration(
                "max_in_flight_requests can only grow for an active client session".to_owned(),
            ));
        }
        if limits.max_in_flight_requests == current {
            return Ok(());
        }
        self.tx
            .send(SessionCommand::SetLimits(
                limits.max_in_flight_requests.saturating_sub(current),
            ))
            .await
            .map_err(|_| RpcClientError::SessionClosed)?;
        self.max_in_flight_requests
            .store(limits.max_in_flight_requests, Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self
            .tx
            .send(SessionCommand::Shutdown { reply: reply_tx })
            .await;
        let _ = reply_rx.await;
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

struct DispatchCommand {
    token: u64,
    context: RequestContext,
    payload: Bytes,
    expects_response: bool,
    timeout: Duration,
    cancel: CancellationToken,
    response: oneshot::Sender<Result<Option<Bytes>, RpcClientError>>,
}

struct PendingEntry {
    context: RequestContext,
    started_at: Instant,
    cancel: CancellationToken,
    response: oneshot::Sender<Result<Option<Bytes>, RpcClientError>>,
}

enum SessionCommand {
    Dispatch(DispatchCommand),
    Complete {
        token: u64,
        result: Result<Option<Bytes>, RpcClientError>,
    },
    Cancel {
        token: u64,
    },
    SetLimits(usize),
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

async fn run_session<T>(
    transport: Arc<T>,
    mut rx: mpsc::Receiver<SessionCommand>,
    tx: mpsc::Sender<SessionCommand>,
    metrics: Arc<ClientMetrics>,
    permits: Arc<Semaphore>,
) where
    T: RpcClientTransport,
{
    let mut pending: HashMap<u64, PendingEntry> = HashMap::new();
    let transport_kind = transport.kind();
    let peer = transport.peer().unwrap_or_else(|| "unknown".to_owned());
    while let Some(command) = rx.recv().await {
        match command {
            SessionCommand::Dispatch(command) => {
                metrics.outbound_queue_depth.fetch_sub(1, Ordering::Relaxed);
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                    let _ = command.response.send(Err(RpcClientError::SessionClosed));
                    continue;
                };
                metrics.active_requests.fetch_add(1, Ordering::Relaxed);
                pending.insert(
                    command.token,
                    PendingEntry {
                        context: command.context.clone(),
                        started_at: Instant::now(),
                        cancel: command.cancel.clone(),
                        response: command.response,
                    },
                );

                let transport = Arc::clone(&transport);
                let tx = tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let request = transport.send_cancelable(
                        command.payload,
                        command.expects_response,
                        command.cancel.clone(),
                    );
                    tokio::pin!(request);
                    let result = tokio::select! {
                        result = &mut request => match result {
                            Ok(response) => Ok(response),
                            Err(error) => {
                                if error.to_string() == "request cancelled" {
                                    Err(RpcClientError::Cancelled {
                                        method: command.context.method.clone(),
                                    })
                                } else {
                                    Err(RpcClientError::Transport(error))
                                }
                            }
                        },
                        _ = command.cancel.cancelled() => {
                            let _ = (&mut request).await;
                            Err(RpcClientError::Cancelled {
                                method: command.context.method.clone(),
                            })
                        }
                        _ = tokio::time::sleep(command.timeout) => {
                            command.cancel.cancel();
                            let _ = (&mut request).await;
                            Err(RpcClientError::Timeout {
                                method: command.context.method.clone(),
                                timeout: command.timeout,
                            })
                        }
                    };
                    let _ = tx
                        .send(SessionCommand::Complete {
                            token: command.token,
                            result,
                        })
                        .await;
                });
            }
            SessionCommand::Complete { token, result } => {
                if let Some(entry) = pending.remove(&token) {
                    metrics.active_requests.fetch_sub(1, Ordering::Relaxed);
                    metrics.rpc_calls_total.fetch_add(1, Ordering::Relaxed);
                    let latency = entry.started_at.elapsed();
                    log_client_completion(transport_kind, &peer, &entry.context, latency, &result);
                    if matches!(
                        result,
                        Err(RpcClientError::Transport(_))
                            | Err(RpcClientError::Timeout { .. })
                            | Err(RpcClientError::Remote(_))
                            | Err(RpcClientError::Cancelled { .. })
                    ) {
                        metrics.rpc_errors_total.fetch_add(1, Ordering::Relaxed);
                    }
                    if matches!(result, Err(RpcClientError::Timeout { .. })) {
                        metrics
                            .request_timeouts_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    if matches!(result, Err(RpcClientError::Cancelled { .. })) {
                        metrics
                            .cancelled_requests_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = entry.response.send(result);
                }
            }
            SessionCommand::Cancel { token } => {
                if let Some(entry) = pending.get(&token) {
                    entry.cancel.cancel();
                }
            }
            SessionCommand::SetLimits(delta) => {
                if delta > 0 {
                    permits.add_permits(delta);
                }
            }
            SessionCommand::Shutdown { reply } => {
                for (_, entry) in pending.drain() {
                    entry.cancel.cancel();
                    let _ = entry.response.send(Err(RpcClientError::SessionClosed));
                }
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn log_client_completion(
    transport: &str,
    peer: &str,
    context: &RequestContext,
    latency: Duration,
    result: &Result<Option<Bytes>, RpcClientError>,
) {
    let request_id = context
        .request_id
        .as_ref()
        .map(serialize_request_id)
        .unwrap_or_else(|| "notification".to_owned());
    match result {
        Ok(Some(response)) => {
            let response_code = extract_response_code(response);
            debug!(
                transport,
                peer,
                request_id,
                method = context.method,
                latency_ms = latency.as_millis() as u64,
                response_code,
                "json-rpc call completed"
            );
            trace!(transport, peer, payload = %String::from_utf8_lossy(response), "json-rpc response payload");
        }
        Ok(None) => {
            debug!(
                transport,
                peer,
                request_id,
                method = context.method,
                latency_ms = latency.as_millis() as u64,
                "json-rpc notification completed"
            );
        }
        Err(RpcClientError::Remote(error)) => {
            warn!(
                transport,
                peer,
                request_id,
                method = context.method,
                latency_ms = latency.as_millis() as u64,
                response_code = error.code,
                "remote json-rpc error"
            );
        }
        Err(error) => {
            warn!(
                transport,
                peer,
                request_id,
                method = context.method,
                latency_ms = latency.as_millis() as u64,
                error = %error,
                "json-rpc request failed"
            );
        }
    }
}

fn serialize_request_id(id: &RpcId) -> String {
    serde_json::to_string(id).unwrap_or_else(|_| "<invalid-id>".to_owned())
}

fn extract_response_code(response: &[u8]) -> i64 {
    serde_json::from_slice::<Value>(response)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_i64)
        })
        .unwrap_or(0)
}

fn parse_response(value: Value) -> Result<(RpcId, Result<Value, RpcError>), RpcClientError> {
    let object = value
        .as_object()
        .ok_or_else(|| RpcClientError::InvalidResponse("expected an object".to_owned()))?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some(VERSION) {
        return Err(RpcClientError::InvalidResponse(
            "jsonrpc must be \"2.0\"".to_owned(),
        ));
    }
    let id: RpcId = serde_json::from_value(
        object
            .get("id")
            .cloned()
            .ok_or_else(|| RpcClientError::InvalidResponse("missing id".to_owned()))?,
    )
    .map_err(RpcClientError::Serialization)?;
    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok((id, Ok(result.clone()))),
        (None, Some(error)) => {
            let error =
                serde_json::from_value(error.clone()).map_err(RpcClientError::Serialization)?;
            Ok((id, Err(error)))
        }
        _ => Err(RpcClientError::InvalidResponse(
            "exactly one of result and error is required".to_owned(),
        )),
    }
}
