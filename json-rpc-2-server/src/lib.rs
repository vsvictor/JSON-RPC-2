//! JSON-RPC 2.0 server implementation and its transport adapters.

use arc_swap::ArcSwap;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::task::JoinSet;

pub use json_rpc_2_core::*;
pub use json_rpc_2_transport::{JsonRpcProtocol, ProtocolFuture, ServerLimits};
pub type TransportLimits = ServerLimits;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

/// Errors encountered while registering a server method.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistrationError {
    ReservedMethod,
    DuplicateMethod,
}

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReservedMethod => f.write_str("method names beginning with `rpc.` are reserved"),
            Self::DuplicateMethod => f.write_str("method is already registered"),
        }
    }
}

impl std::error::Error for RegistrationError {}

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>;
type Handler = Arc<dyn Fn(Value) -> HandlerFuture + Send + Sync>;
type MethodMap = HashMap<String, Handler>;

#[derive(Default)]
struct ServerMetrics {
    active_requests: AtomicUsize,
    rpc_calls_total: AtomicU64,
    rpc_errors_total: AtomicU64,
}

/// Observable server health/metrics snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ServerMetricsSnapshot {
    pub active_requests: usize,
    pub rpc_calls_total: u64,
    pub rpc_errors_total: u64,
}

impl ServerMetrics {
    fn snapshot(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            active_requests: self.active_requests.load(Ordering::Relaxed),
            rpc_calls_total: self.rpc_calls_total.load(Ordering::Relaxed),
            rpc_errors_total: self.rpc_errors_total.load(Ordering::Relaxed),
        }
    }
}

/// A cloneable JSON-RPC server. Methods receive either the supplied
/// structured `params` value or `null` when the member was omitted.
#[derive(Clone)]
pub struct RpcServer {
    methods: Arc<ArcSwap<MethodMap>>,
    limits: ServerLimits,
    request_permits: Arc<Semaphore>,
    metrics: Arc<ServerMetrics>,
}

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServer {
    pub fn new() -> Self {
        Self::new_with_limits(ServerLimits::default())
    }

    pub fn new_with_limits(limits: ServerLimits) -> Self {
        assert!(
            limits.max_connections > 0 && limits.max_concurrent_requests > 0,
            "server limits must be greater than zero"
        );
        Self {
            methods: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            request_permits: Arc::new(Semaphore::new(limits.max_concurrent_requests)),
            limits,
            metrics: Arc::new(ServerMetrics::default()),
        }
    }

    pub fn builder() -> ServerDispatcherBuilder<dispatcher::Empty> {
        ServerDispatcherBuilder::new()
    }

    pub fn limits(&self) -> &ServerLimits {
        &self.limits
    }

    pub fn metrics(&self) -> ServerMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Process a complete JSON-RPC message using the JSON-RPC 2.0 rules.
    ///
    /// Transport implementations should depend on [`JsonRpcProtocol`] rather
    /// than on this concrete server type.
    pub async fn handle_bytes(&self, input: &[u8]) -> Option<Bytes> {
        self.process_message(input).await
    }

    /// Register a method whose handler may perform asynchronous work.
    ///
    /// Registration is copy-on-write and intended for startup time. Runtime
    /// dispatch is immutable and lock-free.
    pub fn register<F, Fut>(
        &self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<(), RegistrationError>
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, RpcError>> + Send + 'static,
    {
        let name = name.into();
        if name.starts_with("rpc.") {
            return Err(RegistrationError::ReservedMethod);
        }

        let current = self.methods.load_full();
        if current.contains_key(&name) {
            return Err(RegistrationError::DuplicateMethod);
        }

        let mut next = (*current).clone();
        next.insert(name, Arc::new(move |params| Box::pin(handler(params))));
        self.methods.store(Arc::new(next));
        Ok(())
    }

    async fn process_message(&self, input: &[u8]) -> Option<Bytes> {
        let value = match serde_json::from_slice::<Value>(input) {
            Ok(value) => value,
            Err(_) => {
                return Some(serialize(&response_error(
                    Value::Null,
                    RpcError::parse_error(),
                )));
            }
        };

        match value {
            Value::Array(batch) => {
                if batch.is_empty() {
                    return Some(serialize(&response_error(
                        Value::Null,
                        RpcError::invalid_request(),
                    )));
                }

                let mut tasks = JoinSet::new();
                for request in batch {
                    let server = self.clone();
                    tasks.spawn(async move { server.handle_value(request).await });
                }

                let mut responses = Vec::new();
                while let Some(response) = tasks.join_next().await {
                    if let Some(response) = response.expect("batch task panicked") {
                        responses.push(response);
                    }
                }

                if responses.is_empty() {
                    None
                } else {
                    Some(serialize(&Value::Array(responses)))
                }
            }
            request => self
                .handle_value(request)
                .await
                .map(|response| serialize(&response)),
        }
    }

    async fn handle_value(&self, value: Value) -> Option<Value> {
        let invalid_id = valid_id_from_object(&value).unwrap_or(Value::Null);
        let Some(object) = value.as_object() else {
            return Some(response_error(invalid_id, RpcError::invalid_request()));
        };

        let Some(version) = object.get("jsonrpc").and_then(Value::as_str) else {
            return Some(response_error(invalid_id, RpcError::invalid_request()));
        };
        let Some(method) = object.get("method").and_then(Value::as_str) else {
            return Some(response_error(invalid_id, RpcError::invalid_request()));
        };
        if version != "2.0" {
            return Some(response_error(invalid_id, RpcError::invalid_request()));
        }

        let params = match object.get("params") {
            None => Value::Null,
            Some(Value::Array(_) | Value::Object(_)) => object["params"].clone(),
            Some(_) => return Some(response_error(invalid_id, RpcError::invalid_request())),
        };

        let id = match object.get("id") {
            None => None,
            Some(id) if valid_id(id) => Some(id.clone()),
            Some(_) => return Some(response_error(Value::Null, RpcError::invalid_request())),
        };

        let Some(handler) = self.lookup(method) else {
            return id.map(|id| response_error(id, RpcError::method_not_found()));
        };

        let Some(id) = id else {
            let _permit = self
                .request_permits
                .clone()
                .acquire_owned()
                .await
                .expect("request semaphore closed");
            self.metrics.active_requests.fetch_add(1, Ordering::Relaxed);
            let _ = handler(params).await;
            self.metrics.active_requests.fetch_sub(1, Ordering::Relaxed);
            return None;
        };

        let _permit = self
            .request_permits
            .clone()
            .acquire_owned()
            .await
            .expect("request semaphore closed");
        self.metrics.active_requests.fetch_add(1, Ordering::Relaxed);
        self.metrics.rpc_calls_total.fetch_add(1, Ordering::Relaxed);
        let response = match handler(params).await {
            Ok(result) => Some(json!({"jsonrpc": "2.0", "result": result, "id": id})),
            Err(error) => {
                self.metrics
                    .rpc_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Some(response_error(id, error))
            }
        };
        self.metrics.active_requests.fetch_sub(1, Ordering::Relaxed);
        response
    }

    fn lookup(&self, method: &str) -> Option<Handler> {
        self.methods.load().get(method).cloned()
    }
}

impl JsonRpcProtocol for RpcServer {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(self.process_message(input))
    }
}

fn valid_id_from_object(value: &Value) -> Option<Value> {
    value
        .as_object()?
        .get("id")
        .filter(|id| valid_id(id))
        .cloned()
}

fn valid_id(id: &Value) -> bool {
    matches!(id, Value::Null | Value::String(_) | Value::Number(_))
}

fn response_error(id: Value, error: RpcError) -> Value {
    json!({"jsonrpc": "2.0", "error": error, "id": id})
}

fn serialize(value: &Value) -> Bytes {
    let mut buffer = BytesMut::with_capacity(256);
    serde_json::to_writer((&mut buffer).writer(), value)
        .expect("JSON values are always serializable");
    buffer.freeze()
}

mod dispatcher {
    use super::{
        Future, MethodMap, RegistrationError, RpcError, RpcServer, Semaphore, ServerLimits,
        ServerMetrics,
    };
    use arc_swap::ArcSwap;
    use std::{marker::PhantomData, sync::Arc};

    pub struct Empty;
    pub struct Ready;

    pub struct Builder<State> {
        methods: MethodMap,
        limits: ServerLimits,
        state: PhantomData<State>,
    }

    impl Builder<Empty> {
        pub fn new() -> Self {
            Self {
                methods: MethodMap::new(),
                limits: ServerLimits::default(),
                state: PhantomData,
            }
        }

        pub fn register<F, Fut>(
            self,
            name: impl Into<String>,
            handler: F,
        ) -> Result<Builder<Ready>, RegistrationError>
        where
            F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<serde_json::Value, RpcError>> + Send + 'static,
        {
            let mut next = Builder::<Ready> {
                methods: self.methods,
                limits: self.limits,
                state: PhantomData,
            };
            next.insert(name.into(), handler)?;
            Ok(next)
        }
    }

    impl Builder<Ready> {
        pub fn register<F, Fut>(
            mut self,
            name: impl Into<String>,
            handler: F,
        ) -> Result<Self, RegistrationError>
        where
            F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<serde_json::Value, RpcError>> + Send + 'static,
        {
            self.insert(name.into(), handler)?;
            Ok(self)
        }

        pub fn build(self) -> RpcServer {
            RpcServer {
                methods: Arc::new(ArcSwap::from(Arc::new(self.methods))),
                request_permits: Arc::new(Semaphore::new(self.limits.max_concurrent_requests)),
                limits: self.limits,
                metrics: Arc::new(ServerMetrics::default()),
            }
        }
    }

    impl<State> Builder<State> {
        pub fn set_limits(mut self, limits: ServerLimits) -> Self {
            self.limits = limits;
            self
        }

        pub fn set_transport_limits(self, limits: ServerLimits) -> Self {
            self.set_limits(limits)
        }

        fn insert<F, Fut>(&mut self, name: String, handler: F) -> Result<(), RegistrationError>
        where
            F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<serde_json::Value, RpcError>> + Send + 'static,
        {
            if name.starts_with("rpc.") {
                return Err(RegistrationError::ReservedMethod);
            }
            if self.methods.contains_key(&name) {
                return Err(RegistrationError::DuplicateMethod);
            }
            self.methods
                .insert(name, Arc::new(move |params| Box::pin(handler(params))));
            Ok(())
        }
    }

    pub type Dispatcher = RpcServer;

    pub use Builder as ServerDispatcherBuilder;
    pub use Dispatcher as ServerDispatcher;
}

pub use dispatcher::{ServerDispatcher, ServerDispatcherBuilder};
