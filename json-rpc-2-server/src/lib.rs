//! JSON-RPC 2.0 server implementation and its transport adapters.

use arc_swap::ArcSwap;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
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
pub use json_rpc_2_transport::{JsonRpcProtocol, ProtocolFuture, ServerLimits, TransportContext};
pub type TransportLimits = ServerLimits;
mod plugin;
pub use plugin::{MaintenanceTask, PluginError, PluginFuture, PluginHandle, ServerPlugin};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

/// Errors encountered while registering a server method.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistrationError {
    ReservedMethod,
    DuplicateMethod,
    UnknownMethod,
}

impl RpcServer {
    /// Attach JSON Schema and synthetic example params to an existing method at startup.
    /// Call before registering aliases, which copy the target's documentation.
    pub fn document_method(
        &self,
        name: &str,
        params_schema: Value,
        example_params: Value,
    ) -> Result<(), RegistrationError> {
        if !self.methods.load().contains_key(name) {
            return Err(RegistrationError::UnknownMethod);
        }
        let mut next = (**self.documentation.load()).clone();
        next.insert(
            name.to_owned(),
            MethodDocumentation {
                params_schema,
                example_params,
            },
        );
        self.documentation.store(Arc::new(next));
        Ok(())
    }

    pub fn method_descriptions(&self) -> Vec<MethodDescription> {
        let documentation = self.documentation.load();
        self.method_names()
            .into_iter()
            .map(|name| {
                let builtin = MethodDocumentation {
                    params_schema: json!({"oneOf":[
                        {"type":"object", "properties":{"describe":{"type":"boolean"}},
                         "additionalProperties":false},
                        {"type":"array","maxItems":0}
                    ]}),
                    example_params: json!({"describe":true}),
                };
                let doc = if name == "rpc.list_methods" {
                    Some(&builtin)
                } else {
                    documentation.get(&name)
                };
                MethodDescription {
                    params_schema: doc.map(|doc| doc.params_schema.clone()),
                    example_request: doc.map(|doc| {
                        json!({
                            "jsonrpc":"2.0", "id":1, "method":name, "params":doc.example_params
                        })
                    }),
                    name,
                }
            })
            .collect()
    }
}

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReservedMethod => f.write_str("method names beginning with `rpc.` are reserved"),
            Self::DuplicateMethod => f.write_str("method is already registered"),
            Self::UnknownMethod => f.write_str("method or alias target is not registered"),
        }
    }
}

impl std::error::Error for RegistrationError {}

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>;
type Handler = Arc<dyn Fn(Value, Option<TransportContext>) -> HandlerFuture + Send + Sync>;
type MethodMap = HashMap<String, Handler>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MethodDescription {
    pub name: String,
    pub params_schema: Option<Value>,
    pub example_request: Option<Value>,
}

#[derive(Clone)]
struct MethodDocumentation {
    params_schema: Value,
    example_params: Value,
}

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
    documentation: Arc<ArcSwap<HashMap<String, MethodDocumentation>>>,
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
            documentation: Arc::new(ArcSwap::from_pointee(HashMap::new())),
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

    /// Sorted snapshot of callable names, including the built-in discovery method.
    pub fn method_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.methods.load().keys().cloned().collect();
        names.push("rpc.list_methods".to_owned());
        names.sort();
        names
    }

    /// Register an alias to an existing handler, including legacy `rpc.*` aliases.
    pub fn register_alias(
        &self,
        alias: impl Into<String>,
        target: &str,
    ) -> Result<(), RegistrationError> {
        let alias = alias.into();
        if alias == "rpc.list_methods" {
            return Err(RegistrationError::ReservedMethod);
        }
        let current = self.methods.load_full();
        if current.contains_key(&alias) {
            return Err(RegistrationError::DuplicateMethod);
        }
        let handler = current
            .get(target)
            .cloned()
            .ok_or(RegistrationError::UnknownMethod)?;
        let mut next = (*current).clone();
        next.insert(alias.clone(), handler);
        self.methods.store(Arc::new(next));
        let current = self.documentation.load_full();
        if let Some(doc) = current.get(target) {
            let mut next = (*current).clone();
            next.insert(alias, doc.clone());
            self.documentation.store(Arc::new(next));
        }
        Ok(())
    }

    /// Process a complete JSON-RPC message using the JSON-RPC 2.0 rules.
    ///
    /// Transport implementations should depend on [`JsonRpcProtocol`] rather
    /// than on this concrete server type.
    pub async fn handle_bytes(&self, input: &[u8]) -> Option<Bytes> {
        self.process_message(input, None).await
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
        self.register_with_context(name, move |params, _context| handler(params))
    }

    /// Register a method that also receives transport metadata when the
    /// caller provides a session-aware transport context.
    pub fn register_with_context<F, Fut>(
        &self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<(), RegistrationError>
    where
        F: Fn(Value, Option<TransportContext>) -> Fut + Send + Sync + 'static,
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
        next.insert(
            name,
            Arc::new(move |params, context| Box::pin(handler(params, context))),
        );
        self.methods.store(Arc::new(next));
        Ok(())
    }

    async fn process_message(
        &self,
        input: &[u8],
        context: Option<TransportContext>,
    ) -> Option<Bytes> {
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
                    let context = context.clone();
                    tasks.spawn(async move { server.handle_value(request, context).await });
                }

                let mut responses = Vec::new();
                while let Some(response) = tasks.join_next().await {
                    if let Some(response) = response.expect("batch docs panicked") {
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
                .handle_value(request, context)
                .await
                .map(|response| serialize(&response)),
        }
    }

    async fn handle_value(&self, value: Value, context: Option<TransportContext>) -> Option<Value> {
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
            let _ = handler(params, context).await;
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
        let response = match handler(params, context).await {
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
        if method == "rpc.list_methods" {
            let names = self.method_names();
            let descriptions = self.method_descriptions();
            return Some(Arc::new(move |params, _| {
                let names = names.clone();
                let descriptions = descriptions.clone();
                Box::pin(async move {
                    if params == json!({"describe":true}) {
                        Ok(json!(descriptions))
                    } else if params == json!({"describe":false})
                        || params.is_null()
                        || params.as_object().is_some_and(|value| value.is_empty())
                        || params.as_array().is_some_and(|value| value.is_empty())
                    {
                        Ok(json!(names))
                    } else {
                        Err(RpcError::invalid_params())
                    }
                })
            }));
        }
        self.methods.load().get(method).cloned()
    }
}

impl JsonRpcProtocol for RpcServer {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(self.process_message(input, None))
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(self.process_message(input, Some(context.clone())))
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
                documentation: Arc::new(ArcSwap::from_pointee(Default::default())),
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
            self.methods.insert(
                name,
                Arc::new(move |params, _context| Box::pin(handler(params))),
            );
            Ok(())
        }
    }

    pub type Dispatcher = RpcServer;

    pub use Builder as ServerDispatcherBuilder;
    pub use Dispatcher as ServerDispatcher;
}

pub use dispatcher::{ServerDispatcher, ServerDispatcherBuilder};

#[cfg(test)]
mod discovery_tests {
    use super::*;

    #[tokio::test]
    async fn discovery_tracks_registration_aliases_and_builder() {
        let server = RpcServer::builder()
            .register("z", |p| async { Ok(p) })
            .unwrap()
            .build();
        let clone = server.clone();
        clone.register("a", |p| async { Ok(p) }).unwrap();
        server
            .document_method("a", json!({"type":"object"}), json!({"message":"mock"}))
            .unwrap();
        server.register_alias("rpc.a", "a").unwrap();
        assert_eq!(
            server.method_names(),
            ["a", "rpc.a", "rpc.list_methods", "z"]
        );
        assert_eq!(
            server.register_alias("missing", "absent"),
            Err(RegistrationError::UnknownMethod)
        );
        assert_eq!(
            server.register_alias("rpc.list_methods", "a"),
            Err(RegistrationError::ReservedMethod)
        );
        assert_eq!(
            server.register_alias("a", "z"),
            Err(RegistrationError::DuplicateMethod)
        );
        let response = server
            .handle_bytes(br#"{"jsonrpc":"2.0","id":1,"method":"rpc.list_methods"}"#)
            .await
            .unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["result"], json!(server.method_names()));
        let response = server.handle_bytes(br#"{"jsonrpc":"2.0","id":3,"method":"rpc.list_methods","params":{"describe":true}}"#).await.unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        let descriptions: Vec<MethodDescription> =
            serde_json::from_value(response["result"].clone()).unwrap();
        assert_eq!(
            descriptions[0].params_schema,
            Some(json!({"type":"object"}))
        );
        assert_eq!(
            descriptions[1].example_request.as_ref().unwrap()["method"],
            "rpc.a"
        );
        assert_eq!(
            descriptions[1].example_request.as_ref().unwrap()["params"],
            json!({"message":"mock"})
        );
        assert!(descriptions[3].params_schema.is_none());
        assert_eq!(
            server.document_method("absent", json!({}), json!({})),
            Err(RegistrationError::UnknownMethod)
        );
        let response = server
            .handle_bytes(br#"{"jsonrpc":"2.0","id":2,"method":"rpc.list_methods","params":[1]}"#)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&response).unwrap()["error"]["code"],
            -32602
        );
        assert!(
            server
                .handle_bytes(br#"{"jsonrpc":"2.0","method":"rpc.list_methods"}"#)
                .await
                .is_none()
        );
    }
}
