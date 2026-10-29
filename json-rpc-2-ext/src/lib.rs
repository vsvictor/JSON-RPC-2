//! Application-level JSON-RPC 2.0 event subscriptions and publishing.
//!
//! The server returns a [`ClientConnection`] for each authenticated,
//! server-assigned client ID. Its receiver is the outbound notification path
//! that a bidirectional transport (such as WebSocket) must write to the peer.
//! Route methods matching [`is_extension_method`] to
//! [`EventServer::handle_connection`] and other methods to the regular dispatcher.
//!
//! The client helper constructs extension requests and accepts inbound
//! `events.event` notifications from the transport's receive loop.

use json_rpc_2_core::{RpcError, VERSION};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::mpsc;

const DEFAULT_OUTBOUND_CAPACITY: usize = 128;
const DEFAULT_CLIENT_EVENT_CAPACITY: usize = 128;
const ERR_FORBIDDEN: i64 = -32000;
const ERR_NOT_FOUND: i64 = -32001;
const ERR_DISCONNECTED: i64 = -32002;

pub const SUBSCRIBE_METHOD: &str = "events.subscribe";
pub const UNSUBSCRIBE_METHOD: &str = "events.unsubscribe";
pub const PUBLISH_METHOD: &str = "events.publish";
pub const EVENT_NOTIFICATION_METHOD: &str = "events.event";

/// Whether a method is handled by this extension.
pub fn is_extension_method(method: &str) -> bool {
    matches!(
        method,
        SUBSCRIBE_METHOD | UNSUBSCRIBE_METHOD | PUBLISH_METHOD
    )
}

/// One target for `events.publish`. Exactly one target kind is represented.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
#[serde(rename_all_fields = "camelCase")]
pub enum EventTarget {
    Client { client_id: String },
    Topic { topic: String },
}

/// Server-to-client event notification parameters.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EventNotification {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub event: String,
    pub data: Value,
    pub subscriptions: Vec<String>,
}

impl EventNotification {
    pub fn into_notification(self) -> Value {
        json!({
            "jsonrpc": VERSION,
            "method": EVENT_NOTIFICATION_METHOD,
            "params": self
        })
    }
}

/// Authorization policy for event operations.
///
/// Implementations receive the server-authenticated sender ID; no sender
/// identity is accepted from JSON-RPC parameters.
pub trait EventAuthorizer: Send + Sync + 'static {
    fn can_subscribe(&self, client_id: &str, topic: &str, event: &str) -> bool;
    fn can_publish_topic(&self, client_id: &str, topic: &str, event: &str) -> bool;
    fn can_send_to_client(&self, client_id: &str, target_client_id: &str, event: &str) -> bool;
}

/// An explicit allow-all policy, useful for trusted deployments and tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllEvents;

impl EventAuthorizer for AllowAllEvents {
    fn can_subscribe(&self, _: &str, _: &str, _: &str) -> bool {
        true
    }
    fn can_publish_topic(&self, _: &str, _: &str, _: &str) -> bool {
        true
    }
    fn can_send_to_client(&self, _: &str, _: &str, _: &str) -> bool {
        true
    }
}

#[derive(Clone)]
pub struct EventServer {
    state: Arc<ServerState>,
}

struct ServerState {
    authorizer: Arc<dyn EventAuthorizer>,
    clients: Mutex<HashMap<String, ClientEntry>>,
    next_client: AtomicU64,
    next_subscription: AtomicU64,
    outbound_capacity: usize,
}

struct ClientEntry {
    sender: mpsc::Sender<EventNotification>,
    subscriptions: Vec<Subscription>,
}

#[derive(Clone)]
struct Subscription {
    id: String,
    topic: String,
    event: String,
}

/// An active connection's outbound event stream. Dropping it removes all
/// subscriptions and disconnects the server-assigned client ID.
pub struct ClientConnection {
    client_id: String,
    receiver: mpsc::Receiver<EventNotification>,
    state: Arc<ServerState>,
}

impl ClientConnection {
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Receive the next event notification for this connection.
    pub async fn recv(&mut self) -> Option<EventNotification> {
        self.receiver.recv().await
    }
}

impl Drop for ClientConnection {
    fn drop(&mut self) {
        disconnect(&self.state, &self.client_id);
    }
}

impl EventServer {
    /// Create a server extension with bounded per-connection notification
    /// queues. A queue capacity of zero is rejected.
    pub fn new(authorizer: Arc<dyn EventAuthorizer>, outbound_capacity: usize) -> Self {
        assert!(
            outbound_capacity > 0,
            "outbound capacity must be greater than zero"
        );
        Self {
            state: Arc::new(ServerState {
                authorizer,
                clients: Mutex::new(HashMap::new()),
                next_client: AtomicU64::new(1),
                next_subscription: AtomicU64::new(1),
                outbound_capacity,
            }),
        }
    }

    pub fn with_default_capacity(authorizer: Arc<dyn EventAuthorizer>) -> Self {
        Self::new(authorizer, DEFAULT_OUTBOUND_CAPACITY)
    }

    /// Register a newly authenticated connection with a server-assigned ID.
    pub fn connect(&self) -> ClientConnection {
        let client_id = format!(
            "client-{}",
            self.state.next_client.fetch_add(1, Ordering::Relaxed)
        );
        self.connect_with_id(client_id)
            .expect("server-generated client IDs are unique")
    }

    /// Register a connection with an ID assigned by the integrating server.
    pub fn connect_with_id(
        &self,
        client_id: impl Into<String>,
    ) -> Result<ClientConnection, RpcError> {
        let client_id = client_id.into();
        if client_id.is_empty() {
            return Err(RpcError::invalid_params());
        }
        let (sender, receiver) = mpsc::channel(self.state.outbound_capacity);
        let mut clients = self
            .state
            .clients
            .lock()
            .expect("event server lock poisoned");
        if clients.contains_key(&client_id) {
            return Err(RpcError::new(
                ERR_DISCONNECTED,
                "Client ID is already connected",
            ));
        }
        clients.insert(
            client_id.clone(),
            ClientEntry {
                sender,
                subscriptions: Vec::new(),
            },
        );
        drop(clients);
        Ok(ClientConnection {
            client_id,
            receiver,
            state: Arc::clone(&self.state),
        })
    }

    /// Process an extension request using the identity bound to this
    /// connection. Prefer this over [`EventServer::handle_message`] in
    /// transport integrations.
    pub fn handle_connection(
        &self,
        connection: &ClientConnection,
        message: Value,
    ) -> Result<Option<Value>, RpcError> {
        if !Arc::ptr_eq(&self.state, &connection.state) {
            return Err(disconnected());
        }
        Ok(self.handle_message(&connection.client_id, message))
    }

    /// Process one inbound JSON-RPC request for the authenticated connection.
    /// Returns a response for calls with an ID and `None` for notifications.
    pub fn handle_message(&self, client_id: &str, message: Value) -> Option<Value> {
        let id = message
            .as_object()
            .and_then(|object| object.get("id"))
            .filter(|id| valid_id(id))
            .cloned();
        let response_id = id.clone().unwrap_or(Value::Null);
        let Some(object) = message.as_object() else {
            return Some(error_response(response_id, RpcError::invalid_request()));
        };
        if object.get("jsonrpc").and_then(Value::as_str) != Some(VERSION) {
            return Some(error_response(response_id, RpcError::invalid_request()));
        }
        let Some(method) = object.get("method").and_then(Value::as_str) else {
            return Some(error_response(response_id, RpcError::invalid_request()));
        };
        if object.get("id").is_some_and(|id| !valid_id(id)) {
            return Some(error_response(Value::Null, RpcError::invalid_request()));
        }
        let params = match object.get("params") {
            Some(Value::Object(params)) => params,
            None => {
                return self.respond(id, Err(RpcError::invalid_params()));
            }
            _ => return self.respond(id, Err(RpcError::invalid_params())),
        };

        if !self
            .state
            .clients
            .lock()
            .expect("event server lock poisoned")
            .contains_key(client_id)
        {
            return self.respond(id, Err(disconnected()));
        }
        let result = match method {
            SUBSCRIBE_METHOD => self.subscribe(client_id, params),
            UNSUBSCRIBE_METHOD => self.unsubscribe(client_id, params),
            PUBLISH_METHOD => self.publish(client_id, params),
            _ => Err(RpcError::method_not_found()),
        };
        self.respond(id, result)
    }

    fn respond(&self, id: Option<Value>, result: Result<Value, RpcError>) -> Option<Value> {
        let id = id?;
        Some(match result {
            Ok(result) => json!({"jsonrpc": VERSION, "result": result, "id": id}),
            Err(error) => error_response(id, error),
        })
    }

    fn subscribe(&self, client_id: &str, params: &Map<String, Value>) -> Result<Value, RpcError> {
        let topic = required_nonempty_string(params, "topic")?;
        let event = required_nonempty_string(params, "event")?;
        if !self.state.authorizer.can_subscribe(client_id, topic, event) {
            return Err(forbidden());
        }
        let mut clients = self
            .state
            .clients
            .lock()
            .expect("event server lock poisoned");
        let entry = clients.get_mut(client_id).ok_or_else(disconnected)?;
        let id = format!(
            "sub-{}",
            self.state.next_subscription.fetch_add(1, Ordering::Relaxed)
        );
        entry.subscriptions.push(Subscription {
            id: id.clone(),
            topic: topic.to_owned(),
            event: event.to_owned(),
        });
        Ok(json!({"subscription": id}))
    }

    fn unsubscribe(&self, client_id: &str, params: &Map<String, Value>) -> Result<Value, RpcError> {
        let subscription_id = required_nonempty_string(params, "subscription")?;
        let mut clients = self
            .state
            .clients
            .lock()
            .expect("event server lock poisoned");
        let entry = clients.get_mut(client_id).ok_or_else(disconnected)?;
        let Some(index) = entry
            .subscriptions
            .iter()
            .position(|s| s.id == subscription_id)
        else {
            return Err(RpcError::new(ERR_NOT_FOUND, "Subscription not found"));
        };
        entry.subscriptions.remove(index);
        Ok(json!({"unsubscribed": true}))
    }

    fn publish(&self, client_id: &str, params: &Map<String, Value>) -> Result<Value, RpcError> {
        let event = required_nonempty_string(params, "event")?;
        let target = params
            .get("target")
            .and_then(Value::as_object)
            .ok_or_else(RpcError::invalid_params)?;
        let data = params
            .get("data")
            .cloned()
            .ok_or_else(RpcError::invalid_params)?;
        let client_target = target.get("clientId").and_then(Value::as_str);
        let topic_target = target.get("topic").and_then(Value::as_str);
        if target.len() != 1 || (client_target.is_some() == topic_target.is_some()) {
            return Err(RpcError::invalid_params());
        }

        if let Some(target_id) = client_target {
            if target_id.is_empty() {
                return Err(RpcError::invalid_params());
            }
            if !self
                .state
                .authorizer
                .can_send_to_client(client_id, target_id, event)
            {
                return Err(forbidden());
            }
            let mut clients = self
                .state
                .clients
                .lock()
                .expect("event server lock poisoned");
            let Some(entry) = clients.get_mut(target_id) else {
                return Ok(json!({"accepted": true, "recipients": 0}));
            };
            let accepted = entry
                .sender
                .try_send(EventNotification {
                    topic: None,
                    event: event.to_owned(),
                    data,
                    subscriptions: Vec::new(),
                })
                .is_ok();
            return Ok(json!({"accepted": true, "recipients": usize::from(accepted)}));
        }

        let topic = topic_target
            .filter(|topic| !topic.is_empty())
            .ok_or_else(RpcError::invalid_params)?;
        if !self
            .state
            .authorizer
            .can_publish_topic(client_id, topic, event)
        {
            return Err(forbidden());
        }
        let mut clients = self
            .state
            .clients
            .lock()
            .expect("event server lock poisoned");
        let mut recipients = 0usize;
        for entry in clients.values_mut() {
            let matching: Vec<String> = entry
                .subscriptions
                .iter()
                .filter(|sub| sub.topic == topic && sub.event == event)
                .map(|sub| sub.id.clone())
                .collect();
            if matching.is_empty() {
                continue;
            }
            if entry
                .sender
                .try_send(EventNotification {
                    topic: Some(topic.to_owned()),
                    event: event.to_owned(),
                    data: data.clone(),
                    subscriptions: matching,
                })
                .is_ok()
            {
                recipients += 1;
            }
        }
        Ok(json!({"accepted": true, "recipients": recipients}))
    }
}

fn disconnect(state: &ServerState, client_id: &str) {
    state
        .clients
        .lock()
        .expect("event server lock poisoned")
        .remove(client_id);
}

fn required_nonempty_string<'a>(
    params: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, RpcError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(RpcError::invalid_params)
}

fn valid_id(id: &Value) -> bool {
    matches!(id, Value::Null | Value::String(_) | Value::Number(_))
}

fn error_response(id: Value, error: RpcError) -> Value {
    json!({"jsonrpc": VERSION, "error": error, "id": id})
}

fn forbidden() -> RpcError {
    RpcError::new(ERR_FORBIDDEN, "Event operation is not authorized")
}

fn disconnected() -> RpcError {
    RpcError::new(ERR_DISCONNECTED, "Client connection is not active")
}

type RequestFuture = Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>;
type RequestHandler = dyn Fn(String, Value) -> RequestFuture + Send + Sync;

/// Client-side helper for the extension methods and inbound event notifications.
///
/// The transport's receive loop must pass incoming JSON-RPC notifications to
/// [`EventClient::handle_notification`]; notifications are not request replies.
#[derive(Clone)]
pub struct EventClient {
    request: Arc<RequestHandler>,
    events_sender: mpsc::Sender<EventNotification>,
    events_receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<EventNotification>>>,
}

impl EventClient {
    pub fn new<F, Fut>(request: F) -> Self
    where
        F: Fn(String, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, RpcError>> + Send + 'static,
    {
        Self::with_event_capacity(request, DEFAULT_CLIENT_EVENT_CAPACITY)
    }

    pub fn with_event_capacity<F, Fut>(request: F, event_capacity: usize) -> Self
    where
        F: Fn(String, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, RpcError>> + Send + 'static,
    {
        assert!(
            event_capacity > 0,
            "event capacity must be greater than zero"
        );
        let (events_sender, events_receiver) = mpsc::channel(event_capacity);
        Self {
            request: Arc::new(move |method, params| Box::pin(request(method, params))),
            events_sender,
            events_receiver: Arc::new(tokio::sync::Mutex::new(events_receiver)),
        }
    }

    pub async fn subscribe(
        &self,
        topic: impl Into<String>,
        event: impl Into<String>,
    ) -> Result<String, RpcError> {
        let result = (self.request)(
            SUBSCRIBE_METHOD.into(),
            json!({
                "topic": topic.into(),
                "event": event.into()
            }),
        )
        .await?;
        result
            .get("subscription")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(RpcError::internal_error)
    }

    pub async fn unsubscribe(&self, subscription: impl Into<String>) -> Result<bool, RpcError> {
        let result = (self.request)(
            UNSUBSCRIBE_METHOD.into(),
            json!({
                "subscription": subscription.into()
            }),
        )
        .await?;
        result
            .get("unsubscribed")
            .and_then(Value::as_bool)
            .ok_or_else(RpcError::internal_error)
    }

    pub async fn publish(
        &self,
        target: EventTarget,
        event: impl Into<String>,
        data: Value,
    ) -> Result<PublishResult, RpcError> {
        let target = match target {
            EventTarget::Client { client_id } => json!({"clientId": client_id}),
            EventTarget::Topic { topic } => json!({"topic": topic}),
        };
        let result = (self.request)(
            PUBLISH_METHOD.into(),
            json!({
                "target": target,
                "event": event.into(),
                "data": data
            }),
        )
        .await?;
        serde_json::from_value(result).map_err(|_| RpcError::internal_error())
    }

    /// Dispatch a received `events.event` notification. Returns false for
    /// unrelated or malformed JSON-RPC messages.
    pub fn handle_notification(&self, message: &Value) -> bool {
        let Some(object) = message.as_object() else {
            return false;
        };
        if object.get("jsonrpc").and_then(Value::as_str) != Some(VERSION)
            || object.get("method").and_then(Value::as_str) != Some(EVENT_NOTIFICATION_METHOD)
            || object.contains_key("id")
        {
            return false;
        }
        let Some(params) = object.get("params") else {
            return false;
        };
        let Ok(notification) = serde_json::from_value::<EventNotification>(params.clone()) else {
            return false;
        };
        self.events_sender.try_send(notification).is_ok()
    }

    pub async fn next_event(&self) -> Option<EventNotification> {
        self.events_receiver.lock().await.recv().await
    }
}

/// Result of an `events.publish` call.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PublishResult {
    pub accepted: bool,
    pub recipients: usize,
}

#[cfg(test)]
mod tests;
