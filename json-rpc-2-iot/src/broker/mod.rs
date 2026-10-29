use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "amqp")]
mod amqp;
#[cfg(feature = "mqtt")]
mod mqtt;

#[cfg(feature = "mqtt")]
#[derive(Clone)]
pub struct MqttConfig {
    pub broker_url: String,
    pub request_topic: String,
    pub operation_timeout: Duration,
    pub response_timeout: Duration,
    pub max_payload_bytes: usize,
    pub heartbeat_interval: Duration,
    pub ca_certificate: Option<Vec<u8>>,
}

#[cfg(feature = "mqtt")]
impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            broker_url: "mqtt://127.0.0.1:1883".into(),
            request_topic: "json-rpc-2/requests".into(),
            operation_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(30),
            max_payload_bytes: 1024 * 1024,
            heartbeat_interval: Duration::from_secs(10),
            ca_certificate: None,
        }
    }
}

#[cfg(feature = "mqtt")]
impl MqttConfig {
    pub fn for_address(address: SocketAddr) -> Self {
        Self {
            broker_url: format!("mqtt://{address}"),
            ..Self::default()
        }
    }
}

#[cfg(feature = "mqtt")]
impl std::fmt::Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConfig")
            .field("broker_url", &"<redacted>")
            .field("request_topic", &self.request_topic)
            .field("operation_timeout", &self.operation_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "amqp")]
#[derive(Clone)]
pub struct AmqpConfig {
    pub broker_url: String,
    pub request_queue: String,
    pub operation_timeout: Duration,
    pub response_timeout: Duration,
    pub max_payload_bytes: usize,
    pub heartbeat_interval: Duration,
    pub prefetch: u16,
}

#[cfg(feature = "amqp")]
impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            broker_url: "amqp://guest:guest@127.0.0.1:5672/%2f".into(),
            request_queue: "json-rpc-2.requests".into(),
            operation_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(30),
            max_payload_bytes: 1024 * 1024,
            heartbeat_interval: Duration::from_secs(10),
            prefetch: 64,
        }
    }
}

#[cfg(feature = "amqp")]
impl AmqpConfig {
    pub fn for_address(address: SocketAddr) -> Self {
        Self {
            broker_url: format!("amqp://{address}/%2f"),
            ..Self::default()
        }
    }
}

#[cfg(feature = "amqp")]
impl std::fmt::Debug for AmqpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpConfig")
            .field("broker_url", &"<redacted>")
            .field("request_queue", &self.request_queue)
            .field("operation_timeout", &self.operation_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("prefetch", &self.prefetch)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MessageKind {
    #[default]
    Rpc,
    Open,
    Close,
    Heartbeat,
    Push,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct SessionIdentity {
    id: String,
    push_to: String,
}

impl SessionIdentity {
    fn new(push_to: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            push_to,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
struct Metadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session: Option<SessionIdentity>,
    #[serde(default)]
    kind: MessageKind,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct Envelope {
    payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reply_to: Option<String>,
    correlation_id: String,
    #[serde(default)]
    control: bool,
    #[serde(default)]
    metadata: Metadata,
}

#[derive(Clone)]
pub enum Config {
    #[cfg(feature = "mqtt")]
    Mqtt(MqttConfig),
    #[cfg(feature = "amqp")]
    Amqp(AmqpConfig),
}

impl Config {
    fn operation_timeout(&self) -> Duration {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(config) => config.operation_timeout,
            #[cfg(feature = "amqp")]
            Self::Amqp(config) => config.operation_timeout,
        }
    }

    fn response_timeout(&self) -> Duration {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(config) => config.response_timeout,
            #[cfg(feature = "amqp")]
            Self::Amqp(config) => config.response_timeout,
        }
    }

    fn max_payload(&self) -> usize {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(config) => config.max_payload_bytes,
            #[cfg(feature = "amqp")]
            Self::Amqp(config) => config.max_payload_bytes,
        }
    }

    fn heartbeat_interval(&self) -> Duration {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(config) => config.heartbeat_interval,
            #[cfg(feature = "amqp")]
            Self::Amqp(config) => config.heartbeat_interval,
        }
    }

    fn request_address(&self) -> &str {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(config) => &config.request_topic,
            #[cfg(feature = "amqp")]
            Self::Amqp(config) => &config.request_queue,
        }
    }

    async fn open(
        &self,
    ) -> Result<(Session, mpsc::Receiver<Message>), json_rpc_2_transport::TransportError> {
        if self.operation_timeout().is_zero()
            || self.response_timeout().is_zero()
            || self.max_payload() == 0
            || self.heartbeat_interval().is_zero()
        {
            return Err(json_rpc_2_transport::TransportError::new(
                "broker client limits must be nonzero",
            ));
        }
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(config) => mqtt::open(config).await,
            #[cfg(feature = "amqp")]
            Self::Amqp(config) => amqp::open(config).await,
        }
    }
}

pub(crate) async fn bounded<T>(
    duration: Duration,
    future: impl Future<Output = Result<T, json_rpc_2_transport::TransportError>>,
) -> Result<T, json_rpc_2_transport::TransportError> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| json_rpc_2_transport::TransportError::new("broker operation timed out"))?
}

pub(crate) struct Message {
    payload: Bytes,
    correlation_id: String,
    metadata: Metadata,
    #[cfg(feature = "amqp")]
    ack: Option<lapin::Acker>,
}

enum Session {
    #[cfg(feature = "mqtt")]
    Mqtt(mqtt::Session),
    #[cfg(feature = "amqp")]
    Amqp(amqp::Session),
}

impl Session {
    async fn publish(
        &self,
        destination: &str,
        payload: Bytes,
        reply_to: Option<String>,
        correlation_id: String,
        control: bool,
        metadata: Metadata,
    ) -> Result<(), json_rpc_2_transport::TransportError> {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(session) => {
                session
                    .publish(
                        destination,
                        payload,
                        reply_to,
                        correlation_id,
                        control,
                        metadata,
                    )
                    .await
            }
            #[cfg(feature = "amqp")]
            Self::Amqp(session) => {
                session
                    .publish(
                        destination,
                        payload,
                        reply_to,
                        correlation_id,
                        control,
                        metadata,
                    )
                    .await
            }
        }
    }

    fn reply_to(&self) -> &str {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(session) => &session.reply_to,
            #[cfg(feature = "amqp")]
            Self::Amqp(session) => &session.reply_to,
        }
    }

    async fn close(&self) -> Result<(), json_rpc_2_transport::TransportError> {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(session) => session.close().await,
            #[cfg(feature = "amqp")]
            Self::Amqp(session) => session.close().await,
        }
    }
}

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Bytes>>>>;

struct PendingGuard {
    pending: Pending,
    id: String,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

struct Connection {
    session: Arc<Session>,
    identity: SessionIdentity,
    reader: tokio::task::JoinHandle<()>,
    heartbeat: tokio::task::JoinHandle<()>,
}

pub struct Client {
    config: Config,
    connection: RwLock<Option<Connection>>,
    pending: Pending,
    push: tokio::sync::Mutex<mpsc::Receiver<Bytes>>,
}

impl Client {
    pub async fn connect(config: Config) -> Result<Self, json_rpc_2_transport::TransportError> {
        let (_, push) = mpsc::channel(256);
        let this = Self {
            config,
            connection: RwLock::new(None),
            pending: Arc::default(),
            push: tokio::sync::Mutex::new(push),
        };
        this.reconnect().await?;
        Ok(this)
    }

    pub async fn reconnect(&self) -> Result<(), json_rpc_2_transport::TransportError> {
        use json_rpc_2_transport::CancellationToken;
        let mut connection = self.connection.write().await;
        if let Some(old) = connection.take() {
            old.heartbeat.abort();
            old.reader.abort();
            self.pending.lock().unwrap().clear();
            let _ = self.lifecycle(&old, MessageKind::Close).await;
            let _ = bounded(self.config.operation_timeout(), old.session.close()).await;
        }
        self.pending.lock().unwrap().clear();
        let (session, mut incoming) = self.config.open().await?;
        let session = Arc::new(session);
        let identity = SessionIdentity::new(session.reply_to().to_owned());
        bounded(
            self.config.operation_timeout(),
            session.publish(
                self.config.request_address(),
                Bytes::new(),
                None,
                String::new(),
                false,
                Metadata {
                    session: Some(identity.clone()),
                    kind: MessageKind::Open,
                },
            ),
        )
        .await?;
        let (push, receiver) = mpsc::channel(256);
        *self.push.lock().await = receiver;
        let pending = self.pending.clone();
        let reader_stopped = CancellationToken::new();
        let heartbeat_stop = reader_stopped.clone();
        #[cfg(feature = "amqp")]
        let ack_timeout = self.config.operation_timeout();
        let reader = tokio::spawn(async move {
            while let Some(message) = incoming.recv().await {
                if message.metadata.kind == MessageKind::Close {
                    #[cfg(feature = "amqp")]
                    if let Some(ack) = message.ack {
                        let _ = tokio::time::timeout(
                            ack_timeout,
                            ack.ack(lapin::options::BasicAckOptions::default()),
                        )
                        .await;
                    }
                    break;
                } else if message.metadata.kind == MessageKind::Push {
                    if push.try_send(message.payload).is_err() {
                        break;
                    }
                } else if let Some(sender) = pending.lock().unwrap().remove(&message.correlation_id)
                {
                    let _ = sender.send(message.payload);
                }
                #[cfg(feature = "amqp")]
                if let Some(ack) = message.ack {
                    let _ = tokio::time::timeout(
                        ack_timeout,
                        ack.ack(lapin::options::BasicAckOptions::default()),
                    )
                    .await;
                }
            }
            pending.lock().unwrap().clear();
            reader_stopped.cancel();
        });
        let heartbeat_session = session.clone();
        let heartbeat_identity = identity.clone();
        let config = self.config.clone();
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.heartbeat_interval());
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = heartbeat_stop.cancelled() => break,
                    _ = interval.tick() => {}
                }
                if bounded(
                    config.operation_timeout(),
                    heartbeat_session.publish(
                        config.request_address(),
                        Bytes::new(),
                        None,
                        String::new(),
                        false,
                        Metadata {
                            session: Some(heartbeat_identity.clone()),
                            kind: MessageKind::Heartbeat,
                        },
                    ),
                )
                .await
                .is_err()
                {
                    break;
                }
            }
        });
        *connection = Some(Connection {
            session,
            identity,
            reader,
            heartbeat,
        });
        Ok(())
    }

    async fn lifecycle(
        &self,
        connection: &Connection,
        kind: MessageKind,
    ) -> Result<(), json_rpc_2_transport::TransportError> {
        bounded(
            self.config.operation_timeout(),
            connection.session.publish(
                self.config.request_address(),
                Bytes::new(),
                None,
                String::new(),
                false,
                Metadata {
                    session: Some(connection.identity.clone()),
                    kind,
                },
            ),
        )
        .await
    }

    pub async fn receive_push(&self) -> Result<Bytes, json_rpc_2_transport::TransportError> {
        self.push
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(json_rpc_2_transport::closed)
    }

    pub async fn send(
        &self,
        payload: Bytes,
        expects_response: bool,
        control: bool,
    ) -> Result<Option<Bytes>, json_rpc_2_transport::TransportError> {
        if payload.len() > self.config.max_payload() {
            return Err(json_rpc_2_transport::TransportError::new(
                "broker payload limit exceeded",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        let _guard = PendingGuard {
            pending: self.pending.clone(),
            id: id.clone(),
        };
        if expects_response {
            self.pending.lock().unwrap().insert(id.clone(), sender);
        }
        let (session, reply_to, metadata) = {
            let connection = self.connection.read().await;
            let connection = connection
                .as_ref()
                .ok_or_else(json_rpc_2_transport::closed)?;
            if connection.reader.is_finished() {
                return Err(json_rpc_2_transport::TransportError::new(
                    "broker connection lost; reconnect required",
                ));
            }
            let reply_to = expects_response.then(|| connection.session.reply_to().to_owned());
            (
                connection.session.clone(),
                reply_to,
                Metadata {
                    session: Some(connection.identity.clone()),
                    kind: MessageKind::Rpc,
                },
            )
        };
        bounded(
            self.config.operation_timeout(),
            session.publish(
                self.config.request_address(),
                payload,
                reply_to,
                id,
                control,
                metadata,
            ),
        )
        .await?;
        if !expects_response {
            return Ok(None);
        }
        bounded(self.config.response_timeout(), async {
            receiver
                .await
                .map(Some)
                .map_err(|_| json_rpc_2_transport::TransportError::new("broker connection lost"))
        })
        .await
    }

    pub async fn close(&self) -> Result<(), json_rpc_2_transport::TransportError> {
        let mut connection = self.connection.write().await;
        if let Some(old) = connection.take() {
            old.heartbeat.abort();
            old.reader.abort();
            self.pending.lock().unwrap().clear();
            let lifecycle = self.lifecycle(&old, MessageKind::Close).await;
            bounded(self.config.operation_timeout(), old.session.close()).await?;
            lifecycle?;
        }
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.get_mut().take() {
            connection.heartbeat.abort();
            connection.reader.abort();
        }
    }
}

#[cfg(test)]
mod tests;
