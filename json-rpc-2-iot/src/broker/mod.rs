//! Broker-backed RPC. MQTT uses a JSON application envelope; AMQP uses native
//! reply-to/correlation-id properties. Neither adapter implements a broker.
use bytes::Bytes;
pub use gateway_sessions::{BrokerEnvelope, MessageKind};
use gateway_sessions::{
    BrokerSessionPolicy, MessageMetadata as Metadata, SessionError, SessionIdentity,
};
use json_rpc_2_transport::{CancellationToken, TransportError, closed};
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, mpsc, oneshot};

#[cfg(feature = "amqp")]
mod amqp;
#[cfg(feature = "mqtt")]
mod mqtt;

/// MQTT 3.1.1 broker settings. URLs support credentials and `mqtts://` TLS.
/// Debug deliberately omits URLs, which may contain passwords.
#[cfg(feature = "mqtt")]
#[derive(Clone)]
pub struct MqttConfig {
    pub broker_url: String,
    pub request_topic: String,
    pub operation_timeout: Duration,
    pub response_timeout: Duration,
    pub max_payload_bytes: usize,
    pub sessions: BrokerSessionPolicy,
    /// PEM CA bundle; None uses the system trust store.
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
            sessions: BrokerSessionPolicy::default(),
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
            .finish_non_exhaustive()
    }
}

/// AMQP 0-9-1 broker settings. `amqps://` validates TLS using system roots;
/// URL credentials and virtual hosts are handled by lapin.
#[cfg(feature = "amqp")]
#[derive(Clone)]
pub struct AmqpConfig {
    pub broker_url: String,
    pub request_queue: String,
    pub operation_timeout: Duration,
    pub response_timeout: Duration,
    pub max_payload_bytes: usize,
    pub sessions: BrokerSessionPolicy,
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
            sessions: BrokerSessionPolicy::default(),
            prefetch: 64,
        }
    }
}
#[cfg(feature = "amqp")]
impl AmqpConfig {
    pub fn for_address(address: SocketAddr) -> Self {
        Self {
            broker_url: format!("amqp://guest:guest@{address}/%2f"),
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
            .field("prefetch", &self.prefetch)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) enum Config {
    #[cfg(feature = "mqtt")]
    Mqtt(MqttConfig),
    #[cfg(feature = "amqp")]
    Amqp(AmqpConfig),
}
impl Config {
    fn sessions(&self) -> BrokerSessionPolicy {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(config) => config.sessions,
            #[cfg(feature = "amqp")]
            Self::Amqp(config) => config.sessions,
        }
    }

    fn operation_timeout(&self) -> Duration {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(c) => c.operation_timeout,
            #[cfg(feature = "amqp")]
            Self::Amqp(c) => c.operation_timeout,
        }
    }
    fn response_timeout(&self) -> Duration {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(c) => c.response_timeout,
            #[cfg(feature = "amqp")]
            Self::Amqp(c) => c.response_timeout,
        }
    }
    fn max_payload(&self) -> usize {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(c) => c.max_payload_bytes,
            #[cfg(feature = "amqp")]
            Self::Amqp(c) => c.max_payload_bytes,
        }
    }
    fn request_address(&self) -> &str {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(c) => &c.request_topic,
            #[cfg(feature = "amqp")]
            Self::Amqp(c) => &c.request_queue,
        }
    }
    async fn open(
        &self,
        server: bool,
    ) -> Result<(Session, mpsc::Receiver<Message>), TransportError> {
        self.sessions()
            .validate()
            .map_err(|error| TransportError::new(error.to_string()))?;
        if self.operation_timeout().is_zero()
            || self.response_timeout().is_zero()
            || self.max_payload() == 0
        {
            return Err(TransportError::new("broker limits must be nonzero"));
        }
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(c) => mqtt::open(c, server).await,
            #[cfg(feature = "amqp")]
            Self::Amqp(c) => amqp::open(c, server).await,
        }
    }
}

pub(crate) async fn bounded<T>(
    duration: Duration,
    future: impl Future<Output = Result<T, TransportError>>,
) -> Result<T, TransportError> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| TransportError::new("broker operation timed out"))?
}

pub(crate) struct Message {
    pub payload: Bytes,
    pub reply_to: Option<String>,
    pub correlation_id: String,
    pub control: bool,
    pub metadata: Metadata,
    #[cfg(feature = "amqp")]
    pub ack: Option<lapin::Acker>,
}
enum Session {
    #[cfg(feature = "mqtt")]
    Mqtt(mqtt::Session),
    #[cfg(feature = "amqp")]
    Amqp(Box<amqp::Session>),
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
    ) -> Result<(), TransportError> {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(s) => {
                s.publish(
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
            Self::Amqp(s) => {
                s.publish(
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
            Self::Mqtt(s) => &s.reply_to,
            #[cfg(feature = "amqp")]
            Self::Amqp(s) => &s.reply_to,
        }
    }
    async fn close(&self) -> Result<(), TransportError> {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(s) => s.close().await,
            #[cfg(feature = "amqp")]
            Self::Amqp(s) => s.close().await,
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
pub(crate) struct Client {
    config: Config,
    connection: RwLock<Option<Connection>>,
    pending: Pending,
    push: tokio::sync::Mutex<mpsc::Receiver<Bytes>>,
}
impl Client {
    pub async fn connect(config: Config) -> Result<Self, TransportError> {
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
    pub async fn reconnect(&self) -> Result<(), TransportError> {
        let mut connection = self.connection.write().await;
        if let Some(old) = connection.take() {
            old.heartbeat.abort();
            old.reader.abort();
            self.pending.lock().unwrap().clear();
            let _ = self.lifecycle(&old, MessageKind::Close).await;
            let _ = bounded(self.config.operation_timeout(), old.session.close()).await;
        }
        self.pending.lock().unwrap().clear();
        let (session, mut incoming) = self.config.open(false).await?;
        let session = Arc::new(session);
        let identity = SessionIdentity::generate(session.reply_to().to_owned())
            .map_err(|error| TransportError::new(error.to_string()))?;
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
                    // Never let a slow push consumer block RPC replies and acknowledgements.
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
            let mut interval = tokio::time::interval(config.sessions().heartbeat_interval);
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
    ) -> Result<(), TransportError> {
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
    /// Receive an unsolicited push independently of pending RPC exchanges.
    /// A full 256-message queue closes the reader; reconnect starts a fresh queue.
    pub async fn receive_push(&self) -> Result<Bytes, TransportError> {
        self.push.lock().await.recv().await.ok_or_else(closed)
    }
    pub async fn send(
        &self,
        payload: Bytes,
        expects_response: bool,
        control: bool,
    ) -> Result<Option<Bytes>, TransportError> {
        if payload.len() > self.config.max_payload() {
            return Err(TransportError::new("broker payload limit exceeded"));
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
            let connection = connection.as_ref().ok_or_else(closed)?;
            if connection.reader.is_finished() {
                return Err(TransportError::new(
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
                .map_err(|_| TransportError::new("broker connection lost"))
        })
        .await
    }
    pub async fn close(&self) -> Result<(), TransportError> {
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
        if let Some(c) = self.connection.get_mut().take() {
            c.heartbeat.abort();
            c.reader.abort();
        }
    }
}

struct IngressSession {
    guard: json_rpc_2_transport::ProtocolSessionGuard,
    push: tokio::task::JoinHandle<()>,
}
impl Drop for IngressSession {
    fn drop(&mut self) {
        self.push.abort();
    }
}

fn ingress_session(
    config: &Config,
    protocol: Arc<dyn json_rpc_2_transport::JsonRpcProtocol>,
    session: Arc<Session>,
    identity: &SessionIdentity,
) -> IngressSession {
    let transport = match config {
        #[cfg(feature = "mqtt")]
        Config::Mqtt(_) => "mqtt",
        #[cfg(feature = "amqp")]
        Config::Amqp(_) => "amqp",
    };
    let mut ingress = json_rpc_2_transport::TransportSession::new(transport, true);
    let guard = json_rpc_2_transport::ProtocolSessionGuard::new(protocol, ingress.context);
    let push_to = identity.push_to.clone();
    let destination = push_to.clone();
    let config = config.clone();
    let push = tokio::spawn(async move {
        while let Some(payload) = ingress.push_receiver.recv().await {
            if payload.len() > config.max_payload() {
                break;
            }
            if bounded(
                config.operation_timeout(),
                session.publish(
                    &destination,
                    payload,
                    None,
                    String::new(),
                    false,
                    Metadata {
                        session: None,
                        kind: MessageKind::Push,
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
    IngressSession { guard, push }
}

pub(crate) struct Prepared {
    session: Session,
    incoming: mpsc::Receiver<Message>,
}
pub(crate) async fn prepare(config: &Config) -> std::io::Result<Prepared> {
    let (session, incoming) = config.open(true).await.map_err(std::io::Error::other)?;
    Ok(Prepared { session, incoming })
}

pub(crate) async fn serve(
    config: Config,
    mut prepared: Option<Prepared>,
    protocol: Arc<dyn json_rpc_2_transport::JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<json_rpc_2_transport::TransportRuntime>,
) -> std::io::Result<()> {
    let mut workers = tokio::task::JoinSet::<Option<String>>::new();
    let mut connected_once = prepared.is_some();
    loop {
        let opened = if let Some(prepared) = prepared.take() {
            Ok((prepared.session, prepared.incoming))
        } else {
            tokio::select! { _ = shutdown.cancelled() => break, result = config.open(true) => result }
        };
        let (session, mut incoming) = match opened {
            Ok(connection) => {
                connected_once = true;
                connection
            }
            Err(error) if !connected_once => return Err(std::io::Error::other(error)),
            Err(error) => {
                json_rpc_2_transport::log_transport_error(
                    "broker",
                    "external",
                    "reconnect",
                    &error,
                );
                tokio::select! { _ = shutdown.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                continue;
            }
        };
        let session = Arc::new(session);
        let mut sessions: HashMap<String, IngressSession> = HashMap::new();
        let policy = config.sessions();
        let session_registry =
            gateway_sessions::SessionRegistry::new(policy, runtime.limits.max_connections.max(1))
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        let mut leases = tokio::time::interval(policy.heartbeat_interval);
        let worker_limit = runtime
            .limits
            .max_connections
            .min(runtime.limits.max_concurrent_requests)
            .max(1);
        loop {
            let message = tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = leases.tick() => {
                    for identity in session_registry.expire(Instant::now()) {
                        sessions.remove(&identity.id);
                    }
                    let failed_push_sessions: Vec<_> = sessions
                        .iter()
                        .filter(|(_, session)| session.push.is_finished())
                        .map(|(id, _)| id.clone())
                        .collect();
                    for id in failed_push_sessions {
                        session_registry.close(&id, Instant::now());
                        sessions.remove(&id);
                    }
                    continue;
                },
                result = workers.join_next(), if !workers.is_empty() => {
                    if let Some(Ok(Some(id))) = result {
                        session_registry.close(&id, Instant::now());
                        sessions.remove(&id);
                    }
                    continue;
                },
                message = incoming.recv() => match message { Some(m) => m, None => break },
            };
            let mut context = None;
            let mut active_request = None;
            if let Some(identity) = &message.metadata.session {
                let valid_identity = identity.validate().is_ok();
                match message.metadata.kind {
                    MessageKind::Close if valid_identity => {
                        if session_registry
                            .close_identity(identity, Instant::now())
                            .is_some()
                        {
                            sessions.remove(&identity.id);
                        }
                    }
                    MessageKind::Heartbeat if valid_identity => {
                        session_registry.heartbeat(identity, Instant::now());
                    }
                    MessageKind::Open if valid_identity => {
                        match session_registry.open(identity.clone(), Instant::now()) {
                            Ok(()) => {
                                sessions.entry(identity.id.clone()).or_insert_with(|| {
                                    ingress_session(
                                        &config,
                                        protocol.clone(),
                                        session.clone(),
                                        identity,
                                    )
                                });
                            }
                            Err(SessionError::Retired) => {
                                sessions.remove(&identity.id);
                            }
                            Err(_) => {}
                        }
                    }
                    MessageKind::Rpc if valid_identity => {
                        // Keep compatibility with clients that attach identity to
                        // their first RPC without a preceding Open control frame.
                        match session_registry.open(identity.clone(), Instant::now()) {
                            Ok(()) => {
                                sessions.entry(identity.id.clone()).or_insert_with(|| {
                                    ingress_session(
                                        &config,
                                        protocol.clone(),
                                        session.clone(),
                                        identity,
                                    )
                                });
                                if let Some(ingress) = sessions.get(&identity.id) {
                                    active_request =
                                        session_registry.begin_request(identity, Instant::now());
                                    if active_request.is_some() {
                                        context = Some(ingress.guard.context().clone());
                                    }
                                }
                            }
                            Err(SessionError::Retired) => {
                                sessions.remove(&identity.id);
                            }
                            Err(_) => {}
                        }
                    }
                    MessageKind::Rpc
                    | MessageKind::Open
                    | MessageKind::Close
                    | MessageKind::Heartbeat
                    | MessageKind::Push => {}
                }
                if message.metadata.kind != MessageKind::Rpc || context.is_none() {
                    #[cfg(feature = "amqp")]
                    if let Some(ack) = message.ack {
                        let _ = tokio::time::timeout(
                            config.operation_timeout(),
                            ack.ack(lapin::options::BasicAckOptions::default()),
                        )
                        .await;
                    }
                    continue;
                }
            } else if message.metadata.kind != MessageKind::Rpc {
                #[cfg(feature = "amqp")]
                if let Some(ack) = message.ack {
                    let _ = tokio::time::timeout(
                        config.operation_timeout(),
                        ack.ack(lapin::options::BasicAckOptions::default()),
                    )
                    .await;
                }
                continue;
            }
            if workers.len() >= worker_limit {
                // Do not queue application work or stop reading the shared
                // route: lifecycle traffic and lease sweeps must remain live.
                #[cfg(feature = "amqp")]
                if let Some(ack) = message.ack {
                    let _ = tokio::time::timeout(
                        config.operation_timeout(),
                        ack.nack(lapin::options::BasicNackOptions {
                            requeue: false,
                            multiple: false,
                        }),
                    )
                    .await;
                }
                json_rpc_2_transport::log_transport_error(
                    "broker",
                    "external",
                    "admission",
                    &TransportError::new(
                        "broker request concurrency limit exceeded; request not dispatched",
                    ),
                );
                continue;
            }
            // An admitted long-running handler retains its session until it
            // completes or reaches the configured response timeout.
            let activity = active_request;
            let session = session.clone();
            let protocol = protocol.clone();
            let config = config.clone();
            workers.spawn(async move {
                let _activity = activity;
                let mut close_session = false;
                let result = bounded(config.response_timeout(), async {
                    let transport = match &config {
                        #[cfg(feature = "mqtt")]
                        Config::Mqtt(_) => "MQTT",
                        #[cfg(feature = "amqp")]
                        Config::Amqp(_) => "AMQP",
                    };
                    let started = Instant::now();
                    let response = if message.control {
                        json_rpc_2_transport::log_control(transport, "external", "ping", "pong");
                        Some(Bytes::from_static(b"pong"))
                    } else {
                        let context = context.unwrap_or_else(|| {
                            json_rpc_2_transport::TransportSession::stateless(
                                transport.to_ascii_lowercase(),
                            )
                        });
                        let response = protocol
                            .handle_owned_message(message.payload.clone(), &context)
                            .await
                            .map(json_rpc_2_transport::TransportMessage::into_bytes);
                        close_session = protocol.session_close_requested(&context);
                        if close_session && let Some(identity) = &message.metadata.session {
                            bounded(
                                config.operation_timeout(),
                                session.publish(
                                    &identity.push_to,
                                    Bytes::new(),
                                    None,
                                    String::new(),
                                    false,
                                    Metadata {
                                        session: None,
                                        kind: MessageKind::Close,
                                    },
                                ),
                            )
                            .await?;
                        }
                        json_rpc_2_transport::log_json_rpc(
                            transport,
                            "external",
                            &message.payload,
                            response.as_deref(),
                            started.elapsed(),
                        );
                        response
                    };
                    if let (Some(response), Some(reply_to)) = (response, message.reply_to) {
                        if response.len() > config.max_payload() {
                            return Err(TransportError::new(
                                "broker response payload limit exceeded",
                            ));
                        }
                        bounded(
                            config.operation_timeout(),
                            session.publish(
                                &reply_to,
                                response,
                                None,
                                message.correlation_id,
                                message.control,
                                Metadata::default(),
                            ),
                        )
                        .await?;
                    }
                    Ok(())
                })
                .await;
                #[cfg(feature = "amqp")]
                if let Some(ack) = message.ack {
                    let acknowledged = bounded(config.operation_timeout(), async {
                        if result.is_ok() {
                            ack.ack(lapin::options::BasicAckOptions::default()).await
                        } else {
                            ack.nack(lapin::options::BasicNackOptions {
                                requeue: false,
                                multiple: false,
                            })
                            .await
                        }
                        .map_err(|_| TransportError::new("AMQP acknowledgement failed"))
                    })
                    .await;
                    if let Err(error) = acknowledged {
                        json_rpc_2_transport::log_transport_error(
                            "AMQP",
                            "external",
                            "acknowledge",
                            &error,
                        );
                    }
                }
                if let Err(error) = result {
                    json_rpc_2_transport::log_transport_error(
                        "broker", "external", "handle", &error,
                    );
                }
                if close_session {
                    message.metadata.session.map(|identity| identity.id)
                } else {
                    None
                }
            });
        }
        if shutdown.is_cancelled() {
            let drained = tokio::time::timeout(runtime.shutdown.close_timeout, async {
                while workers.join_next().await.is_some() {}
            })
            .await;
            if drained.is_err() {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
            }
        } else {
            workers.abort_all();
            while workers.join_next().await.is_some() {}
        }
        drop(sessions);
        let _ = bounded(
            config
                .operation_timeout()
                .min(runtime.shutdown.close_timeout),
            session.close(),
        )
        .await;
        if shutdown.is_cancelled() {
            break;
        }
        tokio::select! { _ = shutdown.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    }
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    Ok(())
}
