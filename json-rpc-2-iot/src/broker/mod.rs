//! Broker-backed RPC. MQTT uses a JSON application envelope; AMQP uses native
//! reply-to/correlation-id properties. Neither adapter implements a broker.
use bytes::Bytes;
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
    ) -> Result<(), TransportError> {
        match self {
            #[cfg(feature = "mqtt")]
            Self::Mqtt(s) => {
                s.publish(destination, payload, reply_to, correlation_id, control)
                    .await
            }
            #[cfg(feature = "amqp")]
            Self::Amqp(s) => {
                s.publish(destination, payload, reply_to, correlation_id, control)
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
    session: Session,
    reader: tokio::task::JoinHandle<()>,
}
pub(crate) struct Client {
    config: Config,
    connection: RwLock<Option<Connection>>,
    pending: Pending,
}
impl Client {
    pub async fn connect(config: Config) -> Result<Self, TransportError> {
        let this = Self {
            config,
            connection: RwLock::new(None),
            pending: Arc::default(),
        };
        this.reconnect().await?;
        Ok(this)
    }
    pub async fn reconnect(&self) -> Result<(), TransportError> {
        let mut connection = self.connection.write().await;
        if let Some(old) = connection.take() {
            old.reader.abort();
            let _ = bounded(self.config.operation_timeout(), old.session.close()).await;
        }
        self.pending.lock().unwrap().clear();
        let (session, mut incoming) = self.config.open(false).await?;
        let pending = self.pending.clone();
        #[cfg(feature = "amqp")]
        let ack_timeout = self.config.operation_timeout();
        let reader = tokio::spawn(async move {
            while let Some(message) = incoming.recv().await {
                if let Some(sender) = pending.lock().unwrap().remove(&message.correlation_id) {
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
        });
        *connection = Some(Connection { session, reader });
        Ok(())
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
        {
            let connection = self.connection.read().await;
            let connection = connection.as_ref().ok_or_else(closed)?;
            if connection.reader.is_finished() {
                return Err(TransportError::new(
                    "broker connection lost; reconnect required",
                ));
            }
            let reply_to = expects_response.then(|| connection.session.reply_to().to_owned());
            bounded(
                self.config.operation_timeout(),
                connection.session.publish(
                    self.config.request_address(),
                    payload,
                    reply_to,
                    id,
                    control,
                ),
            )
            .await?;
        }
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
            old.reader.abort();
            self.pending.lock().unwrap().clear();
            bounded(self.config.operation_timeout(), old.session.close()).await?;
        }
        Ok(())
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        if let Some(c) = self.connection.get_mut().take() {
            c.reader.abort();
        }
    }
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
    let mut workers = tokio::task::JoinSet::new();
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
        loop {
            let message = tokio::select! {
                _ = shutdown.cancelled() => break,
                result = workers.join_next(), if !workers.is_empty() => { let _ = result; continue; },
                message = incoming.recv(), if workers.len() < runtime.limits.max_connections.min(runtime.limits.max_concurrent_requests).max(1) => match message { Some(m) => m, None => break },
            };
            let session = session.clone();
            let protocol = protocol.clone();
            let config = config.clone();
            workers.spawn(async move {
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
                        let response = json_rpc_2_transport::dispatch_stateless(
                            protocol.as_ref(),
                            message.payload.clone(),
                            &transport.to_ascii_lowercase(),
                        )
                        .await;
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
