//! WebSocket application envelopes through MQTT 3.1.1 brokers.
use super::{WebSocketProtocol, WebSocketTransport, WebSocketTransportFuture};
pub use json_rpc_2_iot::MqttConfig;
use json_rpc_2_iot::server::transports::mqtt::MqttTransport;
use json_rpc_2_transport::Transport;
use json_rpc_2_transport::{CancellationToken, TransportRuntime};
use std::{io, sync::Arc};

pub struct MqttWebSocketTransport {
    inner: MqttTransport,
}
impl MqttWebSocketTransport {
    pub fn new(config: MqttConfig) -> Self {
        Self {
            inner: MqttTransport::new(config),
        }
    }
    pub async fn connect(config: MqttConfig) -> io::Result<Self> {
        Ok(Self {
            inner: MqttTransport::connect(config).await?,
        })
    }
    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.inner = self.inner.with_runtime(runtime);
        self
    }
}
impl WebSocketTransport for MqttWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        self.inner.run(
            Arc::new(super::BrokerWebSocketAdapter::new(protocol, "ws-mqtt")),
            shutdown,
        )
    }
}
pub async fn serve(
    config: MqttConfig,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    json_rpc_2_iot::server::transports::mqtt::serve(
        config,
        Arc::new(super::BrokerWebSocketAdapter::new(protocol, "ws-mqtt")),
        shutdown,
        runtime,
    )
    .await
}
