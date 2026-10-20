//! WebSocket application envelopes through MQTT 3.1.1 brokers.
use super::{WebSocketProtocol, WebSocketTransport, WebSocketTransportFuture};
use bytes::Bytes;
pub use json_rpc_2_iot::MqttConfig;
use json_rpc_2_iot::server::transports::mqtt::MqttTransport;
use json_rpc_2_transport::Transport;
use json_rpc_2_transport::{CancellationToken, JsonRpcProtocol, ProtocolFuture, TransportRuntime};
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
        self.inner.run(Arc::new(Adapter(protocol)), shutdown)
    }
}
struct Adapter(Arc<dyn WebSocketProtocol>);
impl JsonRpcProtocol for Adapter {
    fn handle_message<'a>(&'a self, message: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move {
            let message = json_rpc_2_core::decode_websocket_message(message).ok()??;
            let response = super::handle_broker_message(
                self.0.as_ref(),
                message,
                "WebSocket/MQTT",
            )
            .await?;
            json_rpc_2_core::encode_websocket_message(&response)
                .ok()
                .map(Bytes::from)
        })
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
        Arc::new(Adapter(protocol)),
        shutdown,
        runtime,
    )
    .await
}
