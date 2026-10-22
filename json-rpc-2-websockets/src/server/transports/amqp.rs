//! WebSocket application envelopes through AMQP 0-9-1 brokers.
use super::{WebSocketProtocol, WebSocketTransport, WebSocketTransportFuture};
pub use json_rpc_2_iot::AmqpConfig;
use json_rpc_2_iot::server::transports::amqp::AmqpTransport;
use json_rpc_2_transport::Transport;
use json_rpc_2_transport::{CancellationToken, TransportRuntime};
use std::{io, sync::Arc};

pub struct AmqpWebSocketTransport {
    inner: AmqpTransport,
}
impl AmqpWebSocketTransport {
    pub fn new(config: AmqpConfig) -> Self {
        Self {
            inner: AmqpTransport::new(config),
        }
    }
    pub async fn connect(config: AmqpConfig) -> io::Result<Self> {
        Ok(Self {
            inner: AmqpTransport::connect(config).await?,
        })
    }
    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.inner = self.inner.with_runtime(runtime);
        self
    }
}
impl WebSocketTransport for AmqpWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        self.inner.run(
            Arc::new(super::BrokerWebSocketAdapter::new(protocol, "ws-amqp")),
            shutdown,
        )
    }
}
pub async fn serve(
    config: AmqpConfig,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    json_rpc_2_iot::server::transports::amqp::serve(
        config,
        Arc::new(super::BrokerWebSocketAdapter::new(protocol, "ws-amqp")),
        shutdown,
        runtime,
    )
    .await
}
