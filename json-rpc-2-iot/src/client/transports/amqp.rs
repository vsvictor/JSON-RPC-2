//! AMQP 0-9-1 RPC through an external broker.
pub use crate::AmqpConfig;
use crate::broker::{Client, Config};
use bytes::Bytes;
use json_rpc_2_transport::*;
use std::net::SocketAddr;

pub struct AmqpClientTransport {
    inner: Client,
}
impl AmqpClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_config(AmqpConfig::for_address(address)).await
    }
    pub async fn connect_with_config(config: AmqpConfig) -> Result<Self, TransportError> {
        Ok(Self {
            inner: Client::connect(Config::Amqp(config)).await?,
        })
    }
}
impl RpcClientTransport for AmqpClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        let response = expects_response(&message);
        self.send_with_expectation(message, response)
    }
    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(self.inner.send(message, response, false))
    }
    fn kind(&self) -> &'static str {
        "amqp"
    }
    fn transport_ping(&self) -> ClientPingFuture<'_> {
        Box::pin(async { self.inner.send(Bytes::new(), true, true).await.map(|_| ()) })
    }
    fn reconnect(&self) -> ClientRecoverFuture<'_> {
        Box::pin(self.inner.reconnect())
    }
    fn close(&self) -> ClientCloseFuture<'_> {
        Box::pin(self.inner.close())
    }
}
