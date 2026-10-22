//! MQTT 3.1.1 RPC through an external broker.
pub use crate::MqttConfig;
use crate::broker::{Client, Config};
use bytes::Bytes;
use json_rpc_2_transport::*;
use std::net::SocketAddr;

pub struct MqttClientTransport {
    inner: Client,
}
impl MqttClientTransport {
    /// Receive an unsolicited server push without allocating an RPC correlation ID.
    /// Calls may run concurrently with requests. Reconnect resets the receive queue.
    pub async fn receive_push(&self) -> Result<Bytes, TransportError> {
        self.inner.receive_push().await
    }
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_config(MqttConfig::for_address(address)).await
    }
    pub async fn connect_with_config(config: MqttConfig) -> Result<Self, TransportError> {
        Ok(Self {
            inner: Client::connect(Config::Mqtt(config)).await?,
        })
    }
}
impl RpcClientTransport for MqttClientTransport {
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
        "mqtt"
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
