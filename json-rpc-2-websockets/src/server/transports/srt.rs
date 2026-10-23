//! WebSocket application frames over standard encrypted libsrt connections.

use std::{io, net::SocketAddr, sync::Arc};

use json_rpc_2_core::SrtConfig;
use json_rpc_2_datagram::server::transports::srt::SrtTransport;
use json_rpc_2_transport::{Transport, TransportRuntime};

use super::{CancellationToken, WebSocketProtocol, WebSocketTransport, WebSocketTransportFuture};

pub struct SrtWebSocketTransport {
    inner: SrtTransport,
}

impl SrtWebSocketTransport {
    pub async fn bind(address: SocketAddr, config: SrtConfig) -> io::Result<Self> {
        Ok(Self {
            inner: SrtTransport::bind(address, config).await?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.inner = self.inner.with_runtime(runtime);
        self
    }
}

impl WebSocketTransport for SrtWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        self.inner.run(
            super::protocol_adapter_with_kind(protocol, "ws-srt"),
            shutdown,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::transports::srt::SrtWebSocketClientTransport;
    use bytes::Bytes;
    use json_rpc_2_core::{WebSocketFuture, WebSocketMessage};
    use json_rpc_2_transport::RpcClientTransport;

    struct Echo;
    impl WebSocketProtocol for Echo {
        fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a> {
            Box::pin(async move {
                let payload = match &message {
                    WebSocketMessage::Text(text) => text.as_bytes(),
                    WebSocketMessage::Binary(bytes)
                    | WebSocketMessage::Ping(bytes)
                    | WebSocketMessage::Pong(bytes) => bytes.as_ref(),
                };
                json_rpc_2_transport::expects_response(payload).then_some(message)
            })
        }
    }

    #[tokio::test]
    async fn encrypted_libsrt_websocket_round_trip_notification_and_close() {
        let server = SrtWebSocketTransport::bind(
            "127.0.0.1:0".parse().unwrap(),
            SrtConfig {
                passphrase: "websocket-shared-secret".into(),
            },
        )
        .await
        .unwrap();
        let address = server.local_addr().unwrap();
        let running = server.start(Arc::new(Echo));
        let client = SrtWebSocketClientTransport::connect(address, "websocket-shared-secret")
            .await
            .unwrap();
        let payload = Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#);
        assert_eq!(client.send(payload.clone()).await.unwrap(), Some(payload));
        assert_eq!(
            client
                .send(Bytes::from_static(br#"{"method":"notify"}"#))
                .await
                .unwrap(),
            None
        );
        client.transport_ping().await.unwrap();
        client.close().await.unwrap();
        running.close().await.unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_delivers_inflight_websocket_response() {
        struct Gated {
            entered: tokio::sync::Notify,
            release: tokio::sync::Notify,
        }
        impl WebSocketProtocol for Gated {
            fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a> {
                Box::pin(async move {
                    self.entered.notify_one();
                    self.release.notified().await;
                    Some(message)
                })
            }
        }
        let protocol = Arc::new(Gated {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let server = SrtWebSocketTransport::bind(
            "127.0.0.1:0".parse().unwrap(),
            SrtConfig {
                passphrase: "websocket-shared-secret".into(),
            },
        )
        .await
        .unwrap();
        let address = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(server.run(protocol.clone(), shutdown.clone()));
        let client = Arc::new(
            SrtWebSocketClientTransport::connect(address, "websocket-shared-secret")
                .await
                .unwrap(),
        );
        let sender = client.clone();
        let payload = Bytes::from_static(br#"{"method":"echo","id":1}"#);
        let expected = payload.clone();
        let request = tokio::spawn(async move { sender.send(payload).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            protocol.entered.notified(),
        )
        .await
        .unwrap();
        shutdown.cancel();
        protocol.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(request.await.unwrap().unwrap(), Some(expected));
        client.close().await.unwrap();
    }
}
