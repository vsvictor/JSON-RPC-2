//! Opt-in bounded tests using real MQTT and AMQP brokers, not MQTT-over-WS.
#![cfg(all(feature = "websocket", any(feature = "mqtt", feature = "amqp")))]

use bytes::Bytes;
use json_rpc_2_transport::{
    JsonRpcProtocol, ProtocolFuture, RpcClientTransport, Transport, TransportContext,
    TransportError,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

const PUSH: &[u8] = b"\xff\0opaque WS broker push\n";

#[derive(Default)]
struct Sessions {
    opened: Mutex<Vec<TransportContext>>,
    closed: Mutex<Vec<String>>,
}

impl JsonRpcProtocol for Sessions {
    fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
        panic!("WS broker dispatch must retain the persistent context")
    }
    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            assert!(context.push_sender.is_some());
            if input == b"notify" {
                return None;
            }
            Some(Bytes::from(context.connection_id.clone()))
        })
    }
    fn session_opened(&self, context: &TransportContext) {
        context
            .push_sender
            .as_ref()
            .unwrap()
            .send(Bytes::from_static(PUSH))
            .unwrap();
        self.opened.lock().unwrap().push(context.clone());
    }
    fn session_closed(&self, context: &TransportContext) {
        self.closed
            .lock()
            .unwrap()
            .push(context.connection_id.clone());
    }
}

trait PushClient: RpcClientTransport {
    fn push(&self) -> Pin<Box<dyn Future<Output = Result<Bytes, TransportError>> + Send + '_>>;
}
#[cfg(feature = "mqtt")]
impl PushClient for json_rpc_2_websockets::client::transports::mqtt::MqttWebSocketClientTransport {
    fn push(&self) -> Pin<Box<dyn Future<Output = Result<Bytes, TransportError>> + Send + '_>> {
        Box::pin(self.receive_push())
    }
}
#[cfg(feature = "mqtt")]
impl PushClient for json_rpc_2_iot::client::transports::mqtt::MqttClientTransport {
    fn push(&self) -> Pin<Box<dyn Future<Output = Result<Bytes, TransportError>> + Send + '_>> {
        Box::pin(self.receive_push())
    }
}
#[cfg(feature = "amqp")]
impl PushClient for json_rpc_2_websockets::client::transports::amqp::AmqpWebSocketClientTransport {
    fn push(&self) -> Pin<Box<dyn Future<Output = Result<Bytes, TransportError>> + Send + '_>> {
        Box::pin(self.receive_push())
    }
}
#[cfg(feature = "amqp")]
impl PushClient for json_rpc_2_iot::client::transports::amqp::AmqpClientTransport {
    fn push(&self) -> Pin<Box<dyn Future<Output = Result<Bytes, TransportError>> + Send + '_>> {
        Box::pin(self.receive_push())
    }
}

async fn closed(protocol: &Sessions, id: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if protocol
                .closed
                .lock()
                .unwrap()
                .iter()
                .any(|closed| closed == id)
            {
                break;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("missing WS session_closed");
}

async fn close_frame(client: &dyn PushClient, protocol: &Sessions) {
    assert!(!client.push().await.unwrap().is_empty());
    let context = protocol.opened.lock().unwrap()[0].clone();
    let frame = json_rpc_2_core::encode_websocket_close_frame().unwrap();
    client
        .send_with_expectation(Bytes::from(frame), false)
        .await
        .unwrap();
    closed(protocol, &context.connection_id).await;
    assert!(client.push().await.is_err());
    assert!(context.push_sender.unwrap().send(Bytes::new()).is_err());
    assert!(
        client
            .send_with_expectation(Bytes::new(), false)
            .await
            .is_err()
    );
    // Server Close must stop the broker client's heartbeat, not reopen the WS session.
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(protocol.opened.lock().unwrap().len(), 1);
    assert_eq!(protocol.closed.lock().unwrap().len(), 1);
    client.close().await.unwrap();
    protocol.opened.lock().unwrap().clear();
    protocol.closed.lock().unwrap().clear();
}

async fn exercise(
    a: &dyn PushClient,
    b: &dyn PushClient,
    protocol: &Sessions,
    server: json_rpc_2_transport::RunningTransport,
    kind: &str,
) {
    assert_eq!(a.push().await.unwrap().as_ref(), PUSH);
    assert_eq!(b.push().await.unwrap().as_ref(), PUSH);
    // This application envelope is binary and deliberately not business JSON.
    let request = Bytes::from_static(b"APP\xff\0\xffopaque\nrequest");
    let first = a
        .send_with_expectation(request.clone(), true)
        .await
        .unwrap()
        .unwrap();
    uuid_from_bytes(&first);
    assert_eq!(
        a.send_with_expectation(request.clone(), true)
            .await
            .unwrap(),
        Some(first.clone())
    );
    let second = b
        .send_with_expectation(request, true)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(protocol.opened.lock().unwrap().len(), 2);
    let context = protocol
        .opened
        .lock()
        .unwrap()
        .iter()
        .find(|context| context.connection_id.as_bytes() == first.as_ref())
        .unwrap()
        .clone();
    assert_eq!(context.transport, kind);
    a.send_with_expectation(Bytes::from_static(b"notify"), false)
        .await
        .unwrap();
    context
        .push_sender
        .as_ref()
        .unwrap()
        .send(Bytes::from_static(PUSH))
        .unwrap();
    assert_eq!(a.push().await.unwrap().as_ref(), PUSH);
    assert!(
        tokio::time::timeout(Duration::from_millis(80), b.push())
            .await
            .is_err()
    );
    a.reconnect().await.unwrap();
    closed(protocol, &context.connection_id).await;
    assert_eq!(a.push().await.unwrap().as_ref(), PUSH);
    assert!(context.push_sender.unwrap().send(Bytes::new()).is_err());
    a.close().await.unwrap();
    assert!(a.push().await.is_err());
    server.close().await.unwrap();
    closed(protocol, std::str::from_utf8(&second).unwrap()).await;
    assert_eq!(protocol.closed.lock().unwrap().len(), 3);
    b.close().await.unwrap();
}

fn uuid_from_bytes(bytes: &[u8]) {
    let value = std::str::from_utf8(bytes).unwrap();
    assert_eq!(value.len(), 36);
    assert_eq!(value.chars().filter(|ch| *ch == '-').count(), 4);
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires MQTT_BROKER_URL pointing at an isolated real broker"]
async fn ws_mqtt_persistent_sessions_and_decoded_push() {
    use gateway_iot::{MqttConfig, server::transports::mqtt_websocket::MqttWebSocketTransport};
    use json_rpc_2_iot::MqttConfig as MqttClientConfig;
    use json_rpc_2_websockets::client::transports::mqtt::MqttWebSocketClientTransport;
    tokio::time::timeout(Duration::from_secs(20), async {
        let config = MqttConfig {
            broker_url: std::env::var("MQTT_BROKER_URL").unwrap(),
            request_topic: format!("json-rpc-2/ws/sessions/{}", std::process::id()),
            operation_timeout: Duration::from_secs(2),
            response_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let client_config = MqttClientConfig {
            broker_url: config.broker_url.clone(),
            request_topic: config.request_topic.clone(),
            operation_timeout: config.operation_timeout,
            response_timeout: config.response_timeout,
            max_payload_bytes: config.max_payload_bytes,
            heartbeat_interval: config.sessions.heartbeat_interval,
            ca_certificate: config.ca_certificate.clone(),
        };
        let protocol = Arc::new(Sessions::default());
        let server = MqttWebSocketTransport::connect(config.clone())
            .await
            .unwrap()
            .start(protocol.clone());
        let raw =
            json_rpc_2_iot::client::transports::mqtt::MqttClientTransport::connect_with_config(
                client_config.clone(),
            )
            .await
            .unwrap();
        close_frame(&raw, &protocol).await;
        let a = MqttWebSocketClientTransport::connect_with_config(client_config.clone())
            .await
            .unwrap();
        let b = MqttWebSocketClientTransport::connect_with_config(client_config)
            .await
            .unwrap();
        exercise(&a, &b, &protocol, server, "ws-mqtt").await;
    })
    .await
    .expect("WS MQTT test exceeded its bound");
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "requires AMQP_BROKER_URL pointing at an isolated real broker"]
async fn ws_amqp_persistent_sessions_and_decoded_push() {
    use gateway_iot::{AmqpConfig, server::transports::amqp_websocket::AmqpWebSocketTransport};
    use json_rpc_2_iot::AmqpConfig as AmqpClientConfig;
    use json_rpc_2_websockets::client::transports::amqp::AmqpWebSocketClientTransport;
    tokio::time::timeout(Duration::from_secs(20), async {
        let config = AmqpConfig {
            broker_url: std::env::var("AMQP_BROKER_URL").unwrap(),
            request_queue: format!("json-rpc-2.ws.sessions.{}", std::process::id()),
            operation_timeout: Duration::from_secs(2),
            response_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let client_config = AmqpClientConfig {
            broker_url: config.broker_url.clone(),
            request_queue: config.request_queue.clone(),
            operation_timeout: config.operation_timeout,
            response_timeout: config.response_timeout,
            max_payload_bytes: config.max_payload_bytes,
            heartbeat_interval: config.sessions.heartbeat_interval,
            prefetch: config.prefetch,
        };
        let protocol = Arc::new(Sessions::default());
        let server = AmqpWebSocketTransport::connect(config.clone())
            .await
            .unwrap()
            .start(protocol.clone());
        let raw =
            json_rpc_2_iot::client::transports::amqp::AmqpClientTransport::connect_with_config(
                client_config.clone(),
            )
            .await
            .unwrap();
        close_frame(&raw, &protocol).await;
        let a = AmqpWebSocketClientTransport::connect_with_config(client_config.clone())
            .await
            .unwrap();
        let b = AmqpWebSocketClientTransport::connect_with_config(client_config)
            .await
            .unwrap();
        exercise(&a, &b, &protocol, server, "ws-amqp").await;
    })
    .await
    .expect("WS AMQP test exceeded its bound");
}
