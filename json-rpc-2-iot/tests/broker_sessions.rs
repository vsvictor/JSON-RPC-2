//! Bounded, opt-in integration tests against real, isolated broker fixtures.
#![cfg(any(feature = "mqtt", feature = "amqp"))]

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

const PUSH: &[u8] = b"\xff\0unsolicited\npush";

#[cfg(feature = "mqtt")]
#[derive(Default)]
struct SaturatedSessions {
    sessions: Sessions,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    dispatched: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "mqtt")]
impl JsonRpcProtocol for SaturatedSessions {
    fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
        panic!("persistent broker context required")
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.dispatched
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if input == b"hold" {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Some(Bytes::from(context.connection_id.clone()))
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        self.sessions.session_opened(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.sessions.session_closed(context);
    }
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires MQTT_BROKER_URL; verifies the real lease under saturated dispatch"]
async fn mqtt_saturated_dispatch_keeps_close_heartbeat_and_expiration_live() {
    use json_rpc_2_iot::{
        MqttConfig, client::transports::mqtt::MqttClientTransport,
        server::transports::mqtt::MqttTransport,
    };
    use std::sync::atomic::Ordering;
    tokio::time::timeout(Duration::from_secs(65), async {
        let config = MqttConfig {
            broker_url: std::env::var("MQTT_BROKER_URL").unwrap(),
            request_topic: format!("json-rpc-2/saturated/{}", uuid::Uuid::new_v4()),
            operation_timeout: Duration::from_secs(1),
            response_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let protocol = Arc::new(SaturatedSessions::default());
        let mut runtime = json_rpc_2_transport::TransportRuntime::default();
        runtime.limits.max_concurrent_requests = 1;
        let server = MqttTransport::connect(config.clone())
            .await
            .unwrap()
            .with_runtime(Arc::new(runtime))
            .start(protocol.clone());
        let abandoned = MqttClientTransport::connect_with_config(config.clone())
            .await
            .unwrap();
        abandoned.receive_push().await.unwrap();
        let abandoned_context = protocol
            .sessions
            .opened
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        let closing = MqttClientTransport::connect_with_config(config.clone())
            .await
            .unwrap();
        closing.receive_push().await.unwrap();
        let closing_id = protocol
            .sessions
            .opened
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .connection_id
            .clone();
        let busy = Arc::new(
            MqttClientTransport::connect_with_config(config.clone())
                .await
                .unwrap(),
        );
        busy.receive_push().await.unwrap();
        let alive = MqttClientTransport::connect_with_config(MqttConfig {
            response_timeout: Duration::from_millis(150),
            ..config
        })
        .await
        .unwrap();
        alive.receive_push().await.unwrap();
        let alive_id = protocol
            .sessions
            .opened
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .connection_id
            .clone();
        let task_client = busy.clone();
        let request = tokio::spawn(async move {
            task_client
                .send_with_expectation(Bytes::from_static(b"hold"), true)
                .await
        });
        protocol.entered.notified().await;
        assert!(!request.is_finished());
        // An excess RPC is not dispatched or retained for later replay.
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                alive.send_with_expectation(Bytes::from_static(b"reject"), true),
            )
            .await
            .unwrap()
            .is_err()
        );
        assert_eq!(protocol.dispatched.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(2), closing.close())
            .await
            .unwrap()
            .unwrap();
        closed(&protocol.sessions, &closing_id).await;
        drop(abandoned);
        tokio::time::timeout(Duration::from_secs(57), async {
            loop {
                if protocol
                    .sessions
                    .closed
                    .lock()
                    .unwrap()
                    .contains(&abandoned_context.connection_id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("abandoned session did not expire under saturated dispatch");
        assert!(
            !request.is_finished(),
            "dispatcher must remain saturated through lease expiry"
        );
        assert_eq!(protocol.dispatched.load(Ordering::SeqCst), 1);
        assert!(!protocol.sessions.closed.lock().unwrap().contains(&alive_id));
        assert!(
            abandoned_context
                .push_sender
                .unwrap()
                .send(Bytes::new())
                .is_err()
        );
        protocol.release.notify_one();
        request.await.unwrap().unwrap();
        // Heartbeats retained the idle client's UUID, and the rejected RPC was never replayed.
        assert_eq!(
            alive
                .send_with_expectation(Bytes::from_static(b"after"), true)
                .await
                .unwrap(),
            Some(Bytes::from(alive_id))
        );
        assert_eq!(protocol.dispatched.load(Ordering::SeqCst), 2);
        alive.close().await.unwrap();
        busy.close().await.unwrap();
        server.close().await.unwrap();
    })
    .await
    .expect("saturated-dispatch test exceeded its bound");
}

#[derive(Default)]
struct Sessions {
    opened: Mutex<Vec<TransportContext>>,
    closed: Mutex<Vec<String>>,
}

impl JsonRpcProtocol for Sessions {
    fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
        panic!("broker clients must have persistent contexts")
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
impl PushClient for json_rpc_2_iot::client::transports::mqtt::MqttClientTransport {
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
    .expect("session_closed was not delivered");
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
    let request = Bytes::from_static(b"\xff\0opaque request\n");
    let first = a
        .send_with_expectation(request.clone(), true)
        .await
        .unwrap()
        .unwrap();
    uuid::Uuid::parse_str(std::str::from_utf8(&first).unwrap()).unwrap();
    assert_eq!(
        a.send_with_expectation(request.clone(), true)
            .await
            .unwrap(),
        Some(first.clone())
    );
    let second = b
        .send_with_expectation(request.clone(), true)
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
    // No request is running: the target client's private route carries the push.
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
    a.send_with_expectation(Bytes::from_static(b"notify"), false)
        .await
        .unwrap();
    context
        .push_sender
        .as_ref()
        .unwrap()
        .send(Bytes::from_static(b"after notification"))
        .unwrap();
    assert_eq!(a.push().await.unwrap(), "after notification");
    a.reconnect().await.unwrap();
    closed(protocol, &context.connection_id).await;
    assert_eq!(a.push().await.unwrap().as_ref(), PUSH);
    let third = a
        .send_with_expectation(request, true)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, third);
    assert_eq!(protocol.opened.lock().unwrap().len(), 3);
    assert!(context.push_sender.unwrap().send(Bytes::new()).is_err());
    a.close().await.unwrap();
    closed(protocol, std::str::from_utf8(&third).unwrap()).await;
    assert!(a.push().await.is_err());
    server.close().await.unwrap();
    closed(protocol, std::str::from_utf8(&second).unwrap()).await;
    b.close().await.unwrap();
}

async fn exercise_lease(
    a: Box<dyn PushClient>,
    b: Box<dyn PushClient>,
    protocol: &Sessions,
    server: json_rpc_2_transport::RunningTransport,
) {
    assert_eq!(a.push().await.unwrap().as_ref(), PUSH);
    assert_eq!(b.push().await.unwrap().as_ref(), PUSH);
    let request = Bytes::from_static(b"lease identity");
    let first = a
        .send_with_expectation(request.clone(), true)
        .await
        .unwrap()
        .unwrap();
    let second = b
        .send_with_expectation(request.clone(), true)
        .await
        .unwrap()
        .unwrap();
    drop(a);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if protocol
                .closed
                .lock()
                .unwrap()
                .iter()
                .any(|id| id.as_bytes() == first.as_ref())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("abrupt client loss must expire its broker session");
    // The other client was idle for the same period, but heartbeats keep its UUID alive.
    assert_eq!(protocol.closed.lock().unwrap().len(), 1);
    assert_eq!(
        b.send_with_expectation(request, true).await.unwrap(),
        Some(second)
    );
    server.close().await.unwrap();
    b.close().await.unwrap();
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires MQTT_BROKER_URL; exercises the real 45-second session lease"]
async fn mqtt_abrupt_loss_expires_while_idle_client_heartbeats() {
    use json_rpc_2_iot::{
        MqttConfig, client::transports::mqtt::MqttClientTransport,
        server::transports::mqtt::MqttTransport,
    };
    tokio::time::timeout(Duration::from_secs(65), async {
        let config = MqttConfig {
            broker_url: std::env::var("MQTT_BROKER_URL").unwrap(),
            request_topic: format!("json-rpc-2/lease/{}", uuid::Uuid::new_v4()),
            operation_timeout: Duration::from_secs(2),
            response_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let protocol = Arc::new(Sessions::default());
        let server = MqttTransport::connect(config.clone())
            .await
            .unwrap()
            .start(protocol.clone());
        let a = MqttClientTransport::connect_with_config(config.clone())
            .await
            .unwrap();
        let b = MqttClientTransport::connect_with_config(config)
            .await
            .unwrap();
        exercise_lease(Box::new(a), Box::new(b), &protocol, server).await;
    })
    .await
    .expect("MQTT lease test exceeded its bound");
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "requires AMQP_BROKER_URL; exercises the real 45-second session lease"]
async fn amqp_abrupt_loss_expires_while_idle_client_heartbeats() {
    use json_rpc_2_iot::{
        AmqpConfig, client::transports::amqp::AmqpClientTransport,
        server::transports::amqp::AmqpTransport,
    };
    tokio::time::timeout(Duration::from_secs(65), async {
        let config = AmqpConfig {
            broker_url: std::env::var("AMQP_BROKER_URL").unwrap(),
            request_queue: format!("json-rpc-2.lease.{}", uuid::Uuid::new_v4()),
            operation_timeout: Duration::from_secs(2),
            response_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let protocol = Arc::new(Sessions::default());
        let server = AmqpTransport::connect(config.clone())
            .await
            .unwrap()
            .start(protocol.clone());
        let a = AmqpClientTransport::connect_with_config(config.clone())
            .await
            .unwrap();
        let b = AmqpClientTransport::connect_with_config(config)
            .await
            .unwrap();
        exercise_lease(Box::new(a), Box::new(b), &protocol, server).await;
    })
    .await
    .expect("AMQP lease test exceeded its bound");
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires MQTT_BROKER_URL pointing at an isolated real broker"]
async fn mqtt_persistent_sessions_and_independent_push() {
    use json_rpc_2_iot::{
        MqttConfig, client::transports::mqtt::MqttClientTransport,
        server::transports::mqtt::MqttTransport,
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        let config = MqttConfig {
            broker_url: std::env::var("MQTT_BROKER_URL").unwrap(),
            request_topic: format!("json-rpc-2/sessions/{}", uuid::Uuid::new_v4()),
            operation_timeout: Duration::from_secs(2),
            response_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let protocol = Arc::new(Sessions::default());
        let server = MqttTransport::connect(config.clone())
            .await
            .unwrap()
            .start(protocol.clone());
        let a = MqttClientTransport::connect_with_config(config.clone())
            .await
            .unwrap();
        let b = MqttClientTransport::connect_with_config(config)
            .await
            .unwrap();
        exercise(&a, &b, &protocol, server, "mqtt").await;
    })
    .await
    .expect("MQTT session test exceeded its bound");
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "requires AMQP_BROKER_URL pointing at an isolated real broker"]
async fn amqp_persistent_sessions_and_independent_push() {
    use json_rpc_2_iot::{
        AmqpConfig, client::transports::amqp::AmqpClientTransport,
        server::transports::amqp::AmqpTransport,
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        let config = AmqpConfig {
            broker_url: std::env::var("AMQP_BROKER_URL").unwrap(),
            request_queue: format!("json-rpc-2.sessions.{}", uuid::Uuid::new_v4()),
            operation_timeout: Duration::from_secs(2),
            response_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let protocol = Arc::new(Sessions::default());
        let server = AmqpTransport::connect(config.clone())
            .await
            .unwrap()
            .start(protocol.clone());
        let a = AmqpClientTransport::connect_with_config(config.clone())
            .await
            .unwrap();
        let b = AmqpClientTransport::connect_with_config(config)
            .await
            .unwrap();
        exercise(&a, &b, &protocol, server, "amqp").await;
    })
    .await
    .expect("AMQP session test exceeded its bound");
}
