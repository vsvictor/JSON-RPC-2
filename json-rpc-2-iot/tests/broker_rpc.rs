//! Real brokers only; no fake wire peers. See tests/brokers/README.md.
#![cfg(any(feature = "mqtt", feature = "amqp"))]
use bytes::Bytes;
use json_rpc_2_transport::{JsonRpcProtocol, ProtocolFuture, RpcClientTransport, Transport};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Echo(Arc<AtomicUsize>);
impl JsonRpcProtocol for Echo {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            if input == b"notification" {
                return None;
            }
            if input.starts_with(b"slow") {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Some(Bytes::copy_from_slice(input))
        })
    }
}

#[cfg(feature = "mqtt")]
fn mqtt_server_config(config: &json_rpc_2_iot::MqttConfig) -> gateway_iot::MqttConfig {
    gateway_iot::MqttConfig {
        broker_url: config.broker_url.clone(),
        request_topic: config.request_topic.clone(),
        operation_timeout: config.operation_timeout,
        response_timeout: config.response_timeout,
        max_payload_bytes: config.max_payload_bytes,
        ca_certificate: config.ca_certificate.clone(),
        ..Default::default()
    }
}

#[cfg(feature = "amqp")]
fn amqp_server_config(config: &json_rpc_2_iot::AmqpConfig) -> gateway_iot::AmqpConfig {
    gateway_iot::AmqpConfig {
        broker_url: config.broker_url.clone(),
        request_queue: config.request_queue.clone(),
        operation_timeout: config.operation_timeout,
        response_timeout: config.response_timeout,
        max_payload_bytes: config.max_payload_bytes,
        prefetch: config.prefetch,
        ..Default::default()
    }
}

async fn exercise(client: Arc<dyn RpcClientTransport>, seen: Arc<AtomicUsize>) {
    // Control traffic must not enter the protocol handler.
    client.transport_ping().await.unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 0);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let client = client.clone();
        tasks.spawn(async move {
            let payload = Bytes::from(format!(
                "{}-{index}",
                if index % 2 == 0 { "slow" } else { "fast" }
            ));
            assert_eq!(
                client
                    .send_with_expectation(payload.clone(), true)
                    .await
                    .unwrap(),
                Some(payload)
            );
        });
    }

    while let Some(task) = tasks.join_next().await {
        task.unwrap();
    }
    assert!(
        client
            .send_with_expectation(Bytes::from_static(b"notification"), false)
            .await
            .unwrap()
            .is_none()
    );
    let token = tokio_util::sync::CancellationToken::new();
    let future = client.send_cancelable(Bytes::from_static(b"slow-cancel"), true, token.clone());
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => panic!("unexpected early result: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(20)) => token.cancel(),
    }
    assert!(future.await.is_err());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        client
            .send_with_expectation(Bytes::from_static(b"after-cancel"), true)
            .await
            .unwrap()
            .unwrap(),
        "after-cancel"
    );
    client.reconnect().await.unwrap();
    client.transport_ping().await.unwrap();
    assert_eq!(
        client
            .send_with_expectation(Bytes::from_static(b"reconnected"), true)
            .await
            .unwrap()
            .unwrap(),
        "reconnected"
    );
    let closing_request = client.send_with_expectation(Bytes::from_static(b"slow-closing"), true);
    tokio::pin!(closing_request);
    tokio::select! {
        result = &mut closing_request => panic!("unexpected early result: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
    }
    client.close().await.unwrap();
    assert!(closing_request.await.is_err());
    client.close().await.unwrap();
    assert!(
        client
            .send_with_expectation(Bytes::from_static(b"closed"), true)
            .await
            .is_err()
    );
    assert!(seen.load(Ordering::SeqCst) >= 35);
}

async fn wait_ready(client: &dyn RpcClientTransport) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while client.transport_ping().await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("server subscription did not become ready");
}

async fn graceful_shutdown(
    client: &dyn RpcClientTransport,
    server: json_rpc_2_transport::RunningTransport,
    seen: Arc<AtomicUsize>,
) {
    let before = seen.load(Ordering::SeqCst);
    let request = client.send_with_expectation(Bytes::from_static(b"slow-drain"), true);
    tokio::pin!(request);
    tokio::select! {
        result = &mut request => panic!("unexpected early result: {result:?}"),
        _ = async {
            while seen.load(Ordering::SeqCst) == before {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } => {}
    }
    let (response, closed) = tokio::join!(request, server.close());
    closed.unwrap();
    assert_eq!(response.unwrap().unwrap(), "slow-drain");
    client.close().await.unwrap();
}

async fn same_rpc_id_isolated(a: &dyn RpcClientTransport, b: &dyn RpcClientTransport) {
    let first =
        Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"echo","params":["first"]}"#);
    let second =
        Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"echo","params":["second"]}"#);
    let (first_result, second_result) = tokio::join!(a.send(first.clone()), b.send(second.clone()));
    assert_eq!(first_result.unwrap(), Some(first));
    assert_eq!(second_result.unwrap(), Some(second));
    assert!(
        a.send(Bytes::from_static(
            br#"[{"jsonrpc":"2.0","method":"echo"},{"jsonrpc":"2.0","method":"echo"}]"#
        ))
        .await
        .unwrap()
        .is_none()
    );
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires MQTT_BROKER_URL and a real MQTT 3.1.1 broker"]
async fn mqtt_real_broker() {
    use gateway_iot::server::transports::mqtt::MqttTransport;
    use json_rpc_2_iot::{MqttConfig, client::transports::mqtt::MqttClientTransport};
    let config = MqttConfig {
        broker_url: std::env::var("MQTT_BROKER_URL").expect("set MQTT_BROKER_URL"),
        request_topic: format!("json-rpc-2/tests/{}", uuid::Uuid::new_v4()),
        response_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    let seen = Arc::new(AtomicUsize::new(0));
    let server = MqttTransport::connect(mqtt_server_config(&config))
        .await
        .unwrap()
        .start(Arc::new(Echo(seen.clone())));
    let client = Arc::new(
        MqttClientTransport::connect_with_config(config.clone())
            .await
            .unwrap(),
    );
    wait_ready(client.as_ref()).await;
    let other = MqttClientTransport::connect_with_config(config.clone())
        .await
        .unwrap();
    same_rpc_id_isolated(client.as_ref(), &other).await;
    other.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    seen.store(0, Ordering::SeqCst);
    exercise(client, seen.clone()).await;
    let client = MqttClientTransport::connect_with_config(config)
        .await
        .unwrap();
    graceful_shutdown(&client, server, seen).await;
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "requires AMQP_BROKER_URL and a real AMQP 0-9-1 broker"]
async fn amqp_real_broker() {
    use gateway_iot::server::transports::amqp::AmqpTransport;
    use json_rpc_2_iot::{AmqpConfig, client::transports::amqp::AmqpClientTransport};
    let config = AmqpConfig {
        broker_url: std::env::var("AMQP_BROKER_URL").expect("set AMQP_BROKER_URL"),
        request_queue: format!("json-rpc-2.tests.{}", uuid::Uuid::new_v4()),
        response_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    let seen = Arc::new(AtomicUsize::new(0));
    let server = AmqpTransport::connect(amqp_server_config(&config))
        .await
        .unwrap()
        .start(Arc::new(Echo(seen.clone())));
    let client = Arc::new(
        AmqpClientTransport::connect_with_config(config.clone())
            .await
            .unwrap(),
    );
    wait_ready(client.as_ref()).await;
    let other = AmqpClientTransport::connect_with_config(config.clone())
        .await
        .unwrap();
    same_rpc_id_isolated(client.as_ref(), &other).await;
    other.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    seen.store(0, Ordering::SeqCst);
    exercise(client, seen.clone()).await;
    let client = AmqpClientTransport::connect_with_config(config)
        .await
        .unwrap();
    graceful_shutdown(&client, server, seen).await;
}

#[cfg(feature = "mqtt")]
#[tokio::test]
async fn mqtt_bad_config_and_bounded_connect() {
    use gateway_iot::server::transports::mqtt::MqttTransport;
    use json_rpc_2_iot::{MqttConfig, client::transports::mqtt::MqttClientTransport};
    let config = MqttConfig {
        broker_url: "invalid://user:private@example.test".into(),
        ..Default::default()
    };
    let error = MqttClientTransport::connect_with_config(config)
        .await
        .err()
        .unwrap();
    assert!(!error.to_string().contains("private"));
    assert!(
        MqttTransport::connect(gateway_iot::MqttConfig {
            broker_url: "invalid://broker".into(),
            ..Default::default()
        })
        .await
        .is_err()
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = MqttConfig {
        broker_url: format!("mqtt://{}", listener.local_addr().unwrap()),
        operation_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            MqttClientTransport::connect_with_config(config)
        )
        .await
        .unwrap()
        .is_err()
    );
}

#[cfg(feature = "amqp")]
#[tokio::test]
async fn amqp_bad_config_and_bounded_connect() {
    use gateway_iot::server::transports::amqp::AmqpTransport;
    use json_rpc_2_iot::{AmqpConfig, client::transports::amqp::AmqpClientTransport};
    let config = AmqpConfig {
        broker_url: "invalid://user:private@example.test".into(),
        ..Default::default()
    };
    let error = AmqpClientTransport::connect_with_config(config)
        .await
        .err()
        .unwrap();
    assert!(!error.to_string().contains("private"));
    assert!(
        AmqpTransport::connect(gateway_iot::AmqpConfig {
            broker_url: "invalid://broker".into(),
            ..Default::default()
        })
        .await
        .is_err()
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AmqpConfig {
        broker_url: format!("amqp://{}", listener.local_addr().unwrap()),
        operation_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            AmqpClientTransport::connect_with_config(config)
        )
        .await
        .unwrap()
        .is_err()
    );
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires MQTT_BROKER_URL and a real MQTT 3.1.1 broker"]
async fn mqtt_receive_timeout_without_rpc_consumer() {
    use json_rpc_2_iot::{MqttConfig, client::transports::mqtt::MqttClientTransport};
    let config = MqttConfig {
        broker_url: std::env::var("MQTT_BROKER_URL").expect("set MQTT_BROKER_URL"),
        request_topic: format!("json-rpc-2/absent/{}", uuid::Uuid::new_v4()),
        response_timeout: Duration::from_millis(100),
        max_payload_bytes: 32,
        ..Default::default()
    };
    let client = MqttClientTransport::connect_with_config(config)
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client.send_with_expectation(Bytes::from_static(b"request"), true),
    )
    .await
    .unwrap();
    assert!(response.unwrap_err().to_string().contains("timed out"));
    assert!(
        client
            .send_with_expectation(Bytes::from(vec![0; 33]), false)
            .await
            .is_err()
    );
    client.close().await.unwrap();
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "requires AMQP_BROKER_URL and a real AMQP 0-9-1 broker"]
async fn amqp_receive_timeout_without_rpc_consumer() {
    use json_rpc_2_iot::{AmqpConfig, client::transports::amqp::AmqpClientTransport};
    let config = AmqpConfig {
        broker_url: std::env::var("AMQP_BROKER_URL").expect("set AMQP_BROKER_URL"),
        request_queue: format!("json-rpc-2.absent.{}", uuid::Uuid::new_v4()),
        response_timeout: Duration::from_millis(100),
        max_payload_bytes: 32,
        ..Default::default()
    };
    let client = AmqpClientTransport::connect_with_config(config)
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client.send_with_expectation(Bytes::from_static(b"request"), true),
    )
    .await
    .unwrap();
    assert!(response.unwrap_err().to_string().contains("timed out"));
    assert!(
        client
            .send_with_expectation(Bytes::from(vec![0; 33]), false)
            .await
            .is_err()
    );
    client.close().await.unwrap();
}
