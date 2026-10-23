//! Disruptive, real Docker broker restart tests; run separately, serially.
#![cfg(any(feature = "mqtt", feature = "amqp"))]
use bytes::Bytes;
use json_rpc_2_transport::{
    JsonRpcProtocol, ProtocolFuture, RpcClientTransport, RunningTransport, Transport,
};
use std::{sync::Arc, time::Duration};

struct Echo;
impl JsonRpcProtocol for Echo {
    fn handle_message<'a>(&'a self, bytes: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move { Some(Bytes::copy_from_slice(bytes)) })
    }
}

async fn recovery(
    client: Arc<dyn RpcClientTransport>,
    server: RunningTransport,
    broker: &'static str,
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while client.transport_ping().await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("initial RPC subscription");
    tokio::task::spawn_blocking(move || {
        let docker = std::env::var("DOCKER_BIN").unwrap_or_else(|_| "docker".into());
        let compose = format!("{}/tests/brokers/compose.yaml", env!("CARGO_MANIFEST_DIR"));
        let status = std::process::Command::new(docker)
            .args(["compose", "-f", &compose, "restart", broker])
            .status()
            .expect("Docker is required for restart tests");
        assert!(status.success());
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if client.reconnect().await.is_ok() && client.transport_ping().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("client and server consumer recovery after broker restart");
    assert_eq!(
        client
            .send_with_expectation(Bytes::from_static(b"after-restart"), true)
            .await
            .unwrap()
            .unwrap(),
        "after-restart"
    );
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "disruptive: requires real compose brokers, MQTT_BROKER_URL, and Docker"]
async fn mqtt_subscription_recovers_after_broker_restart() {
    use gateway_iot::server::transports::mqtt::MqttTransport;
    use json_rpc_2_iot::{MqttConfig, client::transports::mqtt::MqttClientTransport};
    let config = MqttConfig {
        broker_url: std::env::var("MQTT_BROKER_URL").expect("MQTT_BROKER_URL"),
        request_topic: format!("json-rpc-2/restart/{}", uuid::Uuid::new_v4()),
        operation_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let server_config = gateway_iot::MqttConfig {
        broker_url: config.broker_url.clone(),
        request_topic: config.request_topic.clone(),
        operation_timeout: config.operation_timeout,
        response_timeout: config.response_timeout,
        max_payload_bytes: config.max_payload_bytes,
        ca_certificate: config.ca_certificate.clone(),
        ..Default::default()
    };
    let server = MqttTransport::new(server_config).start(Arc::new(Echo));
    let client = Arc::new(
        MqttClientTransport::connect_with_config(config)
            .await
            .unwrap(),
    );
    recovery(client, server, "mqtt").await;
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "disruptive: requires real compose brokers, AMQP_BROKER_URL, and Docker"]
async fn amqp_consumer_recovers_after_broker_restart() {
    use gateway_iot::server::transports::amqp::AmqpTransport;
    use json_rpc_2_iot::{AmqpConfig, client::transports::amqp::AmqpClientTransport};
    let config = AmqpConfig {
        broker_url: std::env::var("AMQP_BROKER_URL").expect("AMQP_BROKER_URL"),
        request_queue: format!("json-rpc-2.restart.{}", uuid::Uuid::new_v4()),
        operation_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let server_config = gateway_iot::AmqpConfig {
        broker_url: config.broker_url.clone(),
        request_queue: config.request_queue.clone(),
        operation_timeout: config.operation_timeout,
        response_timeout: config.response_timeout,
        max_payload_bytes: config.max_payload_bytes,
        prefetch: config.prefetch,
        ..Default::default()
    };
    let server = AmqpTransport::new(server_config).start(Arc::new(Echo));
    let client = Arc::new(
        AmqpClientTransport::connect_with_config(config)
            .await
            .unwrap(),
    );
    recovery(client, server, "amqp").await;
}
