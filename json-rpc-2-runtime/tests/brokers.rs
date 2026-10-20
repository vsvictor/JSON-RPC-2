//! External broker orchestration. Run explicitly with `--ignored`.
//! Endpoints may include credentials and TLS; tests never print them.

use json_rpc_2_client::RpcClient;
use json_rpc_2_runtime::{MultiTransportServer, RunningServer, TransportKind};
use json_rpc_2_server::RpcServer;
use json_rpc_2_transport::RpcClientTransport;
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn protocol() -> Arc<RpcServer> {
    Arc::new(
        RpcServer::builder()
            .register("echo", |params| async move { Ok(params) })
            .unwrap()
            .build(),
    )
}

fn service() -> String {
    format!(
        "runtime-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn exercise<T: RpcClientTransport>(
    running: RunningServer,
    kind: TransportKind,
    transport: T,
) {
    assert_eq!(
        running.listeners().count(),
        0,
        "brokers must not open local listeners"
    );
    assert_eq!(running.broker_transports().collect::<Vec<_>>(), [kind]);
    let transport = Arc::new(transport);
    let client = RpcClient::new(Arc::clone(&transport));
    let response = client
        .request_value("echo", Some(json!({"broker": kind.id()})))
        .await
        .unwrap();
    assert_eq!(response, json!({"broker": kind.id()}));
    client
        .notify("echo", Some(json!({"notification": true})))
        .await
        .unwrap();
    transport.transport_ping().await.unwrap();
    transport.reconnect().await.unwrap();
    assert_eq!(
        client
            .request_value("echo", Some(json!({"reconnected": true})))
            .await
            .unwrap(),
        json!({"reconnected": true})
    );
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), running.close())
        .await
        .expect("broker shutdown timed out")
        .unwrap();
}

#[cfg(feature = "mqtt")]
fn mqtt_config() -> json_rpc_2_runtime::MqttConfig {
    let mut config = json_rpc_2_runtime::MqttConfig::default();
    if let Ok(endpoint) = std::env::var("JSON_RPC_TEST_MQTT_URL") {
        config.broker_url = endpoint;
    }
    config.request_topic = format!("json-rpc-2/test/{}", service());
    config
}

#[cfg(feature = "amqp")]
fn amqp_config() -> json_rpc_2_runtime::AmqpConfig {
    let mut config = json_rpc_2_runtime::AmqpConfig::default();
    if let Ok(endpoint) = std::env::var("JSON_RPC_TEST_AMQP_URL") {
        config.broker_url = endpoint;
    }
    config.request_queue = format!("json-rpc-2.test.{}", service());
    config
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires external MQTT 3.1.1 broker; set JSON_RPC_TEST_MQTT_URL"]
async fn mqtt_subscribes_once_for_multiple_bind_addresses() {
    let config = mqtt_config();
    let running = MultiTransportServer::builder(protocol())
        .set_bind(["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()])
        .set_mqtt_config(config.clone())
        .start()
        .await
        .unwrap();
    let transport =
        json_rpc_2_iot::client::transports::mqtt::MqttClientTransport::connect_with_config(config)
            .await
            .unwrap();
    exercise(running, TransportKind::Mqtt, transport).await;
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "requires external AMQP 0-9-1 broker; set JSON_RPC_TEST_AMQP_URL"]
async fn amqp_consumes_without_a_local_bind() {
    let config = amqp_config();
    let running = MultiTransportServer::builder(protocol())
        .set_amqp_config(config.clone())
        .start()
        .await
        .unwrap();
    let transport =
        json_rpc_2_iot::client::transports::amqp::AmqpClientTransport::connect_with_config(config)
            .await
            .unwrap();
    exercise(running, TransportKind::Amqp, transport).await;
}

#[cfg(all(feature = "websocket", feature = "mqtt"))]
#[tokio::test]
#[ignore = "requires external MQTT 3.1.1 broker; set JSON_RPC_TEST_MQTT_URL"]
async fn websocket_mqtt_uses_external_service() {
    let config = mqtt_config();
    let running = MultiTransportServer::builder(protocol())
        .set_ws_mqtt_config(config.clone())
        .start()
        .await
        .unwrap();
    let transport = json_rpc_2_websockets::client::transports::mqtt::MqttWebSocketClientTransport::connect_with_config(config).await.unwrap();
    exercise(running, TransportKind::WebSocketMqtt, transport).await;
}

#[cfg(all(feature = "websocket", feature = "amqp"))]
#[tokio::test]
#[ignore = "requires external AMQP 0-9-1 broker; set JSON_RPC_TEST_AMQP_URL"]
async fn websocket_amqp_uses_external_service() {
    let config = amqp_config();
    let running = MultiTransportServer::builder(protocol())
        .set_ws_amqp_config(config.clone())
        .start()
        .await
        .unwrap();
    let transport = json_rpc_2_websockets::client::transports::amqp::AmqpWebSocketClientTransport::connect_with_config(config).await.unwrap();
    exercise(running, TransportKind::WebSocketAmqp, transport).await;
}

#[cfg(feature = "mqtt")]
#[test]
fn mqtt_configuration_enables_a_service_without_a_local_port() {
    let server = MultiTransportServer::builder(protocol())
        .set_mqtt_config(mqtt_config())
        .build();
    server.validate().unwrap();
    assert_eq!(server.port(TransportKind::Mqtt), None);
    assert_eq!(
        server.transports().collect::<Vec<_>>(),
        [TransportKind::Mqtt]
    );
}

#[cfg(feature = "amqp")]
#[test]
fn amqp_configuration_enables_a_service_without_a_local_port() {
    let server = MultiTransportServer::builder(protocol())
        .set_amqp_config(amqp_config())
        .build();
    server.validate().unwrap();
    assert_eq!(server.port(TransportKind::Amqp), None);
    assert_eq!(
        server.transports().collect::<Vec<_>>(),
        [TransportKind::Amqp]
    );
}

#[cfg(feature = "mqtt")]
#[test]
fn mqtt_port_zero_is_not_an_ephemeral_local_listener() {
    let mut server = MultiTransportServer::new(protocol());
    server.set_mqtt_port(0);
    assert_eq!(
        server.validate().unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    server.clear_port(TransportKind::Mqtt);
    assert_eq!(server.transports().count(), 0);
}

#[cfg(all(feature = "mqtt", feature = "websocket"))]
#[test]
fn mqtt_raw_and_websocket_must_not_share_a_service_topic() {
    let config = mqtt_config();
    let server = MultiTransportServer::builder(protocol())
        .set_mqtt_config(config.clone())
        .set_ws_mqtt_config(config)
        .build();
    assert_eq!(
        server.validate().unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[cfg(all(feature = "amqp", feature = "websocket"))]
#[test]
fn amqp_raw_and_websocket_must_not_share_a_service_queue() {
    let config = amqp_config();
    let server = MultiTransportServer::builder(protocol())
        .set_amqp_config(config.clone())
        .set_ws_amqp_config(config)
        .build();
    assert_eq!(
        server.validate().unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[cfg(feature = "amqp")]
#[test]
fn amqp_port_zero_is_not_an_ephemeral_local_listener() {
    let mut server = MultiTransportServer::new(protocol());
    server.set_amqp_port(0);
    assert_eq!(
        server.validate().unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    server.clear_port(TransportKind::Amqp);
    assert_eq!(server.transports().count(), 0);
}

#[cfg(all(feature = "mqtt", feature = "amqp", feature = "websocket"))]
#[test]
fn broker_port_conveniences_do_not_claim_local_ports() {
    let mut server = MultiTransportServer::new(protocol());
    server.set_mqtt_port(1883).set_ws_mqtt_port(1883);
    server.set_amqp_port(5672).set_ws_amqp_port(5672);
    server.validate().unwrap();
    assert_eq!(server.bind().len(), 0);
    assert_eq!(server.transports().count(), 4);
    assert_ne!(
        server.mqtt_config().unwrap().request_topic,
        server.ws_mqtt_config().unwrap().request_topic
    );
    assert_ne!(
        server.amqp_config().unwrap().request_queue,
        server.ws_amqp_config().unwrap().request_queue
    );
}

#[cfg(feature = "mqtt")]
#[tokio::test]
async fn mqtt_start_waits_for_subscription_and_reports_handshake_failure() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let mut config = json_rpc_2_runtime::MqttConfig::for_address(listener.local_addr().unwrap());
    config.operation_timeout = Duration::from_millis(100);
    let started = tokio::time::timeout(
        Duration::from_secs(2),
        MultiTransportServer::builder(protocol())
            .set_mqtt_config(config)
            .start(),
    )
    .await
    .expect("MQTT startup must be bounded");
    assert!(
        started.is_err(),
        "a TCP listener without MQTT must not count as a ready broker"
    );
}

#[cfg(feature = "amqp")]
#[tokio::test]
async fn amqp_start_waits_for_consumer_and_reports_handshake_failure() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let mut config = json_rpc_2_runtime::AmqpConfig::for_address(listener.local_addr().unwrap());
    config.operation_timeout = Duration::from_millis(100);
    let started = tokio::time::timeout(
        Duration::from_secs(2),
        MultiTransportServer::builder(protocol())
            .set_amqp_config(config)
            .start(),
    )
    .await
    .expect("AMQP startup must be bounded");
    assert!(
        started.is_err(),
        "a TCP listener without AMQP must not count as a ready broker"
    );
}
