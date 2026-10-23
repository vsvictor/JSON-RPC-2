//! Requires authenticated brokers; separate from anonymous-compatible CI tests.
#![cfg(any(feature = "mqtt", feature = "amqp"))]
use std::time::Duration;

fn invalid_credentials(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("broker URL scheme");
    let endpoint = rest.rsplit_once('@').map_or(rest, |(_, endpoint)| endpoint);
    format!("{scheme}://rpc-invalid:invalid-password@{endpoint}")
}

#[cfg(feature = "mqtt")]
#[tokio::test]
#[ignore = "requires authenticated MQTT_BROKER_URL rejecting unknown users"]
async fn mqtt_rejects_invalid_credentials() {
    use gateway_iot::server::transports::mqtt::MqttTransport;
    use json_rpc_2_iot::{MqttConfig, client::transports::mqtt::MqttClientTransport};
    let broker_url = invalid_credentials(
        &std::env::var("MQTT_BROKER_URL").expect("authenticated MQTT_BROKER_URL"),
    );
    let config = MqttConfig {
        broker_url: broker_url.clone(),
        operation_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let error = MqttClientTransport::connect_with_config(config.clone())
        .await
        .err()
        .expect("broker must reject credentials");
    assert!(!error.to_string().contains("invalid-password"));
    assert!(
        MqttTransport::connect(gateway_iot::MqttConfig {
            broker_url,
            ..Default::default()
        })
        .await
        .is_err()
    );
}

#[cfg(feature = "amqp")]
#[tokio::test]
#[ignore = "requires authenticated AMQP_BROKER_URL rejecting unknown users"]
async fn amqp_rejects_invalid_credentials() {
    use gateway_iot::server::transports::amqp::AmqpTransport;
    use json_rpc_2_iot::{AmqpConfig, client::transports::amqp::AmqpClientTransport};
    let broker_url = invalid_credentials(
        &std::env::var("AMQP_BROKER_URL").expect("authenticated AMQP_BROKER_URL"),
    );
    let config = AmqpConfig {
        broker_url: broker_url.clone(),
        operation_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let error = AmqpClientTransport::connect_with_config(config.clone())
        .await
        .err()
        .expect("broker must reject credentials");
    assert!(!error.to_string().contains("invalid-password"));
    assert!(
        AmqpTransport::connect(gateway_iot::AmqpConfig {
            broker_url,
            ..Default::default()
        })
        .await
        .is_err()
    );
}

#[cfg(feature = "mqtt")]
#[tokio::test]
async fn mqtt_does_not_ignore_tls_settings_on_plain_transport() {
    use json_rpc_2_iot::{MqttConfig, client::transports::mqtt::MqttClientTransport};
    let error = MqttClientTransport::connect_with_config(MqttConfig {
        ca_certificate: Some(b"invalid CA".to_vec()),
        ..Default::default()
    })
    .await
    .err()
    .expect("TLS settings on plaintext must fail");
    assert!(error.to_string().contains("requires mqtts://"));
}
