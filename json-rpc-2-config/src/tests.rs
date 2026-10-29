use super::*;

#[test]
fn broker_urls_require_supported_schemes_and_valid_authority() {
    assert!(validate_broker_url("mqtt://localhost:1883", &["mqtt://", "mqtts://"], "MQTT").is_ok());
    assert!(
        validate_broker_url(
            "mqtts://user:secret@broker",
            &["mqtt://", "mqtts://"],
            "MQTT"
        )
        .is_ok()
    );
    assert!(validate_broker_url("http://broker", &["mqtt://", "mqtts://"], "MQTT").is_err());
    assert!(validate_broker_url("mqtt://", &["mqtt://", "mqtts://"], "MQTT").is_err());
}

#[test]
fn broker_routes_reject_empty_and_control_characters() {
    assert!(validate_service_route("rpc/requests", "ROUTE").is_ok());
    assert!(validate_service_route("  ", "ROUTE").is_err());
    assert!(validate_service_route("rpc\nrequests", "ROUTE").is_err());
    assert!(validate_mqtt_topic("rpc/+", "TOPIC").is_err());
    assert!(validate_mqtt_topic("rpc/#", "TOPIC").is_err());
    assert!(validate_amqp_queue("amq.reserved", "QUEUE").is_err());
    assert!(validate_amqp_queue("rpc.requests", "QUEUE").is_ok());
}

#[test]
fn srt_passphrase_uses_native_byte_length_limits() {
    assert!(validate_srt_passphrase("1234567890").is_ok());
    assert!(validate_srt_passphrase(&"x".repeat(79)).is_ok());
    assert!(validate_srt_passphrase("123456789").is_err());
    assert!(validate_srt_passphrase(&"x".repeat(80)).is_err());
    assert!(validate_srt_passphrase(&"é".repeat(5)).is_ok());
}
