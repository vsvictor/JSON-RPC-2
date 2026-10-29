use super::*;

fn options() -> ConnectOptions {
    ConnectOptions {
        host: "localhost".into(),
        port: 1883,
        ca_pem: None,
        server_name: None,
        srt_passphrase: None,
        udplite_send_coverage: None,
        udplite_recv_coverage: None,
        broker_url: None,
        broker_route: None,
        broker_ca_pem: None,
        broker_operation_timeout_ms: None,
        broker_response_timeout_ms: None,
    }
}

#[cfg(feature = "quic")]
#[test]
fn certificate_setter_validates_and_replaces_trust_before_connecting() {
    let first = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .unwrap()
        .cert
        .pem();
    let second = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .unwrap()
        .cert
        .pem();
    let builder = RpcClientBuilder::new(FfiTransportKind::Quic, options());
    builder.set_ca_certificate_pem(first.clone()).unwrap();
    assert_eq!(
        builder.options.lock().unwrap().ca_pem.as_ref(),
        Some(&first)
    );
    for invalid in ["", "not PEM"] {
        assert!(matches!(
            builder.set_ca_certificate_pem(invalid.into()),
            Err(FfiError::InvalidConfiguration { .. })
        ));
        assert_eq!(
            builder.options.lock().unwrap().ca_pem.as_ref(),
            Some(&first)
        );
    }
    builder.set_ca_certificate_pem(second.clone()).unwrap();
    assert_eq!(
        builder.options.lock().unwrap().ca_pem.as_ref(),
        Some(&second)
    );
    runtime().block_on(async {
        let endpoint = quic_endpoint(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            QUIC_ALPN,
            builder.options.lock().unwrap().ca_pem.as_deref().unwrap(),
        )
        .unwrap();
        endpoint.close(0u32.into(), b"done");
    });
}

#[cfg(feature = "tcp")]
#[test]
fn broker_options_are_not_silently_ignored_for_socket_transports() {
    let mut options = options();
    options.broker_url = Some("mqtt://broker".into());
    assert!(matches!(
        validate_broker_options(FfiTransportKind::Tcp, &options),
        Err(FfiError::InvalidConfiguration { .. })
    ));
}

#[cfg(feature = "mqtt")]
#[test]
fn mqtt_configuration_uses_external_url_route_timeouts_and_ca() {
    let mut options = options();
    options.broker_url = Some("mqtts://user:password@broker.example:8883".into());
    options.host.clear();
    options.port = 0;
    options.broker_route = Some("service/rpc".into());
    options.broker_ca_pem = Some("PEM CA".into());
    options.broker_operation_timeout_ms = Some(1200);
    options.broker_response_timeout_ms = Some(3400);

    let config = mqtt_config(&options, false).expect("valid MQTT configuration");
    assert_eq!(
        config.broker_url,
        "mqtts://user:password@broker.example:8883"
    );
    assert_eq!(config.request_topic, "service/rpc");
    assert_eq!(config.ca_certificate.as_deref(), Some(b"PEM CA".as_slice()));
    assert_eq!(config.operation_timeout, Duration::from_millis(1200));
    assert_eq!(config.response_timeout, Duration::from_millis(3400));
}

#[cfg(feature = "mqtt")]
#[test]
fn mqtt_configuration_has_websocket_route_and_rejects_bad_urls() {
    let mut options = options();
    options.broker_url = Some("http://broker".into());
    let error = match mqtt_config(&options, true) {
        Err(error) => error,
        Ok(_) => panic!("unsupported URL scheme was accepted"),
    };
    assert!(matches!(error, FfiError::InvalidConfiguration { .. }));

    options.broker_url = None;
    let config = mqtt_config(&options, true).expect("defaults are valid");
    assert_eq!(config.request_topic, "json-rpc-2/ws/requests");
    assert_eq!(config.broker_url, "mqtt://localhost:1883");

    options.broker_ca_pem = Some("PEM CA".into());
    assert!(matches!(
        mqtt_config(&options, false),
        Err(FfiError::InvalidConfiguration { .. })
    ));

    options.broker_ca_pem = None;
    options.broker_url = None;
    options.broker_route = Some("rpc/+".into());
    assert!(matches!(
        mqtt_config(&options, false),
        Err(FfiError::InvalidConfiguration { .. })
    ));
}

#[cfg(feature = "mqtt")]
#[test]
fn mqtt_connect_dispatch_validates_broker_config_without_resolving_legacy_host() {
    let mut options = options();
    options.host = "unresolvable.invalid".into();
    options.broker_url = Some("http://broker".into());
    let error = match RpcClient::connect(FfiTransportKind::Mqtt, options) {
        Err(error) => error,
        Ok(_) => panic!("invalid MQTT URL was accepted"),
    };
    assert!(matches!(error, FfiError::InvalidConfiguration { .. }));
}

#[cfg(feature = "amqp")]
#[test]
fn amqp_configuration_uses_url_queue_and_websocket_default() {
    let mut options = options();
    options.broker_url = Some("amqps://user:password@broker.example/vhost".into());
    options.host.clear();
    options.port = 0;
    options.broker_route = Some("service.queue".into());
    options.broker_operation_timeout_ms = Some(1500);
    let config = amqp_config(&options, false).expect("valid AMQP configuration");
    assert_eq!(
        config.broker_url,
        "amqps://user:password@broker.example/vhost"
    );
    assert_eq!(config.request_queue, "service.queue");
    assert_eq!(config.operation_timeout, Duration::from_millis(1500));

    options.broker_url = None;
    options.host = "localhost".into();
    options.port = 5672;
    options.broker_route = None;
    let config = amqp_config(&options, true).expect("default AMQP config");
    assert_eq!(config.request_queue, "json-rpc-2.ws.requests");
    options.broker_route = Some("amq.reserved".into());
    assert!(matches!(
        amqp_config(&options, false),
        Err(FfiError::InvalidConfiguration { .. })
    ));
}

#[cfg(feature = "amqp")]
#[test]
fn amqp_configuration_rejects_zero_timeout() {
    let mut options = options();
    options.broker_operation_timeout_ms = Some(0);
    assert!(matches!(
        amqp_config(&options, false),
        Err(FfiError::InvalidConfiguration { .. })
    ));

    options.broker_operation_timeout_ms = None;
    options.broker_ca_pem = Some("PEM CA".into());
    assert!(matches!(
        amqp_config(&options, false),
        Err(FfiError::InvalidConfiguration { .. })
    ));
}

#[cfg(feature = "srt")]
#[test]
fn srt_connect_dispatch_rejects_native_invalid_passphrase_length() {
    let mut options = options();
    options.srt_passphrase = Some("short".into());
    let error = match RpcClient::connect(FfiTransportKind::Srt, options) {
        Err(error) => error,
        Ok(_) => panic!("short SRT passphrase was accepted"),
    };
    assert!(matches!(error, FfiError::InvalidConfiguration { .. }));
}
