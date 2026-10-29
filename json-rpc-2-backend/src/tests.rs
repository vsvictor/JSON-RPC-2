use super::*;

fn values(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).into(), (*value).into()))
        .collect()
}

#[test]
fn settings_defaults_and_overrides() {
    let empty = HashMap::new();
    let defaults = BackendBuilder::from_settings(Settings::new(&empty)).unwrap();
    assert_eq!(defaults.tcp_port, BackendBuilder::default().tcp_port);
    let values = values(&[("BACKEND_PORT", "9000"), ("BACKEND_ID", "test-backend")]);
    let builder = BackendBuilder::from_settings(Settings::new(&values)).unwrap();
    assert_eq!(builder.tcp_port, 9000);
    assert_eq!(builder.udp_port, 9000);
    assert_eq!(builder.instance_id, "test-backend");
}

#[test]
fn backend_settings_reject_invalid_values_without_exposing_them() {
    for (key, value) in [
        ("BACKEND_BIND", "private-value"),
        ("BACKEND_PORT", "private-value"),
        ("BACKEND_PORT", "0"),
        ("BACKEND_PORT", "65536"),
        ("BACKEND_TCP_PORT", " "),
        ("BACKEND_ID", ""),
        ("BACKEND_TLS_KEY_FILE", " "),
        ("BACKEND_RTP_PORT", ""),
        ("BACKEND_MQTT_PORT", "1883"),
    ] {
        let values = values(&[(key, value)]);
        let error = BackendBuilder::from_settings(Settings::new(&values))
            .err()
            .expect("invalid settings must fail")
            .to_string();
        assert!(!error.contains("private-value"));
    }
}
