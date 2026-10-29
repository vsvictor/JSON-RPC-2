use super::*;

#[test]
fn mqtt_publish_roundtrip() {
    let packet = MqttPacket::request(7, br#"{"jsonrpc":"2.0"}"#.to_vec());
    let encoded = encode_mqtt_packet(&packet).unwrap();
    assert_eq!(decode_mqtt_packet(&encoded).unwrap(), packet);
}
