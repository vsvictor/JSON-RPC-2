use super::*;

#[test]
fn rudp_roundtrip_preserves_fields() {
    let packet = RudpPacket::request(7, 99, b"hello".to_vec());
    let encoded = encode_rudp_packet(&packet).unwrap();
    assert_eq!(decode_rudp_packet(&encoded).unwrap(), packet);
}
