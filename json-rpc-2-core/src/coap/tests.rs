use super::*;

#[test]
fn coap_roundtrip() {
    let message = CoapMessage::request(9, vec![1, 2, 3, 4], b"hello".to_vec());
    let encoded = encode_coap_message(&message).unwrap();
    assert_eq!(decode_coap_message(&encoded).unwrap(), message);
}
