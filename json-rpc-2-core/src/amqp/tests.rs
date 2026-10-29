use super::*;

#[test]
fn amqp_roundtrip() {
    let frame = AmqpFrame::request(15, b"hello".to_vec());
    let encoded = encode_amqp_frame(&frame).unwrap();
    assert_eq!(decode_amqp_frame(&encoded).unwrap(), frame);
}
