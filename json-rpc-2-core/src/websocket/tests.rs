use super::*;

#[test]
fn ping_and_pong_control_frames_round_trip() {
    let payload = b"alive".to_vec();
    for frame in [
        WebSocketFrame::ping(payload.clone()),
        WebSocketFrame::pong(payload.clone()),
    ] {
        let encoded = encode_websocket_frame(&frame).unwrap();
        assert_eq!(decode_websocket_frame(&encoded).unwrap(), frame);
        assert_eq!(
            websocket_message_bytes(&decode_websocket_message(&encoded).unwrap().unwrap()),
            payload
        );
    }
}

#[test]
fn control_frames_obey_websocket_payload_limits() {
    for frame in [
        WebSocketFrame::ping(vec![0; 126]),
        WebSocketFrame::pong(vec![0; 126]),
        WebSocketFrame {
            kind: WebSocketFrameKind::Close,
            payload: vec![0],
        },
    ] {
        assert!(encode_websocket_frame(&frame).is_err());
    }
}
