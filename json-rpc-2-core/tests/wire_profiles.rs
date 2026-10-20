//! Independent byte vectors, not encoder/decoder round trips.
//! These cover explicit wire subsets, not complete protocol conformance.

use json_rpc_2_core::{
    AmqpFrame, CoapMessage, MqttPacket, RtcpControlPacket, RtcpControlPacketKind, RtpPacket,
    RtpPacketKind, decode_amqp_frame, decode_coap_message, decode_mqtt_packet,
    decode_rtcp_control_packet, decode_rtp_packet, encode_amqp_frame, encode_coap_message,
    encode_mqtt_packet, encode_rtcp_control_packet, encode_rtp_packet,
};

#[test]
fn coap_post_with_token_and_payload_matches_rfc7252_header_layout() {
    let bytes = [0x42, 0x02, 0x12, 0x34, 0xaa, 0xbb, 0xff, b'{', b'}'];
    let message = CoapMessage::request(0x1234, vec![0xaa, 0xbb], b"{}".to_vec());
    assert_eq!(encode_coap_message(&message).unwrap(), bytes);
    assert_eq!(decode_coap_message(&bytes).unwrap(), message);
}

#[test]
fn coap_empty_confirmable_ping_matches_rfc7252_header_layout() {
    let bytes = [0x40, 0x00, 0x12, 0x34];
    assert_eq!(
        encode_coap_message(&CoapMessage::ping(0x1234)).unwrap(),
        bytes
    );
    assert_eq!(
        decode_coap_message(&bytes).unwrap(),
        CoapMessage::ping(0x1234)
    );
}

#[test]
fn mqtt_ping_control_packets_match_mqtt311() {
    assert_eq!(encode_mqtt_packet(&MqttPacket::ping()).unwrap(), [0xc0, 0]);
    assert_eq!(encode_mqtt_packet(&MqttPacket::pong()).unwrap(), [0xd0, 0]);
    assert_eq!(decode_mqtt_packet(&[0xc0, 0]).unwrap(), MqttPacket::ping());
    assert_eq!(decode_mqtt_packet(&[0xd0, 0]).unwrap(), MqttPacket::pong());
}

#[test]
fn mqtt_standard_connect_is_explicitly_unsupported() {
    // MQTT 3.1.1 CONNECT, clean session, client identifier "rpc".
    let connect = [
        0x10, 0x0f, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 3, b'r', b'p', b'c',
    ];
    assert!(
        decode_mqtt_packet(&connect)
            .unwrap_err()
            .contains("unsupported MQTT packet type")
    );
}

#[test]
fn amqp_heartbeat_matches_amqp091_frame_layout() {
    let bytes = [8, 0, 0, 0, 0, 0, 0, 0xce];
    assert_eq!(encode_amqp_frame(&AmqpFrame::heartbeat()).unwrap(), bytes);
    assert_eq!(decode_amqp_frame(&bytes).unwrap(), AmqpFrame::heartbeat());
}

#[test]
fn amqp_standard_method_frames_are_explicitly_unsupported() {
    // AMQP 0-9-1 connection.close-ok: method frame, channel zero.
    let method = [1, 0, 0, 0, 0, 0, 4, 0, 10, 0, 51, 0xce];
    assert!(
        decode_amqp_frame(&method)
            .unwrap_err()
            .contains("unsupported AMQP frame type")
    );
}

#[test]
fn rtp_header_and_private_payload_profile_match_fixed_vector() {
    let bytes = [
        0x80, 96, 0x12, 0x34, 0, 0, 0, 42, 0, 0, 0, 99, 1, 0, 0, 0, 0, 0, 0, 0, 7, b'{', b'}',
    ];
    let packet = RtpPacket {
        sequence_number: 0x1234,
        timestamp: 42,
        ssrc: 99,
        kind: RtpPacketKind::Request,
        message_id: 7,
        payload: b"{}".to_vec(),
    };
    assert_eq!(encode_rtp_packet(&packet).unwrap(), bytes);
    assert_eq!(decode_rtp_packet(&bytes).unwrap(), packet);
}

#[test]
fn rtcp_app_private_control_profile_matches_fixed_vector() {
    let bytes = [
        0x82, 204, 0, 4, 0, 0, 0, 99, b'J', b'R', b'P', b'2', 0, 0, 0, 0, 0, 0, 0, 7,
    ];
    let packet = RtcpControlPacket {
        kind: RtcpControlPacketKind::Ping,
        ssrc: 99,
        message_id: 7,
    };
    assert_eq!(encode_rtcp_control_packet(&packet).unwrap(), bytes);
    assert_eq!(decode_rtcp_control_packet(&bytes).unwrap(), packet);
}
