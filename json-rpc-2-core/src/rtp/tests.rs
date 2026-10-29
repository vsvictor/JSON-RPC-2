use super::*;

#[test]
fn rtp_roundtrip_preserves_header_and_payload() {
    let packet = RtpPacket {
        sequence_number: 7,
        timestamp: 42,
        ssrc: 99,
        kind: RtpPacketKind::Request,
        message_id: 123,
        payload: br#"{"jsonrpc":"2.0"}"#.to_vec(),
    };
    let encoded = encode_rtp_packet(&packet).unwrap();
    assert_eq!(decode_rtp_packet(&encoded).unwrap(), packet);
}

#[test]
fn rtcp_roundtrip_preserves_control_fields() {
    let packet = RtcpControlPacket {
        kind: RtcpControlPacketKind::Ping,
        ssrc: 11,
        message_id: 88,
    };
    let encoded = encode_rtcp_control_packet(&packet).unwrap();
    assert_eq!(decode_rtcp_control_packet(&encoded).unwrap(), packet);
}
