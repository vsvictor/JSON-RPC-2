use std::sync::Arc;

use super::{GatewayBackendProtocol, GatewayFraming};
use bytes::{BufMut, Bytes, BytesMut};
use json_rpc_2_transport::{
    FrameBoundary, JsonRpcProtocol, ProtocolFuture, StreamFraming, TransportContext,
};

const PING: &[u8] = b"JRPC-GW\x03\x05\0\0\0\0\0\0\0\0";
const PONG: &[u8] = b"JRPC-GW\x03\x06\0\0\0\0\0\0\0\0";
const NONE: &[u8] = b"JRPC-GW\x03\x03\0\0\0\0\0\0\0\0";
const ERROR: &[u8] = b"JRPC-GW\x03\x04\0\0\0\0\0\0\0\0";

fn request(payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    frame.extend_from_slice(b"JRPC-GW\x03\x01\0\x08\0\x06");
    frame.put_u32(payload.len() as u32);
    frame.extend_from_slice(b"client-1ws-tcp");
    frame.extend_from_slice(payload);
    frame.freeze()
}

struct ContextEcho;
impl JsonRpcProtocol for ContextEcho {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move { (!input.is_empty()).then(|| Bytes::copy_from_slice(input)) })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        assert_eq!(context.connection_id, "client-1");
        assert_eq!(context.transport, "ws-tcp");
        assert!(context.push_sender.is_none());
        self.handle_message(input)
    }
}

#[tokio::test]
async fn wire_contract_preserves_payload_context_notifications_and_health() {
    let protocol = GatewayBackendProtocol::new(Arc::new(ContextEcho));
    assert_eq!(protocol.handle_message(PING).await.unwrap(), PONG);
    assert_eq!(protocol.handle_message(&request(b"")).await.unwrap(), NONE);
    let response = protocol
        .handle_message(&request(b"\0\xff\nopaque"))
        .await
        .unwrap();
    assert_eq!(&response[..13], b"JRPC-GW\x03\x02\0\0\0\0");
    assert_eq!(u32::from_be_bytes(response[13..17].try_into().unwrap()), 9);
    assert_eq!(&response[17..], b"\0\xff\nopaque");
    assert_eq!(
        protocol.handle_message(b"direct JSON").await.unwrap(),
        b"direct JSON".as_slice()
    );
}

#[tokio::test]
async fn rejects_bad_frames_without_dispatching_business_methods() {
    let protocol = GatewayBackendProtocol::new(Arc::new(ContextEcho));
    let valid = request(b"x");
    let mut invalid = vec![
        valid[..valid.len() - 1].to_vec(),
        PONG.to_vec(),
        b"JRPC-GW/2".to_vec(),
    ];
    for (index, value) in [(8, 99), (9, 255), (13, 255), (17, b'\n')] {
        let mut frame = valid.to_vec();
        frame[index] = value;
        invalid.push(frame);
    }
    let mut trailing = PING.to_vec();
    trailing.push(0);
    invalid.push(trailing);
    for input in invalid {
        assert_eq!(protocol.handle_message(&input).await.unwrap(), ERROR);
    }
}

#[test]
fn stream_codec_handles_fragmentation_coalescing_and_limits() {
    let frame = request(b"line1\nline2");
    for length in 0..17 {
        assert!(matches!(
            GatewayFraming.boundary(&frame[..length]).unwrap(),
            FrameBoundary::Need(17)
        ));
    }

    assert!(
        matches!(GatewayFraming.boundary(&frame).unwrap(), FrameBoundary::Frame(size) if size == frame.len())
    );
    let mut two = frame.to_vec();
    two.extend_from_slice(PING);
    assert!(
        matches!(GatewayFraming.boundary(&two).unwrap(), FrameBoundary::Frame(size) if size == frame.len())
    );
    assert!(matches!(
        GatewayFraming.boundary(b"{").unwrap(),
        FrameBoundary::Line
    ));
    let mut oversized = PING.to_vec();
    oversized[8] = 2;
    oversized[13..17].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(GatewayFraming.boundary(&oversized).is_err());
}

#[tokio::test]
async fn transport_response_retains_payload_and_rejects_oversize() {
    struct Fixed(Bytes);
    impl JsonRpcProtocol for Fixed {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
            Box::pin(async move { Some(self.0.clone()) })
        }
    }
    let context = TransportContext {
        transport: "tcp".into(),
        connection_id: "physical-gateway-connection".into(),
        push_sender: None,
    };
    let payload = Bytes::from_static(b"response");
    let protocol = GatewayBackendProtocol::new(Arc::new(Fixed(payload.clone())));
    let response = protocol
        .handle_transport_message(&request(b"request"), &context)
        .await
        .unwrap();
    assert!(response.binary);
    assert_eq!(response.payload.as_ptr(), payload.as_ptr());
    let too_large = Bytes::from(vec![0; json_rpc_2_core::MAX_MESSAGE_BYTES - 17 - 256 + 1]);
    let protocol = GatewayBackendProtocol::new(Arc::new(Fixed(too_large)));
    assert_eq!(
        protocol.handle_message(&request(b"request")).await.unwrap(),
        ERROR
    );
}
