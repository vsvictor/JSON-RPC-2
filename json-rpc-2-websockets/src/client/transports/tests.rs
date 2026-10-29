use super::*;
use tokio_tungstenite::tungstenite::protocol::Role;

fn opaque_message(payload: &[u8]) -> Bytes {
    let mut message = b"APP\xff".to_vec();
    message.extend_from_slice(&[0; 17]);
    message.extend_from_slice(payload);
    Bytes::from(message)
}

struct FramedPeer {
    request: WebSocketMessage,
    response: WebSocketMessage,
}

impl RpcClientTransport for FramedPeer {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            assert_eq!(
                decode_websocket_message(&message).unwrap(),
                Some(self.request.clone())
            );
            Ok(Some(Bytes::from(
                encode_websocket_message(&self.response).unwrap(),
            )))
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn wrapped_transports_preserve_binary_messages_and_direct_text() {
    for (request, response, binary) in [
        (
            opaque_message(b"\xffrequest\n\0"),
            opaque_message(b"\xferesponse\n\n"),
            true,
        ),
        (
            Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#),
            Bytes::from_static(br#"{"jsonrpc":"2.0","result":"ok","id":1}"#),
            false,
        ),
    ] {
        let wrap = |bytes: Bytes| {
            if binary {
                WebSocketMessage::Binary(bytes)
            } else {
                WebSocketMessage::Text(std::str::from_utf8(&bytes).unwrap().to_owned())
            }
        };
        let transport = DatagramWebSocketClientTransport::new(
            FramedPeer {
                request: wrap(request.clone()),
                response: wrap(response.clone()),
            },
            "test",
            "closed",
        );
        assert_eq!(
            transport
                .send_with_expectation(request.clone(), true)
                .await
                .unwrap(),
            Some(response.clone())
        );
        assert_eq!(
            transport
                .send_cancelable(request, true, tokio_util::sync::CancellationToken::new(),)
                .await
                .unwrap(),
            Some(response)
        );
    }
}

#[tokio::test]
async fn datagram_transport_ping_uses_wsu2_control_frames() {
    let payload = Bytes::from_static(PING_PAYLOAD);
    let transport = DatagramWebSocketClientTransport::new(
        FramedPeer {
            request: WebSocketMessage::Ping(payload.clone()),
            response: WebSocketMessage::Pong(payload),
        },
        "ws-test",
        "closed",
    );
    transport.transport_ping().await.unwrap();
}

#[tokio::test]
async fn native_websockets_preserve_binary_messages_and_direct_text() {
    for (request, response, binary) in [
        (
            opaque_message(b"\xffrequest\n\0"),
            opaque_message(b"\xferesponse\n\n"),
            true,
        ),
        (
            Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#),
            Bytes::from_static(br#"{"jsonrpc":"2.0","result":"ok","id":1}"#),
            false,
        ),
    ] {
        let (client, server) = tokio::io::duplex(4096);
        let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let expected_request = request.clone();
        let expected_response = response.clone();
        let peer = async move {
            let inbound = server.next().await.unwrap().unwrap();
            assert_eq!(inbound.is_binary(), binary);
            assert_eq!(inbound.into_data(), expected_request);
            let outbound = if binary {
                Message::Binary(expected_response)
            } else {
                Message::Text(
                    std::str::from_utf8(&expected_response)
                        .unwrap()
                        .to_owned()
                        .into(),
                )
            };
            server.send(outbound).await.unwrap();
        };
        let (result, ()) = tokio::join!(exchange(&mut client, request.into(), true), peer);
        assert_eq!(result.unwrap(), Some(response));
    }
}

#[test]
fn non_utf8_application_payload_uses_binary_without_application_detection() {
    let payload = Bytes::from_static(b"\xffnot-a-gateway-frame");
    let frame =
        DatagramWebSocketClientTransport::<FramedPeer>::encode_outbound(payload.clone()).unwrap();
    assert_eq!(
        decode_websocket_message(&frame).unwrap(),
        Some(WebSocketMessage::Binary(payload)),
    );
}
