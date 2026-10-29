use super::*;

#[test]
fn push_preserves_text_binary_and_application_frames() {
    assert_eq!(
        push_message(Bytes::from_static(b"notification")),
        WebSocketMessage::Text("notification".to_owned())
    );
    for payload in [Bytes::from_static(b"\xff\0binary")] {
        assert_eq!(
            push_message(payload.clone()),
            WebSocketMessage::Binary(payload)
        );
    }
}

struct Reply {
    request: Bytes,
    response: Bytes,
}

impl JsonRpcProtocol for Reply {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
        Box::pin(async move {
            assert_eq!(input, self.request.as_ref());
            Some(self.response.clone())
        })
    }
}

#[tokio::test]
async fn protocol_preserves_binary_and_text_response_kinds() {
    for (request, response, binary) in [
        (
            Bytes::from_static(b"APP\xff\0\xff\nrequest"),
            Bytes::from_static(b"APP\xff\0\xfe\nresponse"),
            true,
        ),
        (
            Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#),
            Bytes::from_static(br#"{"jsonrpc":"2.0","result":"ok","id":1}"#),
            false,
        ),
    ] {
        let protocol = JsonRpcWebSocket::new(Arc::new(Reply {
            request: request.clone(),
            response: response.clone(),
        }));
        let wrap = |bytes: Bytes| {
            if binary {
                WebSocketMessage::Binary(bytes)
            } else {
                WebSocketMessage::Text(std::str::from_utf8(&bytes).unwrap().to_owned())
            }
        };
        assert_eq!(
            protocol.handle_message(wrap(request)).await,
            Some(wrap(response))
        );
    }
}

struct OwnedReply {
    request: Bytes,
    kind: &'static str,
}

impl JsonRpcProtocol for OwnedReply {
    fn handle_message<'a>(&'a self, _: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
        panic!("WebSocket requests must use the owned message contract")
    }

    fn handle_owned_message<'a>(
        &'a self,
        input: Bytes,
        context: &'a TransportContext,
    ) -> json_rpc_2_transport::ProtocolMessageFuture<'a> {
        Box::pin(async move {
            assert_eq!(input.as_ptr(), self.request.as_ptr());
            assert_eq!(input, self.request);
            assert_eq!(context.transport, self.kind);
            Some(json_rpc_2_transport::TransportMessage {
                header: Bytes::from_static(b"APP\xff"),
                payload: Bytes::from_static(b"\xff\nresponse"),
                binary: true,
            })
        })
    }
}

#[tokio::test]
async fn protocol_transfers_owned_binary_and_coalesces_response_at_boundary() {
    for kind in ["ws-tcp", "ws-udp"] {
        let request = Bytes::from_static(b"APP\xff\xfe\nrequest");
        let protocol = JsonRpcWebSocket::new(Arc::new(OwnedReply {
            request: request.clone(),
            kind,
        }));
        let response = if kind == "ws-tcp" {
            let session = TransportSession::new(kind, true);
            protocol
                .handle_message_with_context(WebSocketMessage::Binary(request), &session.context)
                .await
        } else {
            handle_stateless_message(&protocol, WebSocketMessage::Binary(request), kind).await
        };
        assert_eq!(
            response,
            Some(WebSocketMessage::Binary(Bytes::from_static(
                b"APP\xff\xff\nresponse"
            )))
        );
    }
}

struct RejectControl;

impl WebSocketProtocol for RejectControl {
    fn handle_message<'a>(&'a self, _: WebSocketMessage) -> WebSocketFuture<'a> {
        panic!("WebSocket control frames must not reach the application protocol")
    }
}

#[tokio::test]
async fn stateless_control_ping_echoes_pong_without_calling_protocol() {
    let payload = Bytes::from_static(b"health-check");
    assert_eq!(
        handle_stateless_message(
            &RejectControl,
            WebSocketMessage::Ping(payload.clone()),
            "ws-udp",
        )
        .await,
        Some(WebSocketMessage::Pong(payload.clone()))
    );
    assert_eq!(
        handle_stateless_message(&RejectControl, WebSocketMessage::Pong(payload), "ws-udp",).await,
        None
    );
}

struct RejectRpc;

impl JsonRpcProtocol for RejectRpc {
    fn handle_message<'a>(&'a self, _: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
        panic!("WebSocket control frames must not reach the JSON-RPC protocol")
    }
}

#[tokio::test]
async fn session_adapter_echoes_control_ping_without_calling_protocol() {
    let protocol = JsonRpcWebSocket::new(Arc::new(RejectRpc));
    let payload = Bytes::from_static(b"session-ping");
    assert_eq!(
        protocol
            .handle_message(WebSocketMessage::Ping(payload.clone()))
            .await,
        Some(WebSocketMessage::Pong(payload.clone()))
    );
    assert_eq!(
        protocol
            .handle_message(WebSocketMessage::Pong(payload))
            .await,
        None
    );
}

#[tokio::test]
async fn session_adapter_marks_close_control_frame_for_cleanup() {
    let protocol = WebSocketJsonRpcAdapter {
        protocol: Arc::new(RejectControl),
        kind: "ws-sctp",
        sessions: std::sync::Mutex::new(std::collections::HashMap::new()),
    };
    let session = TransportSession::new("ws-sctp", true);
    protocol.session_opened(&session.context);
    let close = json_rpc_2_core::encode_websocket_close_frame().unwrap();
    assert!(
        protocol
            .handle_message_with_context(&close, &session.context)
            .await
            .is_none()
    );
    assert!(protocol.session_close_requested(&session.context));
    protocol.session_closed(&session.context);
}
