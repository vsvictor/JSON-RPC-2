#![cfg(all(feature = "websocket", any(feature = "srt", feature = "sctp")))]

use std::{future::Future, sync::Arc, time::Duration};

use bytes::Bytes;
use json_rpc_2_core::{
    WebSocketFuture, WebSocketMessage, decode_websocket_message, encode_websocket_close_frame,
    encode_websocket_message,
};
use json_rpc_2_transport::{RpcClientTransport, TransportContext};
use json_rpc_2_websockets::server::transports::{WebSocketProtocol, WebSocketTransport};
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(5);

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("bounded message-oriented WebSocket operation")
}

struct Probe {
    opened: mpsc::UnboundedSender<TransportContext>,
    handled: mpsc::UnboundedSender<TransportContext>,
    closed: mpsc::UnboundedSender<TransportContext>,
}

impl WebSocketProtocol for Probe {
    fn handle_message<'a>(&'a self, _: WebSocketMessage) -> WebSocketFuture<'a> {
        panic!("persistent WebSocket messages must use their session context")
    }

    fn handle_message_with_context<'a>(
        &'a self,
        message: WebSocketMessage,
        context: &'a TransportContext,
    ) -> WebSocketFuture<'a> {
        Box::pin(async move {
            self.handled.send(context.clone()).unwrap();
            Some(message)
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        self.opened.send(context.clone()).unwrap();
    }

    fn session_closed(&self, context: &TransportContext) {
        let _ = self.closed.send(context.clone());
    }
}

struct Events {
    opened: mpsc::UnboundedReceiver<TransportContext>,
    handled: mpsc::UnboundedReceiver<TransportContext>,
    closed: mpsc::UnboundedReceiver<TransportContext>,
}

fn probe() -> (Arc<Probe>, Events) {
    let (opened, opens) = mpsc::unbounded_channel();
    let (handled, messages) = mpsc::unbounded_channel();
    let (closed, closes) = mpsc::unbounded_channel();
    (
        Arc::new(Probe {
            opened,
            handled,
            closed,
        }),
        Events {
            opened: opens,
            handled: messages,
            closed: closes,
        },
    )
}

impl Events {
    async fn opened(&mut self, kind: &str) -> TransportContext {
        let context = bounded(self.opened.recv()).await.unwrap();
        assert_eq!(context.transport, kind);
        assert_eq!(context.connection_id.len(), 36);
        assert_ne!(
            context.connection_id,
            "00000000-0000-0000-0000-000000000000"
        );
        assert!(context.push_sender.is_some());
        context
    }

    async fn handled(&mut self, context: &TransportContext) {
        let handled = bounded(self.handled.recv()).await.unwrap();
        assert_eq!(handled.connection_id, context.connection_id);
        assert_eq!(handled.transport, context.transport);
        assert!(self.opened.try_recv().is_err(), "one open per session");
    }

    async fn closed(&mut self, context: &TransportContext) {
        let closed = bounded(self.closed.recv()).await.unwrap();
        assert_eq!(closed.connection_id, context.connection_id);
        assert_eq!(closed.transport, context.transport);
        assert!(self.closed.try_recv().is_err(), "one close per session");
        bounded(async {
            while !context.push_sender.as_ref().unwrap().is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            context
                .push_sender
                .as_ref()
                .unwrap()
                .send(Bytes::from_static(b"after close"))
                .is_err()
        );
    }
}

fn opaque_messages() -> [WebSocketMessage; 3] {
    [
        WebSocketMessage::Text("opaque text, not JSON".into()),
        WebSocketMessage::Binary(Bytes::from_static(b"\0\xff\nopaque binary\r\n")),
        WebSocketMessage::Binary(gateway_payload()),
    ]
}

fn gateway_payload() -> Bytes {
    // Valid UTF-8 on purpose: gateway frames must still use Binary, not Text.
    json_rpc_2_transport::gateway::encode_request(
        &Bytes::from_static(b"opaque ASCII payload"),
        "test-connection",
        "ws-srt",
    )
    .unwrap()
    .into_bytes()
}

#[cfg(feature = "srt")]
mod srt {
    use super::*;
    use json_rpc_2_core::SrtConfig;
    use json_rpc_2_datagram::client::transports::srt::SrtClientTransport;
    use json_rpc_2_websockets::{
        client::transports::srt::SrtWebSocketClientTransport,
        server::transports::srt::SrtWebSocketTransport,
    };

    const PASSWORD: &str = "message-session-test-secret";

    async fn server(
        protocol: Arc<Probe>,
    ) -> (std::net::SocketAddr, json_rpc_2_transport::RunningTransport) {
        let listener = bounded(SrtWebSocketTransport::bind(
            "127.0.0.1:0".parse().unwrap(),
            SrtConfig {
                passphrase: PASSWORD.into(),
            },
        ))
        .await
        .unwrap();
        let address = listener.local_addr().unwrap();
        (address, listener.start(protocol))
    }

    #[tokio::test]
    async fn encrypted_srt_websocket_stable_context_opaque_response_push_and_reconnect() {
        let (protocol, mut events) = probe();
        let (address, server) = server(protocol).await;
        let client = bounded(SrtWebSocketClientTransport::connect(address, PASSWORD))
            .await
            .unwrap();
        let context = events.opened("ws-srt").await;
        for payload in [
            Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#),
            gateway_payload(),
        ] {
            assert_eq!(
                bounded(client.send_with_expectation(payload.clone(), true))
                    .await
                    .unwrap(),
                Some(payload)
            );
            events.handled(&context).await;
        }
        let mut pushed = Vec::new();
        for message in opaque_messages() {
            let payload = match &message {
                WebSocketMessage::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
                WebSocketMessage::Binary(payload) => payload.clone(),
            };
            context.push_sender.as_ref().unwrap().send(payload).unwrap();
            pushed.push((bounded(client.recv_push()).await.unwrap(), message));
        }
        bounded(client.transport_ping()).await.unwrap();
        bounded(client.reconnect()).await.unwrap();
        events.closed(&context).await;
        let next = events.opened("ws-srt").await;
        assert_ne!(next.connection_id, context.connection_id);
        let payload = gateway_payload();
        assert_eq!(
            bounded(client.send_with_expectation(payload.clone(), true))
                .await
                .unwrap(),
            Some(payload)
        );
        events.handled(&next).await;
        bounded(client.close()).await.unwrap();
        events.closed(&next).await;
        bounded(server.close()).await.unwrap();
        assert!(events.closed.try_recv().is_err());
        for (received, expected) in pushed {
            assert_eq!(received, expected);
        }
    }

    #[tokio::test]
    async fn encrypted_srt_websocket_explicit_close_cleans_up_without_client_socket_close() {
        let (protocol, mut events) = probe();
        let (address, server) = server(protocol).await;
        // Use the raw carrier so Close is an application frame, not client.close().
        let client = bounded(SrtClientTransport::connect(address, PASSWORD))
            .await
            .unwrap();
        let context = events.opened("ws-srt").await;
        for message in opaque_messages() {
            let frame = Bytes::from(encode_websocket_message(&message).unwrap());
            let response = bounded(client.send_with_expectation(frame, true))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(decode_websocket_message(&response).unwrap(), Some(message));
            events.handled(&context).await;
        }
        assert_eq!(
            bounded(client.send_with_expectation(
                Bytes::from(encode_websocket_close_frame().unwrap()),
                false,
            ))
            .await
            .unwrap(),
            None
        );
        events.closed(&context).await;
        assert!(
            events.handled.try_recv().is_err(),
            "Close is not a business message"
        );
        bounded(client.close()).await.unwrap();
        bounded(server.close()).await.unwrap();
        assert!(events.closed.try_recv().is_err());
    }
}

#[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
mod sctp {
    use super::*;
    use json_rpc_2_websockets::server::transports::sctp::SctpWebSocketTransport;
    use std::{
        io,
        net::SocketAddrV4,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    async fn connect(address: std::net::SocketAddr) -> TcpStream {
        #[repr(C)]
        struct SockAddrIn {
            family: u16,
            port: u16,
            address: u32,
            padding: [u8; 8],
        }
        unsafe extern "C" {
            fn socket(domain: i32, kind: i32, protocol: i32) -> i32;
            #[link_name = "connect"]
            fn connect_socket(fd: i32, address: *const SockAddrIn, length: u32) -> i32;
        }
        let std::net::SocketAddr::V4(address) = address else {
            panic!("IPv4 test listener")
        };
        // Linux AF_INET, SOCK_STREAM | SOCK_CLOEXEC and IPPROTO_SCTP.
        let fd = unsafe { socket(2, 1 | 0x80000, 132) };
        assert!(fd >= 0, "SCTP socket: {}", io::Error::last_os_error());
        // SAFETY: socket returned a new descriptor; OwnedFd becomes its sole owner.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let stream = std::net::TcpStream::from(fd);
        stream.set_nonblocking(true).unwrap();
        let sockaddr = sockaddr(address);
        fn sockaddr(address: SocketAddrV4) -> SockAddrIn {
            SockAddrIn {
                family: 2,
                port: address.port().to_be(),
                address: u32::from_ne_bytes(address.ip().octets()),
                padding: [0; 8],
            }
        }
        // SAFETY: sockaddr is a valid initialized IPv4 sockaddr for the duration of connect.
        let result = unsafe {
            connect_socket(
                stream.as_raw_fd(),
                &sockaddr,
                std::mem::size_of::<SockAddrIn>() as u32,
            )
        };
        if result < 0 {
            assert_eq!(
                io::Error::last_os_error().raw_os_error(),
                Some(115),
                "nonblocking connect"
            );
        }
        let stream = TcpStream::from_std(stream).unwrap();
        bounded(stream.writable()).await.unwrap();
        assert!(stream.take_error().unwrap().is_none());
        stream
    }

    async fn send(stream: &mut TcpStream, frame: &[u8]) {
        assert_eq!(
            bounded(stream.write(frame)).await.unwrap(),
            frame.len(),
            "one SCTP record"
        );
    }

    async fn receive(stream: &mut TcpStream) -> WebSocketMessage {
        let mut buffer = [0; 4096];
        let read = bounded(stream.read(&mut buffer)).await.unwrap();
        assert!(read > 0, "unexpected SCTP EOF");
        decode_websocket_message(&buffer[..read]).unwrap().unwrap()
    }

    #[tokio::test]
    #[ignore = "requires kernel SCTP support"]
    async fn sctp_websocket_stable_context_binary_push_explicit_close_and_eof() {
        let (protocol, mut events) = probe();
        let listener = SctpWebSocketTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr();
        let server = listener.start(protocol);
        let mut stream = connect(address).await;
        let context = events.opened("ws-sctp").await;
        for message in opaque_messages() {
            send(&mut stream, &encode_websocket_message(&message).unwrap()).await;
            assert_eq!(receive(&mut stream).await, message);
            events.handled(&context).await;
        }
        let mut pushed = Vec::new();
        for message in opaque_messages() {
            let payload = match &message {
                WebSocketMessage::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
                WebSocketMessage::Binary(payload) => payload.clone(),
            };
            context.push_sender.as_ref().unwrap().send(payload).unwrap();
            pushed.push((receive(&mut stream).await, message));
        }
        send(&mut stream, &encode_websocket_close_frame().unwrap()).await;
        events.closed(&context).await;
        assert!(
            events.handled.try_recv().is_err(),
            "Close is not a business message"
        );
        // The client descriptor remains open until after lifecycle cleanup.
        let mut byte = [0];
        assert_eq!(bounded(stream.read(&mut byte)).await.unwrap(), 0);
        drop(stream);
        let stream = connect(address).await;
        let next = events.opened("ws-sctp").await;
        assert_ne!(next.connection_id, context.connection_id);
        drop(stream);
        events.closed(&next).await;
        bounded(server.close()).await.unwrap();
        assert!(events.closed.try_recv().is_err());
        for (received, expected) in pushed {
            assert_eq!(received, expected);
        }
    }
}
