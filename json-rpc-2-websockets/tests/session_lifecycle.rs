#![cfg(all(
    feature = "websocket",
    feature = "http1",
    feature = "tls",
    feature = "quic",
    feature = "http3"
))]

use std::{future::Future, sync::Arc, time::Duration};

use bytes::{Buf, Bytes};
use futures_util::{SinkExt, StreamExt};
use json_rpc_2_transport::{RunningTransport, TransportContext, TransportRuntime};
use json_rpc_2_websockets::server::transports::{
    WebSocketFuture, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
    http1::{Http1WebSocketTransport, TlsWebSocketTransport},
    http3::Http3WebSocketTransport,
    quic::QuicWebSocketTransport,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};

const WAIT: Duration = Duration::from_secs(3);

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("bounded network operation")
}

struct Probe {
    opened: mpsc::UnboundedSender<TransportContext>,
    closed: mpsc::UnboundedSender<TransportContext>,
}

impl WebSocketProtocol for Probe {
    fn handle_message<'a>(&'a self, _: WebSocketMessage) -> WebSocketFuture<'a> {
        panic!("persistent WebSocket messages must carry their context")
    }

    fn handle_message_with_context<'a>(
        &'a self,
        message: WebSocketMessage,
        context: &'a TransportContext,
    ) -> WebSocketFuture<'a> {
        Box::pin(async move {
            Some(match message {
                WebSocketMessage::Text(text) => {
                    WebSocketMessage::Text(format!("{}:{text}", context.connection_id))
                }
                message => message,
            })
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        self.opened.send(context.clone()).unwrap();
    }

    fn session_closed(&self, context: &TransportContext) {
        let _ = self.closed.send(context.clone());
    }
}

fn probe() -> (
    Arc<Probe>,
    mpsc::UnboundedReceiver<TransportContext>,
    mpsc::UnboundedReceiver<TransportContext>,
) {
    let (opened, opens) = mpsc::unbounded_channel();
    let (closed, closes) = mpsc::unbounded_channel();
    (Arc::new(Probe { opened, closed }), opens, closes)
}

async fn exercise<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
    kind: &str,
    opened: &mut mpsc::UnboundedReceiver<TransportContext>,
    closed: &mut mpsc::UnboundedReceiver<TransportContext>,
    migration: Option<&quinn::Endpoint>,
) {
    // Raw QUIC only exposes a stream to its peer after the first frame is written.
    bounded(socket.send(Message::Text("first".into())))
        .await
        .unwrap();
    let context = bounded(opened.recv()).await.unwrap();
    assert_eq!(context.transport, kind);
    assert_eq!(context.connection_id.len(), 36);
    assert_ne!(
        context.connection_id,
        "00000000-0000-0000-0000-000000000000"
    );
    assert!(context.push_sender.is_some());
    assert_eq!(
        bounded(socket.next()).await.unwrap().unwrap(),
        Message::Text(format!("{}:first", context.connection_id).into())
    );

    if let Some(endpoint) = migration {
        let old = endpoint.local_addr().unwrap();
        endpoint
            .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
            .unwrap();
        assert_ne!(old, endpoint.local_addr().unwrap());
    }
    for text in ["second", "not business JSON"] {
        bounded(socket.send(Message::Text(text.into())))
            .await
            .unwrap();
        assert_eq!(
            bounded(socket.next()).await.unwrap().unwrap(),
            Message::Text(format!("{}:{text}", context.connection_id).into())
        );
    }
    for payload in [
        Bytes::from_static(b"\xff\0arbitrary binary"),
        Bytes::from_static(b"JRPC-GW\x03ASCII gateway frame"),
    ] {
        bounded(socket.send(Message::Binary(payload.clone())))
            .await
            .unwrap();
        assert_eq!(
            bounded(socket.next()).await.unwrap().unwrap(),
            Message::Binary(payload.clone())
        );
        context
            .push_sender
            .as_ref()
            .unwrap()
            .send(payload.clone())
            .unwrap();
        assert_eq!(
            bounded(socket.next()).await.unwrap().unwrap(),
            Message::Binary(payload)
        );
    }
    context
        .push_sender
        .as_ref()
        .unwrap()
        .send(Bytes::from_static(b"unsolicited"))
        .unwrap();
    assert_eq!(
        bounded(socket.next()).await.unwrap().unwrap(),
        Message::Text("unsolicited".into())
    );
    bounded(socket.send(Message::Ping(Bytes::from_static(b"ping"))))
        .await
        .unwrap();
    assert_eq!(
        bounded(socket.next()).await.unwrap().unwrap(),
        Message::Pong(Bytes::from_static(b"ping"))
    );
    assert!(opened.try_recv().is_err(), "one open per logical stream");

    bounded(socket.send(Message::Close(None))).await.unwrap();
    // Cleanup must happen on Close, without waiting for another client read or EOF.
    let ended = bounded(closed.recv()).await.unwrap();
    assert_eq!(ended.connection_id, context.connection_id);
    assert_eq!(ended.transport, context.transport);
    assert!(matches!(
        bounded(socket.next()).await,
        Some(Ok(Message::Close(_)))
    ));
    assert!(closed.try_recv().is_err(), "one close per logical stream");
}

async fn graceful_shutdown<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: WebSocketStream<S>,
    server: RunningTransport,
    opened: &mut mpsc::UnboundedReceiver<TransportContext>,
    closed: &mut mpsc::UnboundedReceiver<TransportContext>,
) {
    bounded(socket.send(Message::Text("shutdown".into())))
        .await
        .unwrap();
    let context = bounded(opened.recv()).await.unwrap();
    let _ = bounded(socket.next()).await.unwrap().unwrap();
    let stopping = tokio::spawn(server.close());
    assert_eq!(
        bounded(closed.recv()).await.unwrap().connection_id,
        context.connection_id
    );
    let close = bounded(socket.next()).await.unwrap().unwrap();
    assert!(
        matches!(close, Message::Close(Some(frame)) if frame.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away)
    );
    bounded(socket.flush()).await.unwrap();
    drop(socket);
    bounded(stopping).await.unwrap().unwrap();
}

fn tls() -> (rustls::ServerConfig, rustls::ClientConfig) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate: CertificateDer<'static> = generated.cert.into();
    let key = PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into());
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (server, client)
}

#[tokio::test]
async fn http1_persistent_lifecycle_push_and_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (protocol, mut opened, mut closed) = probe();
    let server = Http1WebSocketTransport::new(listener).start(protocol);
    let url = format!("ws://{address}/ws");
    let (mut socket, _) = bounded(tokio_tungstenite::connect_async(&url))
        .await
        .unwrap();
    exercise(&mut socket, "ws-http1", &mut opened, &mut closed, None).await;
    drop(socket);

    let (socket, _) = bounded(tokio_tungstenite::connect_async(&url))
        .await
        .unwrap();
    let context = bounded(opened.recv()).await.unwrap();
    drop(socket);
    assert_eq!(
        bounded(closed.recv()).await.unwrap().connection_id,
        context.connection_id
    );
    assert!(
        context
            .push_sender
            .unwrap()
            .send(Bytes::from_static(b"after EOF"))
            .is_err()
    );
    let (socket, _) = bounded(tokio_tungstenite::connect_async(&url))
        .await
        .unwrap();
    graceful_shutdown(socket, server, &mut opened, &mut closed).await;
}

#[tokio::test]
async fn wss_persistent_lifecycle_push_and_eof() {
    let (tls, client_tls) = tls();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (protocol, mut opened, mut closed) = probe();
    let server = TlsWebSocketTransport::new(listener, Arc::new(tls)).start(protocol);
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(client_tls));
    let url = format!("wss://localhost:{}/ws", address.port());
    let (mut socket, _) = bounded(tokio_tungstenite::connect_async_tls_with_config(
        &url,
        None,
        false,
        Some(connector.clone()),
    ))
    .await
    .unwrap();
    exercise(&mut socket, "ws-http1", &mut opened, &mut closed, None).await;
    drop(socket);
    let (socket, _) = bounded(tokio_tungstenite::connect_async_tls_with_config(
        &url,
        None,
        false,
        Some(connector.clone()),
    ))
    .await
    .unwrap();
    let context = bounded(opened.recv()).await.unwrap();
    drop(socket);
    assert_eq!(
        bounded(closed.recv()).await.unwrap().connection_id,
        context.connection_id
    );
    let (socket, _) = bounded(tokio_tungstenite::connect_async_tls_with_config(
        &url,
        None,
        false,
        Some(connector),
    ))
    .await
    .unwrap();
    graceful_shutdown(socket, server, &mut opened, &mut closed).await;
}

fn endpoints(alpn: &[u8]) -> (quinn::Endpoint, quinn::Endpoint) {
    let (mut server_tls, mut client_tls) = tls();
    server_tls.alpn_protocols = vec![alpn.to_vec()];
    client_tls.alpn_protocols = vec![alpn.to_vec()];
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_tls).unwrap(),
    ));
    let server = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).unwrap(),
    )));
    (server, client)
}

#[tokio::test]
async fn quic_persistent_lifecycle_push_and_migration() {
    let (endpoint, client) = endpoints(b"websocket");
    let address = endpoint.local_addr().unwrap();
    let (protocol, mut opened, mut closed) = probe();
    let server = QuicWebSocketTransport::new(endpoint).start(protocol);
    let connection = bounded(client.connect(address, "localhost").unwrap())
        .await
        .unwrap();
    let (send, recv) = bounded(connection.open_bi()).await.unwrap();
    let mut socket =
        WebSocketStream::from_raw_socket(tokio::io::join(recv, send), Role::Client, None).await;
    exercise(
        &mut socket,
        "ws-quic",
        &mut opened,
        &mut closed,
        Some(&client),
    )
    .await;
    drop(socket);

    let (send, recv) = bounded(connection.open_bi()).await.unwrap();
    let mut socket =
        WebSocketStream::from_raw_socket(tokio::io::join(recv, send), Role::Client, None).await;
    bounded(socket.send(Message::Text("EOF".into())))
        .await
        .unwrap();
    let context = bounded(opened.recv()).await.unwrap();
    let _ = bounded(socket.next()).await.unwrap().unwrap();
    drop(socket);
    assert_eq!(
        bounded(closed.recv()).await.unwrap().connection_id,
        context.connection_id
    );
    let (send, recv) = bounded(connection.open_bi()).await.unwrap();
    let socket =
        WebSocketStream::from_raw_socket(tokio::io::join(recv, send), Role::Client, None).await;
    graceful_shutdown(socket, server, &mut opened, &mut closed).await;
    bounded(client.wait_idle()).await;
}

#[tokio::test]
async fn http3_persistent_lifecycle_push_close_and_eof() {
    let (endpoint, client) = endpoints(b"h3");
    let address = endpoint.local_addr().unwrap();
    let (protocol, mut opened, mut closed) = probe();
    let mut server = Some(
        Http3WebSocketTransport::new(endpoint)
            .with_runtime(Arc::new(TransportRuntime::default()))
            .start(protocol),
    );
    let (mut sender, driver) = bounded(json_rpc_2_quic::client::transports::http3::connect_h3(
        &client,
        address,
        "localhost",
        true,
    ))
    .await
    .unwrap();
    for mode in ["close", "eof", "shutdown"] {
        let mut request = http::Request::builder()
            .method(http::Method::CONNECT)
            .uri("https://localhost/ws")
            .header(http::header::SEC_WEBSOCKET_VERSION, "13")
            .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap();
        request
            .extensions_mut()
            .insert(h3::ext::Protocol::WEBSOCKET);
        let stream = bounded(sender.send_request(request)).await.unwrap();
        let (mut send, mut recv) = stream.split();
        assert_eq!(
            bounded(recv.recv_response()).await.unwrap().status(),
            http::StatusCode::OK
        );
        let (io, bridge) = tokio::io::duplex(64 * 1024);
        let (mut reader, mut writer) = tokio::io::split(bridge);
        let outgoing = tokio::spawn(async move {
            let mut buffer = [0; 4096];
            loop {
                let count = reader.read(&mut buffer).await.unwrap();
                if count == 0 {
                    break;
                }
                send.send_data(Bytes::copy_from_slice(&buffer[..count]))
                    .await
                    .unwrap();
            }
            send.finish().await.unwrap();
        });
        let incoming = tokio::spawn(async move {
            while let Some(mut data) = recv.recv_data().await.unwrap() {
                let bytes = data.copy_to_bytes(data.remaining());
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });
        let mut socket = WebSocketStream::from_raw_socket(io, Role::Client, None).await;
        if mode == "shutdown" {
            graceful_shutdown(socket, server.take().unwrap(), &mut opened, &mut closed).await;
        } else if mode == "eof" {
            bounded(socket.send(Message::Text("EOF".into())))
                .await
                .unwrap();
            let context = bounded(opened.recv()).await.unwrap();
            let _ = bounded(socket.next()).await.unwrap().unwrap();
            drop(socket);
            assert_eq!(
                bounded(closed.recv()).await.unwrap().connection_id,
                context.connection_id
            );
        } else {
            exercise(&mut socket, "ws-http3", &mut opened, &mut closed, None).await;
            drop(socket);
        }
        bounded(outgoing).await.unwrap();
        bounded(incoming).await.unwrap();
    }
    bounded(json_rpc_2_quic::client::transports::http3::close_h3(
        &client, sender, driver,
    ))
    .await
    .unwrap();
    assert!(server.is_none());
}
