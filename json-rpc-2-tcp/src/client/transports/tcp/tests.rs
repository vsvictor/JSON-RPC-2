use std::time::Duration;

use bytes::Bytes;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use super::*;

const PUSH: &[u8] = br#"{"jsonrpc":"2.0","method":"events.event","params":{"n":1}}"#;
const RESPONSE: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#;
const REQUEST: &[u8] = br#"{"jsonrpc":"2.0","id":1,"method":"echo"}"#;

async fn listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    (listener, address)
}

async fn write_line(stream: &mut TcpStream, line: &[u8]) {
    stream.write_all(line).await.unwrap();
    stream.write_all(b"\n").await.unwrap();
    stream.flush().await.unwrap();
}

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("operation timed out")
}

#[tokio::test]
async fn receives_push_while_idle() {
    let (listener, address) = listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        write_line(&mut stream, PUSH).await;
        stream
    });
    let client = TcpClientTransport::connect(address).await.unwrap();
    assert!(client.supports_incoming());
    assert_eq!(
        within(client.recv_incoming()).await.unwrap(),
        Bytes::from_static(PUSH)
    );
    drop(server.await.unwrap());
}

#[tokio::test]
async fn push_before_response_is_not_taken_as_the_reply() {
    let (listener, address) = listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let mut request = String::new();
        stream.read_line(&mut request).await.unwrap();
        write_line(stream.get_mut(), PUSH).await;
        write_line(stream.get_mut(), RESPONSE).await;
        stream
    });
    let client = TcpClientTransport::connect(address).await.unwrap();
    let response = within(client.send(Bytes::from_static(REQUEST)))
        .await
        .unwrap();
    assert_eq!(response, Some(Bytes::from_static(RESPONSE)));
    assert_eq!(
        within(client.recv_incoming()).await.unwrap(),
        Bytes::from_static(PUSH)
    );
    drop(server.await.unwrap());
}

#[tokio::test]
async fn waiting_listener_yields_the_connection_to_requests_and_pings() {
    let (listener, address) = listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        for _ in 0..2 {
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let reply: &[u8] = if line.trim_end().as_bytes() == CONTROL_PING {
                CONTROL_PONG
            } else {
                RESPONSE
            };
            write_line(stream.get_mut(), reply).await;
        }
        write_line(stream.get_mut(), PUSH).await;
        stream
    });
    let client = std::sync::Arc::new(TcpClientTransport::connect(address).await.unwrap());
    let listening = {
        let client = std::sync::Arc::clone(&client);
        tokio::spawn(async move { client.recv_incoming().await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let response = within(client.send(Bytes::from_static(REQUEST)))
        .await
        .unwrap();
    assert_eq!(response, Some(Bytes::from_static(RESPONSE)));
    within(client.transport_ping()).await.unwrap();
    let push = within(listening).await.unwrap().unwrap();
    assert_eq!(push, Bytes::from_static(PUSH));
    drop(server.await.unwrap());
}

#[tokio::test]
async fn listener_survives_server_disconnect_and_transparent_reopen() {
    let (listener, address) = listener().await;
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        drop(first);
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let mut request = String::new();
        stream.read_line(&mut request).await.unwrap();
        write_line(stream.get_mut(), RESPONSE).await;
        write_line(stream.get_mut(), PUSH).await;
        stream
    });
    let client = std::sync::Arc::new(TcpClientTransport::connect(address).await.unwrap());
    let listening = {
        let client = std::sync::Arc::clone(&client);
        tokio::spawn(async move { client.recv_incoming().await })
    };
    // Let the listener observe EOF and invalidate the first connection.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = within(client.send(Bytes::from_static(REQUEST)))
        .await
        .unwrap();
    assert_eq!(response, Some(Bytes::from_static(RESPONSE)));
    let push = within(listening).await.unwrap().unwrap();
    assert_eq!(push, Bytes::from_static(PUSH));
    drop(server.await.unwrap());
}

#[tokio::test]
async fn close_ends_a_waiting_listener() {
    let (listener, address) = listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink).await;
    });
    let client = std::sync::Arc::new(TcpClientTransport::connect(address).await.unwrap());
    let listening = {
        let client = std::sync::Arc::clone(&client);
        tokio::spawn(async move { client.recv_incoming().await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    within(client.close()).await.unwrap();
    assert!(within(listening).await.unwrap().is_err());
    within(server).await.unwrap();
}

#[tokio::test]
async fn cancelled_listener_keeps_a_partial_push_line() {
    let (listener, address) = listener().await;
    let (half_sent, half_sent_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let (head, tail) = PUSH.split_at(10);
        stream.write_all(head).await.unwrap();
        stream.flush().await.unwrap();
        half_sent.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        write_line(&mut stream, tail).await;
        stream
    });
    let client = TcpClientTransport::connect(address).await.unwrap();
    half_sent_rx.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.recv_incoming())
            .await
            .is_err()
    );
    assert_eq!(
        within(client.recv_incoming()).await.unwrap(),
        Bytes::from_static(PUSH)
    );
    drop(server.await.unwrap());
}

#[tokio::test]
async fn custom_framing_does_not_support_incoming() {
    struct Fixed;
    impl json_rpc_2_transport::StreamFraming for Fixed {
        fn boundary(&self, _: &[u8]) -> std::io::Result<json_rpc_2_transport::FrameBoundary> {
            Ok(json_rpc_2_transport::FrameBoundary::Need(4))
        }
    }
    let (listener, address) = listener().await;
    let _server = tokio::spawn(async move { listener.accept().await });
    let client = TcpClientTransport::connect(address)
        .await
        .unwrap()
        .with_stream_framing(std::sync::Arc::new(Fixed));
    assert!(!client.supports_incoming());
    assert!(within(client.recv_incoming()).await.is_err());
}
