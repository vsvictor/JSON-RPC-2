use std::time::Duration;

use bytes::Bytes;
use json_rpc_2_transport::RpcClientTransport;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    time::timeout,
};

use super::*;

const PUSH_A: &str = r#"{"jsonrpc":"2.0","method":"a","params":{"n":1}}"#;
const PUSH_B: &str = r#"{"jsonrpc":"2.0","method":"b"}"#;
const RESPONSE: &str = r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#;
const REQUEST: &str = r#"{"jsonrpc":"2.0","id":1,"method":"m"}"#;

async fn server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, mut write) = stream.into_split();
        write.write_all(format!("{PUSH_A}\n").as_bytes()).await.unwrap();
        let mut lines = BufReader::new(read).lines();
        lines.next_line().await.unwrap().unwrap();
        write
            .write_all(format!("{PUSH_B}\n{RESPONSE}\n").as_bytes())
            .await
            .unwrap();
        let _ = lines.next_line().await;
    });
    (address, task)
}

#[tokio::test]
async fn idle_push_is_received() {
    let (address, _server) = server().await;
    let client = TcpClientTransport::connect(address).await.unwrap();
    let push = timeout(Duration::from_secs(2), client.recv_push())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(push, Bytes::from(PUSH_A));
}

#[tokio::test]
async fn push_during_exchange_is_not_taken_for_the_response() {
    let (address, _server) = server().await;
    let client = TcpClientTransport::connect(address).await.unwrap();
    let response = client
        .send(Bytes::from(REQUEST))
        .await
        .unwrap()
        .expect("response");
    assert_eq!(response, Bytes::from(RESPONSE));
    let first = client.recv_push().await.unwrap();
    let second = client.recv_push().await.unwrap();
    assert_eq!(first, Bytes::from(PUSH_A));
    assert_eq!(second, Bytes::from(PUSH_B));
}

#[tokio::test]
async fn request_interrupts_a_pending_recv_push() {
    let (address, _server) = server().await;
    let client = std::sync::Arc::new(TcpClientTransport::connect(address).await.unwrap());
    let waiting = {
        let client = client.clone();
        tokio::spawn(async move { client.recv_push().await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = timeout(Duration::from_secs(2), client.send(Bytes::from(REQUEST)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, Some(Bytes::from(RESPONSE)));
    let first = timeout(Duration::from_secs(2), waiting).await.unwrap().unwrap();
    assert_eq!(first.unwrap(), Bytes::from(PUSH_A));
}
