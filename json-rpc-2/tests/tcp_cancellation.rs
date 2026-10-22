#![cfg(feature = "tcp")]

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use json_rpc_2::{
    JsonRpcProtocol, MultiTransportServer, RpcClientTransport, RpcServer, RunningServer,
    TransportKind,
    client::{RpcClient, RpcClientError, transports::tcp::TcpClientTransport},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::Notify,
};

const BOUND: Duration = Duration::from_secs(3);

async fn server() -> (
    RunningServer,
    Arc<TcpClientTransport>,
    Arc<Notify>,
    Arc<AtomicUsize>,
) {
    let rpc = Arc::new(RpcServer::new());
    let entered = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let slow_entered = entered.clone();
    let slow_calls = calls.clone();
    rpc.register("slow", move |_| {
        let entered = slow_entered.clone();
        let calls = slow_calls.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            entered.notify_one();
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(json!("old response"))
        }
    })
    .unwrap();
    rpc.register("echo", |params| async move { Ok(params) })
        .unwrap();
    let running = MultiTransportServer::builder(rpc as Arc<dyn JsonRpcProtocol>)
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_port(TransportKind::Tcp, 0)
        .start()
        .await
        .unwrap();
    let (_, address) = running.listeners().next().unwrap();
    let transport = Arc::new(TcpClientTransport::connect(address).await.unwrap());
    (running, transport, entered, calls)
}

#[tokio::test]
async fn timeout_then_next_request_succeeds_without_reconnect_or_replay() {
    let (running, transport, entered, calls) = server().await;
    let client = RpcClient::new(transport.clone());
    client
        .set_request_timeout(Duration::from_millis(80))
        .unwrap();
    let pending = {
        let client = client.clone();
        tokio::spawn(async move { client.request_value("slow", None).await })
    };
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    let result = tokio::time::timeout(BOUND, pending).await.unwrap().unwrap();
    assert!(matches!(result, Err(RpcClientError::Timeout { .. })));
    client.set_request_timeout(BOUND).unwrap();
    assert_eq!(
        tokio::time::timeout(
            BOUND,
            client.request_value("echo", Some(json!({"value": "new response"})))
        )
        .await
        .unwrap()
        .unwrap(),
        json!({"value": "new response"})
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "never replay a timed-out call"
    );
    transport.close().await.unwrap();
    running.close().await.unwrap();
}

#[tokio::test]
async fn aborted_caller_does_not_poison_concurrent_queued_calls() {
    let (running, transport, entered, calls) = server().await;
    let client = RpcClient::new(transport.clone());
    client.set_request_timeout(BOUND).unwrap();
    let pending = {
        let client = client.clone();
        tokio::spawn(async move { client.request_value("slow", None).await })
    };
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    let mut queued = Vec::new();
    for value in 0..4 {
        let client = client.clone();
        queued.push(tokio::spawn(async move {
            let result = client
                .request_value("echo", Some(json!({"value": value})))
                .await
                .unwrap();
            assert_eq!(result, json!({"value": value}));
        }));
    }
    tokio::time::timeout(BOUND, async {
        while client.metrics().active_requests < 5 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
    })
    .await
    .unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    for call in queued {
        tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    transport.close().await.unwrap();
    running.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_while_queued_preserves_the_active_exchange() {
    let (running, transport, entered, _) = server().await;
    let client = RpcClient::new(transport.clone());
    client.set_request_timeout(BOUND).unwrap();
    let pending = {
        let client = client.clone();
        tokio::spawn(async move { client.request_value("slow", None).await })
    };
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), transport.transport_ping())
            .await
            .is_err()
    );
    assert_eq!(
        tokio::time::timeout(BOUND, pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!("old response")
    );
    client
        .request_value("echo", Some(json!({"value": "still connected"})))
        .await
        .unwrap();
    transport.close().await.unwrap();
    running.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_ping_drops_socket_and_next_request_uses_a_new_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let transport = Arc::new(
        TcpClientTransport::connect(listener.local_addr().unwrap())
            .await
            .unwrap(),
    );
    let peer = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = BufReader::new(first);
        let mut line = String::new();
        first.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim_end().as_bytes(), json_rpc_2::CONTROL_PING);
        let mut rest = Vec::new();
        assert_eq!(first.read_to_end(&mut rest).await.unwrap(), 0);
        let (second, _) = listener.accept().await.unwrap();
        let mut second = BufReader::new(second);
        line.clear();
        second.read_line(&mut line).await.unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        let response = json!({"jsonrpc": "2.0", "id": request["id"], "result": "fresh"});
        second
            .get_mut()
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(80), transport.transport_ping())
            .await
            .is_err()
    );
    let client = RpcClient::new(transport);
    assert_eq!(
        tokio::time::timeout(BOUND, client.request_value("echo", None))
            .await
            .unwrap()
            .unwrap(),
        json!("fresh")
    );
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn close_after_cancellation_is_idempotent_and_does_not_reopen() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let transport = TcpClientTransport::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), transport.transport_ping())
            .await
            .is_err()
    );
    tokio::time::timeout(BOUND, transport.close())
        .await
        .unwrap()
        .unwrap();
    transport.close().await.unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(BOUND, peer.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(transport.transport_ping().await.is_err());
    assert!(
        transport
            .send_with_expectation("{}".into(), true)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancelled_partial_notification_is_not_flushed_on_the_next_connection() {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(64 * 1024).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let transport = TcpClientTransport::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    // Legal frame larger than the socket buffers; no reader drains the peer.
    let payload = format!(
        "{{\"jsonrpc\":\"2.0\",\"method\":\"notify\",\"params\":[\"{}\"]}}",
        "x".repeat(8 * 1024 * 1024)
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(80),
            transport.send_with_expectation(payload.into(), false),
        )
        .await
        .is_err()
    );
    let mut received = Vec::new();
    tokio::time::timeout(BOUND, peer.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(!received.is_empty(), "send must have started writing");
    assert!(
        !received.ends_with(b"\n"),
        "the old frame must remain incomplete"
    );
    let receiver = tokio::spawn(async move {
        let (second, _) = listener.accept().await.unwrap();
        let mut second = BufReader::new(second);
        let mut line = String::new();
        second.read_line(&mut line).await.unwrap();
        assert_eq!(line, "{}\n");
    });
    tokio::time::timeout(BOUND, transport.send_with_expectation("{}".into(), false))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(BOUND, receiver)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn oversized_notification_is_rejected_before_writing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let transport = TcpClientTransport::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let payload = vec![b'x'; json_rpc_2_core::MAX_MESSAGE_BYTES + 1].into();
    let result = tokio::time::timeout(BOUND, transport.send_with_expectation(payload, false))
        .await
        .expect("size validation must be bounded");
    assert!(result.unwrap_err().to_string().contains("maximum"));
    let mut byte = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.read(&mut byte))
            .await
            .is_err()
    );
    transport
        .send_with_expectation("{}".into(), false)
        .await
        .unwrap();
    let mut line = String::new();
    BufReader::new(peer).read_line(&mut line).await.unwrap();
    assert_eq!(line, "{}\n");
}
