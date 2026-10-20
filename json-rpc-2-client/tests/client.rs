use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};

use json_rpc_2_client::{
    ClientCloseFuture, ClientLivenessConfig, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, ReconnectPolicy, RpcClient, RpcClientConfig, RpcClientError,
    RpcClientTransport, RpcError, TransportError, TransportLimits,
};

struct MockTransport {
    request: Arc<Mutex<Option<Vec<u8>>>>,
    response: Mutex<Option<Vec<u8>>>,
    closed: Arc<Mutex<bool>>,
    ping_count: Arc<AtomicUsize>,
    reconnect_count: Arc<AtomicUsize>,
    restore_count: Arc<AtomicUsize>,
    ping_results: Arc<Mutex<VecDeque<Result<(), TransportError>>>>,
    restore_results: Arc<Mutex<VecDeque<Result<(), TransportError>>>>,
    send_delay: Option<Duration>,
}

impl RpcClientTransport for MockTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            if let Some(delay) = self.send_delay {
                tokio::time::sleep(delay).await;
            }
            *self.request.lock().expect("request mutex poisoned") = Some(message.to_vec());
            Ok(self
                .response
                .lock()
                .expect("response mutex poisoned")
                .take()
                .map(Bytes::from))
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            *self.closed.lock().expect("closed mutex poisoned") = true;
            Ok(())
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            self.ping_count.fetch_add(1, Ordering::Relaxed);
            self.ping_results
                .lock()
                .expect("ping results mutex poisoned")
                .pop_front()
                .unwrap_or(Ok(()))
        })
    }

    fn restore<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            self.restore_count.fetch_add(1, Ordering::Relaxed);
            self.restore_results
                .lock()
                .expect("restore results mutex poisoned")
                .pop_front()
                .unwrap_or(Ok(()))
        })
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            self.reconnect_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

fn mock(response: Option<&[u8]>) -> (MockTransport, Arc<Mutex<Option<Vec<u8>>>>) {
    let request = Arc::new(Mutex::new(None));
    (
        MockTransport {
            request: request.clone(),
            response: Mutex::new(response.map(<[u8]>::to_vec)),
            closed: Arc::new(Mutex::new(false)),
            ping_count: Arc::new(AtomicUsize::new(0)),
            reconnect_count: Arc::new(AtomicUsize::new(0)),
            restore_count: Arc::new(AtomicUsize::new(0)),
            ping_results: Arc::new(Mutex::new(VecDeque::new())),
            restore_results: Arc::new(Mutex::new(VecDeque::new())),
            send_delay: None,
        },
        request,
    )
}

#[tokio::test]
async fn sends_call_and_decodes_matching_response() {
    let (transport, request) = mock(Some(br#"{"jsonrpc":"2.0","result":42,"id":1}"#));
    let client = RpcClient::new(transport);
    let result: i64 = client
        .request("answer", json!({"life": true}))
        .await
        .unwrap();
    assert_eq!(result, 42);

    let sent: Value = serde_json::from_slice(
        request
            .lock()
            .expect("request mutex poisoned")
            .as_ref()
            .expect("request captured"),
    )
    .unwrap();
    assert_eq!(
        sent,
        json!({"jsonrpc":"2.0","method":"answer","params":{"life":true},"id":1})
    );
}

#[tokio::test]
async fn reports_remote_errors_and_mismatched_ids() {
    let (transport, _) = mock(Some(
        br#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":1}"#,
    ));
    let client = RpcClient::new(transport);
    assert!(matches!(
        client.request_value("absent", None).await,
        Err(RpcClientError::Remote(RpcError { code: -32601, .. }))
    ));

    let (transport, _) = mock(Some(br#"{"jsonrpc":"2.0","result":1,"id":99}"#));
    let client = RpcClient::new(transport);
    assert!(matches!(
        client.request_value("answer", None).await,
        Err(RpcClientError::IdMismatch)
    ));
}

#[tokio::test]
async fn set_ping_frequency_uses_restore_then_reconnect_after_threshold() {
    let (transport, _) = mock(None);
    let reconnect_count = transport.reconnect_count.clone();
    let restore_count = transport.restore_count.clone();
    transport
        .ping_results
        .lock()
        .expect("ping results mutex poisoned")
        .extend([
            Err(TransportError::new("miss-1")),
            Err(TransportError::new("miss-2")),
            Err(TransportError::new("miss-3")),
            Err(TransportError::new("miss-4")),
        ]);
    transport
        .restore_results
        .lock()
        .expect("restore results mutex poisoned")
        .push_back(Err(TransportError::new("restore-failed")));

    let client = RpcClient::new(transport);
    client.set_max_missed_pongs(4).await.unwrap();
    client.set_ping_frequency(10).await.unwrap();

    tokio::time::timeout(Duration::from_millis(200), async {
        while reconnect_count.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("client never attempted reconnect");
    assert_eq!(restore_count.load(Ordering::Relaxed), 1);

    client.set_ping_frequency(0).await.unwrap();
}

#[tokio::test]
async fn rejects_zero_max_missed_pongs() {
    let (transport, _) = mock(None);
    let client = RpcClient::new(transport);
    assert!(matches!(
        client.set_max_missed_pongs(0).await,
        Err(RpcClientError::InvalidConfiguration(_))
    ));
}

#[tokio::test]
async fn request_timeout_is_applied_by_session_actor() {
    let (mut transport, _) = mock(Some(br#"{"jsonrpc":"2.0","result":42,"id":1}"#));
    transport.send_delay = Some(Duration::from_millis(50));
    let client = RpcClient::new_with_config(
        transport,
        RpcClientConfig {
            request_timeout: Duration::from_millis(10),
            liveness: ClientLivenessConfig::default(),
            reconnect: ReconnectPolicy::default(),
            limits: TransportLimits::default(),
        },
    )
    .unwrap();

    assert!(matches!(
        client.request_value("answer", None).await,
        Err(RpcClientError::Timeout { method, .. }) if method == "answer"
    ));
    assert_eq!(client.metrics().request_timeouts_total, 1);
}

#[tokio::test]
async fn rejects_invalid_reconnect_policy() {
    let (transport, _) = mock(None);
    let client = RpcClient::new(transport);
    assert!(matches!(
        client.set_reconnect_policy(ReconnectPolicy {
            restore_before_reconnect: true,
            reconnect_cooldown: Duration::from_millis(20),
            max_reconnect_backoff: Duration::from_millis(10),
            reconnect_jitter: Duration::ZERO,
            max_reconnect_elapsed: None,
        }),
        Err(RpcClientError::InvalidConfiguration(_))
    ));
}
