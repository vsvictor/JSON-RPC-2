use std::future::pending;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use bytes::Bytes;

use json_rpc_2_client::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture, RpcClient,
    RpcClientError, RpcClientTransport,
};

#[derive(Default)]
struct HangingTransport {
    hang: AtomicBool,
    pings: Arc<AtomicUsize>,
    restores: Arc<AtomicUsize>,
    reconnects: Arc<AtomicUsize>,
}

impl RpcClientTransport for HangingTransport {
    fn send<'a>(&'a self, _message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async { Ok(None) })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            self.pings.fetch_add(1, Ordering::SeqCst);
            if self.hang.load(Ordering::SeqCst) {
                pending::<()>().await;
            }
            Ok(())
        })
    }

    fn restore<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            self.restores.fetch_add(1, Ordering::SeqCst);
            if self.hang.load(Ordering::SeqCst) {
                pending::<()>().await;
            }
            Ok(())
        })
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            self.reconnects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[tokio::test]
async fn bounded_ping_and_restore_succeed_on_live_transport() {
    let transport = HangingTransport::default();
    let (pings, restores, reconnects) = (
        transport.pings.clone(),
        transport.restores.clone(),
        transport.reconnects.clone(),
    );
    let client = RpcClient::new(transport);

    client
        .transport_ping_with_timeout(Duration::from_millis(100))
        .await
        .unwrap();
    client
        .restore_connection(Duration::from_millis(100))
        .await
        .unwrap();

    assert_eq!(pings.load(Ordering::SeqCst), 1);
    assert_eq!(restores.load(Ordering::SeqCst), 1);
    assert_eq!(reconnects.load(Ordering::SeqCst), 0);
    let log = client.take_liveness_log();
    assert!(log.contains("PONG: manual transport pong received"), "{log}");
    assert!(log.contains("RESTORED: manual restore succeeded"), "{log}");
}

#[tokio::test]
async fn bounded_ping_and_restore_time_out_inside_runtime() {
    let transport = HangingTransport::default();
    transport.hang.store(true, Ordering::SeqCst);
    let (restores, reconnects) = (transport.restores.clone(), transport.reconnects.clone());
    let client = RpcClient::new(transport);

    let started = Instant::now();
    let ping = client
        .transport_ping_with_timeout(Duration::from_millis(50))
        .await
        .unwrap_err();
    let restore = client
        .restore_connection(Duration::from_millis(50))
        .await
        .unwrap_err();

    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(ping.to_string().contains("pong deadline exceeded after 50 ms"), "{ping}");
    assert!(restore.to_string().contains("restore deadline exceeded after 50 ms"), "{restore}");
    assert_eq!(restores.load(Ordering::SeqCst), 1, "restore must be a single attempt");
    assert_eq!(reconnects.load(Ordering::SeqCst), 0, "restore must not rebuild the client");
}

#[tokio::test]
async fn bounded_operations_reject_zero_timeout_and_closed_client() {
    let client = RpcClient::new(HangingTransport::default());
    assert!(matches!(
        client.transport_ping_with_timeout(Duration::ZERO).await,
        Err(RpcClientError::InvalidConfiguration(_))
    ));
    assert!(matches!(
        client.restore_connection(Duration::ZERO).await,
        Err(RpcClientError::InvalidConfiguration(_))
    ));
    client.close().await.unwrap();
    assert!(matches!(
        client.transport_ping_with_timeout(Duration::from_millis(10)).await,
        Err(RpcClientError::SessionClosed)
    ));
    assert!(matches!(
        client.restore_connection(Duration::from_millis(10)).await,
        Err(RpcClientError::SessionClosed)
    ));
}
