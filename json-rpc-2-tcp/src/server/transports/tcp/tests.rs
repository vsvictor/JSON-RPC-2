use super::*;
use json_rpc_2_transport::ProtocolFuture;

#[tokio::test]
async fn cancellation_closes_an_idle_stream_and_releases_its_session() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Gate {
        disabled: CancellationToken,
        opened: tokio::sync::Notify,
        closed: AtomicUsize,
    }
    impl JsonRpcProtocol for Gate {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
            panic!("idle stream must not dispatch")
        }
        fn session_opened(&self, _: &json_rpc_2_transport::TransportContext) {
            self.opened.notify_one();
        }
        fn session_closed(&self, _: &json_rpc_2_transport::TransportContext) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }
        fn session_cancelled<'a>(
            &'a self,
            _: &'a json_rpc_2_transport::TransportContext,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(async move { self.disabled.cancelled().await })
        }
    }
    let protocol = Arc::new(Gate {
        disabled: CancellationToken::new(),
        opened: tokio::sync::Notify::new(),
        closed: AtomicUsize::new(0),
    });
    let (mut client, server) = tokio::io::duplex(128);
    let (reader, writer) = tokio::io::split(server);
    let task = tokio::spawn(serve_stream(
        reader,
        writer,
        protocol.clone(),
        CancellationToken::new(),
        "127.0.0.1:12345".parse().unwrap(),
        "tcp",
    ));
    protocol.opened.notified().await;
    protocol.disabled.cancel();
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read(&mut client, &mut byte)
        )
        .await
        .unwrap()
        .unwrap(),
        0
    );
    task.await.unwrap().unwrap();
    assert_eq!(protocol.closed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dispatch_preserves_the_owned_input_allocation() {
    struct OwnedOnly(Bytes);
    impl JsonRpcProtocol for OwnedOnly {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
            panic!("stream dispatch must use the owned protocol entry point");
        }

        fn handle_owned_message<'a>(
            &'a self,
            input: Bytes,
            _: &'a json_rpc_2_transport::TransportContext,
        ) -> json_rpc_2_transport::ProtocolMessageFuture<'a> {
            Box::pin(async move {
                assert_eq!(input.as_ptr(), self.0.as_ptr());
                None
            })
        }
    }
    let input = Bytes::from_static(b"\xffopaque\npayload");
    let protocol: Arc<dyn JsonRpcProtocol> = Arc::new(OwnedOnly(input.clone()));
    let session = TransportSession::new("tcp", true);
    process_message(
        &protocol,
        &mut tokio::io::sink(),
        "127.0.0.1:12345".parse().unwrap(),
        input,
        &session.context,
        "tcp",
    )
    .await
    .unwrap();
}
