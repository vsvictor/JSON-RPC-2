use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use json_rpc_2_transport::{JsonRpcProtocol, ProtocolFuture, TransportContext};
use tokio::sync::mpsc;

struct Echo {
    opened: mpsc::UnboundedSender<TransportContext>,
    requests: mpsc::UnboundedSender<String>,
    closed: mpsc::UnboundedSender<String>,
}

impl JsonRpcProtocol for Echo {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move { Some(Bytes::copy_from_slice(input)) })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        self.requests.send(context.connection_id.clone()).unwrap();
        self.handle_message(input)
    }

    fn session_opened(&self, context: &TransportContext) {
        self.opened.send(context.clone()).unwrap();
    }

    fn session_closed(&self, context: &TransportContext) {
        self.closed.send(context.connection_id.clone()).unwrap();
    }
}

#[cfg(all(feature = "sctp", any(target_os = "linux", target_os = "android")))]
#[tokio::test]
#[ignore = "requires SCTP kernel support"]
async fn sctp_session_reuses_identity_delivers_push_and_cleans_up_on_eof() {
    use json_rpc_2_datagram::server::transports::sctp::SctpTransport;
    use json_rpc_2_transport::Transport;
    use socket2::{Domain, Protocol, Socket, Type};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = SctpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr();
        let (opened, mut sessions) = mpsc::unbounded_channel();
        let (requests, mut identities) = mpsc::unbounded_channel();
        let (closed, mut closures) = mpsc::unbounded_channel();
        let running = listener.start(Arc::new(Echo {
            opened,
            requests,
            closed,
        }));
        let socket = tokio::task::spawn_blocking(move || {
            let socket = Socket::new(
                Domain::IPV4,
                Type::STREAM,
                Some(Protocol::from(libc::IPPROTO_SCTP)),
            )
            .unwrap();
            socket.connect(&address.into()).unwrap();
            socket.set_nonblocking(true).unwrap();
            std::net::TcpStream::from(socket)
        })
        .await
        .unwrap();
        let mut client = tokio::net::TcpStream::from_std(socket).unwrap();
        let context = sessions.recv().await.unwrap();
        assert_eq!(context.transport, "sctp");
        for _ in 0..3 {
            client.write_all(b"\0\xffopaque").await.unwrap();
            let mut response = [0; 8];
            client.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"\0\xffopaque");
            assert_eq!(identities.recv().await.unwrap(), context.connection_id);
        }
        context
            .push_sender
            .as_ref()
            .unwrap()
            .send(Bytes::from_static(b"push"))
            .unwrap();
        let mut push = [0; 4];
        client.read_exact(&mut push).await.unwrap();
        assert_eq!(&push, b"push");
        drop(client);
        assert_eq!(closures.recv().await.unwrap(), context.connection_id);
        assert!(context.push_sender.unwrap().send(Bytes::new()).is_err());
        running.close().await.unwrap();
    })
    .await
    .expect("SCTP lifecycle test timed out");
}

#[cfg(feature = "srt")]
#[tokio::test]
async fn srt_session_push_is_separate_from_correlated_responses() {
    use json_rpc_2_core::SrtConfig;
    use json_rpc_2_datagram::{
        client::transports::srt::SrtClientTransport, server::transports::srt::SrtTransport,
    };
    use json_rpc_2_transport::{RpcClientTransport, Transport};

    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = SrtTransport::bind(
            "127.0.0.1:0".parse().unwrap(),
            SrtConfig {
                passphrase: "session-push-test".into(),
            },
        )
        .await
        .unwrap();
        let address = listener.local_addr().unwrap();
        let (opened, mut sessions) = mpsc::unbounded_channel();
        let (requests, mut identities) = mpsc::unbounded_channel();
        let (closed, mut closures) = mpsc::unbounded_channel();
        let running = listener.start(Arc::new(Echo {
            opened,
            requests,
            closed,
        }));
        let client = SrtClientTransport::connect(address, "session-push-test")
            .await
            .unwrap();
        let context = sessions.recv().await.unwrap();
        let payload = Bytes::from(vec![0xff; 80_000]);
        for _ in 0..3 {
            context
                .push_sender
                .as_ref()
                .unwrap()
                .send(payload.clone())
                .unwrap();
            let (response, push) = tokio::join!(
                client.send_with_expectation(payload.clone(), true),
                client.recv_push(),
            );
            assert_eq!(response.unwrap(), Some(payload.clone()));
            assert_eq!(push.unwrap(), payload);
            assert_eq!(identities.recv().await.unwrap(), context.connection_id);
        }
        client.close().await.unwrap();
        assert_eq!(closures.recv().await.unwrap(), context.connection_id);
        running.close().await.unwrap();
    })
    .await
    .expect("SRT lifecycle test timed out");
}
