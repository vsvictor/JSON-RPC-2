#![cfg(feature = "quic")]

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use json_rpc_2_quic::{
    client::transports::quic::QuicClientTransport, server::transports::quic::QuicTransport,
};
use json_rpc_2_transport::{
    JsonRpcProtocol, ProtocolFuture, RpcClientTransport, Transport, TransportContext,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
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

#[tokio::test]
async fn connection_identity_and_push_survive_real_path_migration() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert: rustls::pki_types::CertificateDer<'static> = identity.cert.into();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(identity.key_pair.serialize_der().into());
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
        tls.alpn_protocols = vec![b"session-test".to_vec()];
        let server = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap())),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"session-test".to_vec()];
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(tls).unwrap(),
        )));
        let (opened, mut sessions) = mpsc::unbounded_channel();
        let (requests, mut identities) = mpsc::unbounded_channel();
        let (closed, mut closures) = mpsc::unbounded_channel();
        let running = QuicTransport::new(server).start(Arc::new(Echo {
            opened,
            requests,
            closed,
        }));
        let client = QuicClientTransport::connect(endpoint.clone(), address, "localhost")
            .await
            .unwrap();
        let context = sessions.recv().await.unwrap();
        assert_eq!(context.transport, "quic");
        let payload = Bytes::from_static(b"\0\xffopaque\r\n");
        for _ in 0..3 {
            assert_eq!(
                client.send(payload.clone()).await.unwrap(),
                Some(payload.clone())
            );
            assert_eq!(identities.recv().await.unwrap(), context.connection_id);
        }
        let original = endpoint.local_addr().unwrap();
        endpoint
            .rebind(std::net::UdpSocket::bind("127.0.0.2:0").unwrap())
            .unwrap();
        assert_ne!(endpoint.local_addr().unwrap().ip(), original.ip());
        for _ in 0..3 {
            assert_eq!(
                client.send(payload.clone()).await.unwrap(),
                Some(payload.clone())
            );
            assert_eq!(identities.recv().await.unwrap(), context.connection_id);
        }
        context
            .push_sender
            .as_ref()
            .unwrap()
            .send(payload.clone())
            .unwrap();
        assert_eq!(client.recv_push().await.unwrap(), payload);
        assert!(
            sessions.try_recv().is_err(),
            "migration must not open another session"
        );
        client.close().await.unwrap();
        assert_eq!(closures.recv().await.unwrap(), context.connection_id);
        assert!(
            context
                .push_sender
                .unwrap()
                .send(Bytes::from_static(b"closed"))
                .is_err()
        );
        running.close().await.unwrap();
    })
    .await
    .expect("QUIC migration/session test timed out");
}
