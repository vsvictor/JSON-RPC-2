use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use json_rpc_2_transport::RpcClientTransport;
use json_rpc_2_websockets::{
    client::transports::http1::Http1WebSocketClientTransport,
    server::transports::{
        WebSocketFuture, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
        http1::TlsWebSocketTransport,
    },
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;

struct Echo;

impl WebSocketProtocol for Echo {
    fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a> {
        Box::pin(async move { Some(message) })
    }
}

#[tokio::test]
async fn verified_tls_http1_upgrade_exchanges_websocket_messages() {
    let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
    let certificate: CertificateDer<'static> = generated.cert.into();
    let key = PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into());
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate.clone()], key)
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let server = TlsWebSocketTransport::new(listener, Arc::new(tls)).start(Arc::new(Echo));
    let url = format!("wss://{address}/ws");
    let client = Http1WebSocketClientTransport::connect_with_certificates_to_addr(
        &url,
        address,
        &[certificate],
        json_rpc_2_transport::TcpTuningConfig::default(),
    )
    .await
    .unwrap();

    let message = Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"id\":1}");
    let response = RpcClientTransport::send(&client, message.clone())
        .await
        .unwrap();
    assert_eq!(response, Some(message));

    RpcClientTransport::close(&client).await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn tls_client_rejects_untrusted_ca() {
    let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
    let certificate: CertificateDer<'static> = generated.cert.into();
    let key = PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into());
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate], key)
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = TlsWebSocketTransport::new(listener, Arc::new(tls)).start(Arc::new(Echo));

    let untrusted = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
    let untrusted: CertificateDer<'static> = untrusted.cert.into();
    let result = Http1WebSocketClientTransport::connect_with_certificates_to_addr(
        &format!("wss://{address}/ws"),
        address,
        &[untrusted],
        json_rpc_2_transport::TcpTuningConfig::default(),
    )
    .await;
    assert!(result.is_err());

    server.close().await.unwrap();
}
