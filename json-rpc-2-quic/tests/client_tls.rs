use std::{net::SocketAddr, sync::Arc, time::Duration};

use json_rpc_2_quic::client::ClientTlsConfig;
use rustls::pki_types::PrivateKeyDer;

#[test]
fn invalid_bundle_preserves_previous_trust() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = certificate.cert.pem();
    let mut config = ClientTlsConfig::default();
    config.set_ca_certificate_pem(&pem).unwrap();
    for invalid in [
        "",
        "not a certificate",
        "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----",
    ] {
        assert!(config.set_ca_certificate_pem(invalid).is_err());
        assert_eq!(config.ca_certificate_pem(), pem);
    }
}

#[tokio::test]
async fn endpoint_requires_certificate_before_opening_socket() {
    let error = ClientTlsConfig::default()
        .endpoint("127.0.0.1:0".parse().unwrap(), b"json-rpc-2")
        .unwrap_err();
    assert!(error.to_string().contains("PEM CA certificate"));
}

#[tokio::test]
async fn configured_trust_accepts_server_and_rejects_wrong_name_and_ca() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let other = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    for alpn in [b"json-rpc-2".as_slice(), b"h3", b"json-rpc-2-ws"] {
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            PrivateKeyDer::Pkcs8(certificate.key_pair.serialize_der().into()),
        )
        .unwrap();
        tls.alpn_protocols = vec![alpn.to_vec()];
        let server = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
            )),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        for (pem, name, success) in [
            (certificate.cert.pem(), "localhost", true),
            (certificate.cert.pem(), "wrong.example", false),
            (other.cert.pem(), "localhost", false),
        ] {
            let mut config = ClientTlsConfig::default();
            config.set_ca_certificate_pem(&pem).unwrap();
            let local: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let client = config.endpoint(local, alpn).unwrap();
            let connect = client.connect(server.local_addr().unwrap(), name).unwrap();
            let (connected, accepted) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(connect, async { server.accept().await.unwrap().await })
            })
            .await
            .unwrap();
            assert_eq!(connected.is_ok(), success);
            assert_eq!(accepted.is_ok(), success);
            client.close(0u32.into(), b"done");
        }
        server.close(0u32.into(), b"done");
    }
}
