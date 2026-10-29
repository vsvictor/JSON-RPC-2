use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use json_rpc_2_backend::BackendBuilder;
use json_rpc_2_runtime::{H3_ALPN, QUIC_ALPN, TransportKind, WS_QUIC_ALPN};
use json_rpc_2_transport::{FrameBoundary, RpcClientTransport, StreamFraming, TransportMessage};
use rustls::pki_types::CertificateDer;

/// Independent client-side codec for the gateway v3 stream framing.
struct GatewayFraming;

impl StreamFraming for GatewayFraming {
    fn boundary(&self, prefix: &[u8]) -> std::io::Result<FrameBoundary> {
        const MAGIC: &[u8] = b"JRPC-GW\x03";
        let compared = prefix.len().min(MAGIC.len());
        if prefix[..compared] != MAGIC[..compared] {
            return Ok(FrameBoundary::Line);
        }
        if prefix.len() < 17 {
            return Ok(FrameBoundary::Need(17));
        }
        let id = u16::from_be_bytes([prefix[9], prefix[10]]) as usize;
        let transport = u16::from_be_bytes([prefix[11], prefix[12]]) as usize;
        let payload = u32::from_be_bytes([prefix[13], prefix[14], prefix[15], prefix[16]]) as usize;
        Ok(FrameBoundary::Frame(17 + id + transport + payload))
    }
}

const PASSPHRASE: &str = "test-only-srt-passphrase";
const PING: &[u8] = b"JRPC-GW\x03\x05\0\0\0\0\0\0\0\0";
const PONG: &[u8] = b"JRPC-GW\x03\x06\0\0\0\0\0\0\0\0";

fn endpoint(address: SocketAddr, cert: CertificateDer<'static>, alpn: &[u8]) -> quinn::Endpoint {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).unwrap();
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![alpn.to_vec()];
    let mut endpoint = quinn::Endpoint::client((address.ip(), 0).into()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(config).unwrap(),
    )));
    endpoint
}

async fn client(
    kind: TransportKind,
    address: SocketAddr,
    cert: CertificateDer<'static>,
) -> Box<dyn RpcClientTransport> {
    use json_rpc_2_datagram::client::transports as datagram;
    use json_rpc_2_quic::client::transports as quic;
    use json_rpc_2_tcp::client::transports as tcp;
    use json_rpc_2_websockets::client::transports as ws;
    macro_rules! simple {
        ($ty:path) => {
            Box::new(<$ty>::connect(address).await.unwrap())
        };
    }
    match kind {
        TransportKind::Tcp => Box::new(
            tcp::tcp::TcpClientTransport::connect(address)
                .await
                .unwrap()
                .with_stream_framing(Arc::new(GatewayFraming)),
        ),
        TransportKind::Tls => Box::new(
            tcp::tls::TlsClientTransport::connect_with_certificates(
                address,
                "localhost",
                &[cert],
                Default::default(),
            )
            .await
            .unwrap()
            .with_stream_framing(Arc::new(GatewayFraming)),
        ),
        TransportKind::Udp => simple!(datagram::udp::UdpClientTransport),
        TransportKind::Rtp => simple!(datagram::rtp::RtpClientTransport),
        TransportKind::Rudp => simple!(datagram::rudp::RudpClientTransport),
        TransportKind::Srt => Box::new(
            datagram::srt::SrtClientTransport::connect(address, PASSPHRASE)
                .await
                .unwrap(),
        ),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::UdpLite => simple!(datagram::udplite::UdpLiteClientTransport),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::Dccp => simple!(datagram::dccp::DccpClientTransport),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::Sctp => simple!(datagram::sctp::SctpClientTransport),
        TransportKind::Http1 => simple!(tcp::http1::Http1ClientTransport),
        TransportKind::Quic => Box::new(
            quic::quic::QuicClientTransport::connect(
                endpoint(address, cert, QUIC_ALPN),
                address,
                "localhost",
            )
            .await
            .unwrap(),
        ),
        TransportKind::Http3 => Box::new(
            quic::http3::Http3ClientTransport::connect(
                endpoint(address, cert, H3_ALPN),
                address,
                "localhost",
            )
            .await
            .unwrap(),
        ),
        TransportKind::WebSocketTcp => Box::new(
            ws::tcp::TcpWebSocketClientTransport::connect(&address.to_string())
                .await
                .unwrap(),
        ),
        TransportKind::WebSocketTls => Box::new(
            ws::http1::Http1WebSocketClientTransport::connect_with_certificates_to_addr(
                &format!("wss://localhost:{}/ws", address.port()),
                address,
                &[cert],
                Default::default(),
            )
            .await
            .unwrap(),
        ),
        TransportKind::WebSocketUdp => simple!(ws::udp::UdpWebSocketClientTransport),
        TransportKind::WebSocketRtp => simple!(ws::rtp::RtpWebSocketClientTransport),
        TransportKind::WebSocketRudp => simple!(ws::rudp::RudpWebSocketClientTransport),
        TransportKind::WebSocketSrt => Box::new(
            ws::srt::SrtWebSocketClientTransport::connect(address, PASSPHRASE)
                .await
                .unwrap(),
        ),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketUdpLite => simple!(ws::udplite::UdpLiteWebSocketClientTransport),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketDccp => simple!(ws::dccp::DccpWebSocketClientTransport),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketSctp => simple!(ws::sctp::SctpWebSocketClientTransport),
        TransportKind::WebSocketHttp1 => Box::new(
            ws::http1::Http1WebSocketClientTransport::connect(&format!("ws://{address}/ws"))
                .await
                .unwrap(),
        ),
        TransportKind::WebSocketQuic => Box::new(
            ws::quic::QuicWebSocketClientTransport::connect(
                endpoint(address, cert, WS_QUIC_ALPN),
                address,
                "localhost",
            )
            .await
            .unwrap(),
        ),
        TransportKind::WebSocketHttp3 => Box::new(
            ws::http3::Http3WebSocketClientTransport::connect(
                endpoint(address, cert, H3_ALPN),
                address,
                "localhost",
                "/ws",
            )
            .await
            .unwrap(),
        ),
    }
}

async fn exercise(kind: TransportKind) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = directory.path().join("cert.pem");
        let key_path = directory.path().join("key.pem");
        std::fs::write(&cert_path, identity.cert.pem()).unwrap();
        std::fs::write(&key_path, identity.key_pair.serialize_pem()).unwrap();
        let mut builder = BackendBuilder::new()
            .allow_ephemeral_ports()
            .tcp(0)
            .udp(0)
            .tls(0)
            .quic(0)
            .tls_files(cert_path, key_path)
            .srt_passphrase(PASSPHRASE);
        if !matches!(kind, TransportKind::Tcp | TransportKind::Tls | TransportKind::Quic | TransportKind::Udp) {
            builder = builder.transport(kind, 0);
        }
        let backend = builder.build().await.unwrap();
        let running = backend.start().await.unwrap();
        let transport = client(kind, running.address(kind).unwrap(), identity.cert.into()).await;
        assert_eq!(transport.send_message(TransportMessage::binary(Bytes::from_static(PING)), true).await.unwrap().unwrap(), PONG, "{}", kind.id());

        for payload in [
            b"{\n\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"echo\",\"params\":{\"through\":\"gateway\"}\n}".as_slice(),
            br#"{"jsonrpc":"2.0","method":"echo","params":{}}"#,
            br#"[{"jsonrpc":"2.0","id":1,"method":"echo","params":{"through":"gateway"}},{"jsonrpc":"2.0","method":"echo"}]"#,
        ] {
            let mut frame = b"JRPC-GW\x03\x01\0\x06\0\x03".to_vec();
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(b"clienttcp");
            frame.extend_from_slice(payload);
            let response = transport.send_message(TransportMessage::binary(frame.into()), true).await.unwrap().unwrap();
            assert_eq!(&response[..8], b"JRPC-GW\x03");
            if payload.starts_with(br#"{"jsonrpc":"2.0","method"#) {
                assert_eq!(response[8], 3, "{}", kind.id());
            } else {
                assert_eq!(response[8], 2, "{}", kind.id());
                let value: serde_json::Value = serde_json::from_slice(&response[17..]).unwrap();
                let value = if value.is_array() { &value[0] } else { &value };
                assert_eq!(value["result"]["through"], "gateway", "{}", kind.id());
            }
        }
        transport.close().await.unwrap();
        running.close().await.unwrap();
    }).await.unwrap_or_else(|_| panic!("{} exchange/shutdown timed out", kind.id()));
}

#[tokio::test]
async fn gateway_contract_over_portable_transport_matrix() {
    for &kind in TransportKind::ALL {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if matches!(
            kind,
            TransportKind::Dccp
                | TransportKind::Sctp
                | TransportKind::WebSocketDccp
                | TransportKind::WebSocketSctp
                | TransportKind::UdpLite
                | TransportKind::WebSocketUdpLite
        ) {
            continue;
        }
        exercise(kind).await;
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
#[ignore = "requires kernel DCCP support"]
async fn gateway_contract_over_dccp() {
    exercise(TransportKind::Dccp).await;
    exercise(TransportKind::WebSocketDccp).await;
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
#[ignore = "requires kernel SCTP support"]
async fn gateway_contract_over_sctp() {
    exercise(TransportKind::Sctp).await;
    exercise(TransportKind::WebSocketSctp).await;
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
#[ignore = "requires kernel UDP-Lite support"]
async fn gateway_contract_over_udplite() {
    exercise(TransportKind::UdpLite).await;
    exercise(TransportKind::WebSocketUdpLite).await;
}
