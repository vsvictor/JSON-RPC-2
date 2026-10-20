use std::{
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::client::transports::dccp::DccpClientTransport;
#[cfg(feature = "rtp")]
use json_rpc_2_datagram::client::transports::rtp::RtpClientTransport;
#[cfg(feature = "rudp")]
use json_rpc_2_datagram::client::transports::rudp::RudpClientTransport;
#[cfg(feature = "mqtt")]
use json_rpc_2_iot::client::transports::mqtt::MqttClientTransport;
#[cfg(feature = "coap")]
use json_rpc_2_iot::client::transports::coap::CoapClientTransport;
#[cfg(feature = "amqp")]
use json_rpc_2_iot::client::transports::amqp::AmqpClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::client::transports::sctp::SctpClientTransport;
#[cfg(feature = "srt")]
use json_rpc_2_datagram::client::transports::srt::SrtClientTransport;
#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
use json_rpc_2_datagram::client::transports::udplite::UdpLiteClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::client::transports::dccp::DccpWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::client::transports::sctp::SctpWebSocketClientTransport;
use json_rpc_2_client::{RpcClient, RpcClientError, RpcClientTransport, TransportError};
use json_rpc_2_datagram::client::transports::udp::UdpClientTransport;
use json_rpc_2_quic::client::transports::{http3::Http3ClientTransport, quic::QuicClientTransport};
use json_rpc_2_tcp::client::transports::{http1::Http1ClientTransport, tcp::TcpClientTransport};
use json_rpc_2_websockets::client::transports::{
    http1::Http1WebSocketClientTransport, http3::Http3WebSocketClientTransport,
    quic::QuicWebSocketClientTransport, tcp::TcpWebSocketClientTransport,
    udp::UdpWebSocketClientTransport,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::server::transports::dccp::DccpTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::server::transports::sctp::SctpTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::server::transports::dccp::DccpWebSocketTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::server::transports::sctp::SctpWebSocketTransport;
use json_rpc_2_server::{
    H3_ALPN, JsonRpcProtocol, MultiTransportServer, QUIC_ALPN, RegistrationError, RpcError,
    RpcServer, RunningServer, RunningTransport, SrtConfig, Transport, TransportKind, WS_QUIC_ALPN,
};
use json_rpc_2_datagram::server::transports::udp::UdpTransport;
use json_rpc_2_quic::server::transports::{http3::Http3Transport, quic::QuicTransport};
use json_rpc_2_tcp::server::transports::{http1::Http1Transport, tcp::TcpTransport};
use json_rpc_2_websockets::server::transports::{
    JsonRpcWebSocket, WebSocketProtocol, WebSocketTransport,
    http1::Http1WebSocketTransport, http3::Http3WebSocketTransport,
    quic::QuicWebSocketTransport, tcp::TcpWebSocketTransport, udp::UdpWebSocketTransport,
};
#[cfg(all(feature = "websocket", feature = "rtp"))]
use json_rpc_2_websockets::client::transports::rtp::RtpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "rudp"))]
use json_rpc_2_websockets::client::transports::rudp::RudpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "mqtt"))]
use json_rpc_2_websockets::client::transports::mqtt::MqttWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "coap"))]
use json_rpc_2_websockets::client::transports::coap::CoapWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "amqp"))]
use json_rpc_2_websockets::client::transports::amqp::AmqpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "srt"))]
use json_rpc_2_websockets::client::transports::srt::SrtWebSocketClientTransport;
#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2_websockets::client::transports::udplite::UdpLiteWebSocketClientTransport;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::{Value, json};
use tokio::net::TcpListener;

const CLOSE_LIMIT: Duration = Duration::from_secs(3);

fn make_server() -> Arc<dyn JsonRpcProtocol> {
    Arc::new(
        RpcServer::builder()
            .register("echo", |params| async move { Ok(params) })
            .unwrap()
            .build(),
    )
}

fn websocket_protocol(protocol: &Arc<dyn JsonRpcProtocol>) -> Arc<dyn WebSocketProtocol> {
    Arc::new(JsonRpcWebSocket::new(protocol.clone()))
}

fn quic_endpoints(ip: IpAddr, alpn: &[u8]) -> (quinn::Endpoint, quinn::Endpoint) {
    let certificate = rcgen::generate_simple_self_signed(vec![
        "localhost".into(),
        "127.0.0.1".into(),
        "::1".into(),
    ])
    .unwrap();
    let certificate_der: CertificateDer<'static> = certificate.cert.into();
    let key_der = PrivateKeyDer::Pkcs8(certificate.key_pair.serialize_der().into());

    let mut server_tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate_der.clone()], key_der)
    .unwrap();
    server_tls.alpn_protocols = vec![alpn.to_vec()];
    let server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_tls).unwrap()));
    let server_endpoint = quinn::Endpoint::server(server_config, SocketAddr::new(ip, 0)).unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate_der).unwrap();
    let mut client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    client_tls.alpn_protocols = vec![alpn.to_vec()];
    let client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_tls).unwrap()));
    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(ip, 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);
    (server_endpoint, client_endpoint)
}

type Connect<T> = std::pin::Pin<Box<dyn Future<Output = Result<T, TransportError>> + Send>>;

trait DynTransport: RpcClientTransport {}
impl<T: RpcClientTransport> DynTransport for T {}

impl RpcClientTransport for Box<dyn DynTransport> {
    fn send<'a>(&'a self, message: Bytes) -> json_rpc_2_client::ClientTransportFuture<'a> {
        (**self).send(message)
    }

    fn transport_ping<'a>(&'a self) -> json_rpc_2_client::ClientPingFuture<'a> {
        (**self).transport_ping()
    }

    fn restore<'a>(&'a self) -> json_rpc_2_client::ClientRecoverFuture<'a> {
        (**self).restore()
    }

    fn reconnect<'a>(&'a self) -> json_rpc_2_client::ClientRecoverFuture<'a> {
        (**self).reconnect()
    }

    fn close<'a>(&'a self) -> json_rpc_2_client::ClientCloseFuture<'a> {
        (**self).close()
    }
}

fn boxed<T: RpcClientTransport>(
    connect: impl Future<Output = Result<T, TransportError>> + Send + 'static,
) -> Connect<Box<dyn DynTransport>> {
    Box::pin(async move { Ok(Box::new(connect.await?) as Box<dyn DynTransport>) })
}

async fn start(
    ip: IpAddr,
    kind: &str,
    protocol: &Arc<dyn JsonRpcProtocol>,
) -> (RunningTransport, Connect<Box<dyn DynTransport>>) {
    let server_name = ip.to_string();
    match kind {
        "tcp" => {
            let listener = TcpListener::bind(SocketAddr::new(ip, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = TcpTransport::new(listener).start(protocol.clone());
            (server, boxed(TcpClientTransport::connect(address)))
        }
        "udp" => {
            let socket = tokio::net::UdpSocket::bind(SocketAddr::new(ip, 0))
                .await
                .unwrap();
            let address = socket.local_addr().unwrap();
            let server = UdpTransport::new(socket).start(protocol.clone());
            (server, boxed(UdpClientTransport::connect(address)))
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        "dccp" => {
            let transport = DccpTransport::bind(SocketAddr::new(ip, 0)).unwrap();
            let address = transport.local_addr();
            let server = transport.start(protocol.clone());
            (server, boxed(DccpClientTransport::connect(address)))
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        "sctp" => {
            let transport = SctpTransport::bind(SocketAddr::new(ip, 0)).unwrap();
            let address = transport.local_addr();
            let server = transport.start(protocol.clone());
            (server, boxed(SctpClientTransport::connect(address)))
        }
        "quic" => {
            let (server, client) = quic_endpoints(ip, QUIC_ALPN);
            let address = server.local_addr().unwrap();
            let server = QuicTransport::new(server).start(protocol.clone());
            (
                server,
                boxed(
                    async move { QuicClientTransport::connect(client, address, &server_name).await },
                ),
            )
        }
        "http1" => {
            let listener = TcpListener::bind(SocketAddr::new(ip, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = Http1Transport::new(listener).start(protocol.clone());
            (server, boxed(Http1ClientTransport::connect(address)))
        }
        "http3" => {
            let (server, client) = quic_endpoints(ip, H3_ALPN);
            let address = server.local_addr().unwrap();
            let server = Http3Transport::new(server).start(protocol.clone());
            (
                server,
                boxed(
                    async move { Http3ClientTransport::connect(client, address, &server_name).await },
                ),
            )
        }
        "websocket-tcp" => {
            let listener = TcpListener::bind(SocketAddr::new(ip, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = TcpWebSocketTransport::new(listener).start(websocket_protocol(protocol));
            (
                server,
                boxed(async move {
                    TcpWebSocketClientTransport::connect(&format!("ws://{address}")).await
                }),
            )
        }
        "websocket-udp" => {
            let socket = tokio::net::UdpSocket::bind(SocketAddr::new(ip, 0))
                .await
                .unwrap();
            let address = socket.local_addr().unwrap();
            let server = UdpWebSocketTransport::new(socket).start(websocket_protocol(protocol));
            (
                server,
                boxed(async move { UdpWebSocketClientTransport::connect(address).await }),
            )
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        "websocket-dccp" => {
            let transport = DccpWebSocketTransport::bind(SocketAddr::new(ip, 0)).unwrap();
            let address = transport.local_addr();
            let server = transport.start(websocket_protocol(protocol));
            (
                server,
                boxed(async move { DccpWebSocketClientTransport::connect(address).await }),
            )
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        "websocket-sctp" => {
            let transport = SctpWebSocketTransport::bind(SocketAddr::new(ip, 0)).unwrap();
            let address = transport.local_addr();
            let server = transport.start(websocket_protocol(protocol));
            (
                server,
                boxed(async move { SctpWebSocketClientTransport::connect(address).await }),
            )
        }
        "websocket-quic" => {
            let (server, client) = quic_endpoints(ip, WS_QUIC_ALPN);
            let address = server.local_addr().unwrap();
            let server = QuicWebSocketTransport::new(server).start(websocket_protocol(protocol));
            (
                server,
                boxed(async move {
                    QuicWebSocketClientTransport::connect(client, address, &server_name).await
                }),
            )
        }
        "websocket-http1" => {
            let listener = TcpListener::bind(SocketAddr::new(ip, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = Http1WebSocketTransport::new(listener).start(websocket_protocol(protocol));
            (
                server,
                boxed(async move {
                    Http1WebSocketClientTransport::connect(&format!("ws://{address}/ws")).await
                }),
            )
        }
        "websocket-http3" => {
            let (server, client) = quic_endpoints(ip, H3_ALPN);
            let address = server.local_addr().unwrap();
            let server = Http3WebSocketTransport::new(server).start(websocket_protocol(protocol));
            (
                server,
                boxed(async move {
                    Http3WebSocketClientTransport::connect(client, address, &server_name, "/ws")
                        .await
                }),
            )
        }
        other => panic!("unknown transport {other}"),
    }
}

const ADDRESS_FAMILIES: [IpAddr; 2] = [
    IpAddr::V4(Ipv4Addr::LOCALHOST),
    IpAddr::V6(Ipv6Addr::LOCALHOST),
];

const TRANSPORTS: &[&str] = &[
    "tcp",
    "udp",
    #[cfg(any(target_os = "linux", target_os = "android"))]
    "dccp",
    #[cfg(any(target_os = "linux", target_os = "android"))]
    "sctp",
    "quic",
    "http1",
    "http3",
    "websocket-tcp",
    "websocket-udp",
    #[cfg(any(target_os = "linux", target_os = "android"))]
    "websocket-dccp",
    #[cfg(any(target_os = "linux", target_os = "android"))]
    "websocket-sctp",
    "websocket-quic",
    "websocket-http1",
    "websocket-http3",
];

fn every_transport() -> impl Iterator<Item = (IpAddr, &'static str)> {
    ADDRESS_FAMILIES
        .into_iter()
        .flat_map(|ip| TRANSPORTS.iter().copied().map(move |kind| (ip, kind)))
}

#[tokio::test]
async fn batch_dispatch_runs_independent_calls_concurrently() {
    let protocol: Arc<dyn JsonRpcProtocol> = Arc::new(
        RpcServer::builder()
            .register("delay_echo", |params| async move {
                let delay_ms = params.get("delay_ms").and_then(Value::as_u64).unwrap_or(25);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                Ok(params["value"].clone())
            })
            .unwrap()
            .build(),
    );
    let batch = serde_json::to_vec(&json!([
        {"jsonrpc":"2.0","method":"delay_echo","params":{"value":1,"delay_ms":120},"id":1},
        {"jsonrpc":"2.0","method":"delay_echo","params":{"value":2,"delay_ms":120},"id":2},
        {"jsonrpc":"2.0","method":"delay_echo","params":{"value":3,"delay_ms":120},"id":3}
    ]))
    .unwrap();

    let started = tokio::time::Instant::now();
    let response = protocol.handle_message(&batch).await.unwrap();
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_millis(250),
        "batch should complete concurrently, took {elapsed:?}"
    );
    let responses: Vec<Value> = serde_json::from_slice(&response).unwrap();
    assert_eq!(responses.len(), 3);
}

#[tokio::test]
async fn one_rpc_server_accepts_clients_over_every_transport() {
    let protocol = make_server();
    let mut servers = Vec::new();
    for (ip, transport) in every_transport() {
        let kind = format!("{transport} {ip}");
        let (server, connect) = start(ip, transport, &protocol).await;
        servers.push((kind.clone(), server));
        let client = RpcClient::new(connect.await.unwrap());
        client.transport_ping().await.unwrap();

        let params = json!({"transport": kind});
        let result: Value = client.request("echo", params.clone()).await.unwrap();
        assert_eq!(result, params);
        client
            .notify("echo", Some(json!(["notification"])))
            .await
            .unwrap();
        let result: Value = client.request("echo", json!(["again"])).await.unwrap();
        assert_eq!(result, json!(["again"]));

        tokio::time::timeout(CLOSE_LIMIT, client.close())
            .await
            .unwrap_or_else(|_| panic!("{kind}: client close timed out"))
            .unwrap_or_else(|error| panic!("{kind}: client close failed: {error}"));
        assert!(
            matches!(
                client.request_value("echo", Some(json!([1]))).await,
                Err(RpcClientError::Transport(_)) | Err(RpcClientError::SessionClosed)
            ),
            "{kind}: send after close must fail"
        );
        client.close().await.unwrap();
    }

    for (kind, server) in servers {
        tokio::time::timeout(CLOSE_LIMIT, server.close())
            .await
            .unwrap_or_else(|_| panic!("{kind}: server close timed out"))
            .unwrap_or_else(|error| panic!("{kind}: server close failed: {error}"));
    }
}

#[tokio::test]
async fn server_close_finishes_while_clients_are_connected() {
    let protocol = make_server();
    for (ip, transport) in every_transport() {
        let kind = format!("{transport} {ip}");
        let (server, connect) = start(ip, transport, &protocol).await;
        let client = RpcClient::new(connect.await.unwrap());
        let result: Value = client.request("echo", json!([kind])).await.unwrap();
        assert_eq!(result, json!([kind]));

        let (server_closed, client_closed) = tokio::time::timeout(CLOSE_LIMIT, async {
            tokio::join!(server.close(), client.close())
        })
        .await
        .unwrap_or_else(|_| panic!("{kind}: simultaneous close timed out"));
        server_closed.unwrap_or_else(|error| panic!("{kind}: server close failed: {error}"));
        client_closed.unwrap_or_else(|error| panic!("{kind}: client close failed: {error}"));
    }
}

#[tokio::test]
async fn closed_server_refuses_new_clients() {
    let protocol = make_server();
    for (ip, transport) in every_transport() {
        let kind = format!("{transport} {ip}");
        let (server, connect) = start(ip, transport, &protocol).await;
        server.close().await.unwrap();
        let refused = tokio::time::timeout(Duration::from_millis(500), async {
            match connect.await {
                Err(_) => true,
                Ok(transport) => RpcClient::new(transport)
                    .request_value("echo", Some(json!([1])))
                    .await
                    .is_err(),
            }
        })
        .await
        .unwrap_or(true);
        assert!(refused, "{kind}: closed server must not serve new clients");
    }
}

fn identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let generated =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "::1".into()]).unwrap();
    let key = PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into());
    (generated.cert.into(), key)
}

fn client_endpoint(
    ip: IpAddr,
    certificate: &CertificateDer<'static>,
    alpn: &[u8],
) -> quinn::Endpoint {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate.clone()).unwrap();
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![alpn.to_vec()];
    let mut endpoint = quinn::Endpoint::client(SocketAddr::new(ip, 0)).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls).unwrap(),
    )));
    endpoint
}

async fn connect(
    kind: TransportKind,
    to: SocketAddr,
    certificate: &CertificateDer<'static>,
) -> Box<dyn DynTransport> {
    let server_name = to.ip().to_string();
    let connect = match kind {
        TransportKind::Tcp => boxed(TcpClientTransport::connect(to)),
        TransportKind::Udp => boxed(UdpClientTransport::connect(to)),
        #[cfg(feature = "rtp")]
        TransportKind::Rtp => boxed(RtpClientTransport::connect(to)),
        #[cfg(feature = "rudp")]
        TransportKind::Rudp => boxed(RudpClientTransport::connect(to)),
        #[cfg(feature = "mqtt")]
        TransportKind::Mqtt => boxed(MqttClientTransport::connect(to)),
        #[cfg(feature = "coap")]
        TransportKind::Coap => boxed(CoapClientTransport::connect(to)),
        #[cfg(feature = "amqp")]
        TransportKind::Amqp => boxed(AmqpClientTransport::connect(to)),
        #[cfg(feature = "srt")]
        TransportKind::Srt => boxed(SrtClientTransport::connect(to, "test-passphrase")),
        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        TransportKind::UdpLite => boxed(UdpLiteClientTransport::connect(to)),
        TransportKind::Quic => {
            let endpoint = client_endpoint(to.ip(), certificate, QUIC_ALPN);
            boxed(async move { QuicClientTransport::connect(endpoint, to, &server_name).await })
        }
        TransportKind::Http1 => boxed(Http1ClientTransport::connect(to)),
        TransportKind::Http3 => {
            let endpoint = client_endpoint(to.ip(), certificate, H3_ALPN);
            boxed(async move { Http3ClientTransport::connect(endpoint, to, &server_name).await })
        }
        TransportKind::WebSocketTcp => {
            boxed(async move { TcpWebSocketClientTransport::connect(&format!("ws://{to}")).await })
        }
        TransportKind::WebSocketUdp => boxed(UdpWebSocketClientTransport::connect(to)),
        #[cfg(all(feature = "websocket", feature = "rtp"))]
        TransportKind::WebSocketRtp => boxed(RtpWebSocketClientTransport::connect(to)),
        #[cfg(all(feature = "websocket", feature = "rudp"))]
        TransportKind::WebSocketRudp => boxed(RudpWebSocketClientTransport::connect(to)),
        #[cfg(all(feature = "websocket", feature = "mqtt"))]
        TransportKind::WebSocketMqtt => boxed(MqttWebSocketClientTransport::connect(to)),
        #[cfg(all(feature = "websocket", feature = "coap"))]
        TransportKind::WebSocketCoap => boxed(CoapWebSocketClientTransport::connect(to)),
        #[cfg(all(feature = "websocket", feature = "amqp"))]
        TransportKind::WebSocketAmqp => boxed(AmqpWebSocketClientTransport::connect(to)),
        #[cfg(all(feature = "websocket", feature = "srt"))]
        TransportKind::WebSocketSrt => {
            boxed(SrtWebSocketClientTransport::connect(to, "test-passphrase"))
        }
        #[cfg(all(
            feature = "websocket",
            feature = "udplite",
            any(target_os = "linux", target_os = "android")
        ))]
        TransportKind::WebSocketUdpLite => boxed(UdpLiteWebSocketClientTransport::connect(to)),
        TransportKind::WebSocketQuic => {
            let endpoint = client_endpoint(to.ip(), certificate, WS_QUIC_ALPN);
            boxed(
                async move { QuicWebSocketClientTransport::connect(endpoint, to, &server_name).await },
            )
        }
        TransportKind::WebSocketHttp1 => {
            boxed(
                async move { Http1WebSocketClientTransport::connect(&format!("ws://{to}/ws")).await },
            )
        }
        TransportKind::WebSocketHttp3 => {
            let endpoint = client_endpoint(to.ip(), certificate, H3_ALPN);
            boxed(async move {
                Http3WebSocketClientTransport::connect(endpoint, to, &server_name, "/ws").await
            })
        }
    };
    connect.await.unwrap()
}

#[tokio::test]
async fn serves_every_transport_on_every_bind_address() {
    let (certificate, key) = identity();
    let ips = [
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
    ];
    let builder = MultiTransportServer::builder(make_server())
        .set_bind(ips)
        .set_tcp_port(0)
        .set_udp_port(0)
        .set_quic_port(0)
        .set_http1_port(0)
        .set_http3_port(0)
        .set_ws_tcp_port(0)
        .set_ws_udp_port(0)
        .set_ws_rtp_port(0)
        .set_ws_quic_port(0)
        .set_ws_http1_port(0)
        .set_ws_http3_port(0);
    #[cfg(feature = "rtp")]
    let builder = builder.set_rtp_port(0);
    #[cfg(feature = "rudp")]
    let builder = builder.set_rudp_port(0).set_ws_rudp_port(0);
    #[cfg(feature = "mqtt")]
    let builder = builder.set_mqtt_port(0);
    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    let builder = builder.set_ws_mqtt_port(0);
    #[cfg(feature = "coap")]
    let builder = builder.set_coap_port(0);
    #[cfg(all(feature = "websocket", feature = "coap"))]
    let builder = builder.set_ws_coap_port(0);
    #[cfg(feature = "amqp")]
    let builder = builder.set_amqp_port(0);
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    let builder = builder.set_ws_amqp_port(0);
    #[cfg(feature = "srt")]
    let builder = builder
        .set_srt_config(SrtConfig {
            passphrase: "test-passphrase".to_owned(),
        })
        .set_srt_port(0)
        .set_ws_srt_port(0);
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    let builder = builder.set_udplite_port(0).set_ws_udplite_port(0);
    let server = builder.set_tls(vec![certificate.clone()], key).build();
    let running: RunningServer = server.start().await.unwrap();
    assert_eq!(
        running.listeners().count(),
        ADDRESS_FAMILIES.len() * TransportKind::ALL.len()
    );

    for ip in ips {
        for kind in TransportKind::ALL.iter().copied() {
            let to = running.address(kind, ip).unwrap();
            assert_ne!(to.port(), 0, "{kind:?} {ip}");
            let client = RpcClient::new(connect(kind, to, &certificate).await);
            let params = json!({"transport": kind.id(), "ip": ip});
            let result: Value = client.request("echo", params.clone()).await.unwrap();
            assert_eq!(result, params);
            client.close().await.unwrap();
        }
    }

    tokio::time::timeout(CLOSE_LIMIT, running.close())
        .await
        .expect("close timed out")
        .unwrap();
}

#[tokio::test]
async fn starts_only_transports_with_a_port() {
    let server = MultiTransportServer::builder(make_server())
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_tcp_port(0)
        .set_ws_tcp_port(0)
        .build();
    assert_eq!(server.port(TransportKind::Tcp), Some(0));
    assert_eq!(server.port(TransportKind::Quic), None);
    let running = server.start().await.unwrap();
    let kinds: Vec<_> = running.listeners().map(|(kind, _)| kind).collect();
    assert_eq!(kinds, [TransportKind::Tcp, TransportKind::WebSocketTcp]);
    running.close().await.unwrap();
}

#[tokio::test]
async fn typestate_multi_transport_builder_can_start_directly() {
    let running = MultiTransportServer::builder(make_server())
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_tcp_port(0)
        .start()
        .await
        .unwrap();
    assert_eq!(running.listeners().count(), 1);
    running.close().await.unwrap();
}

#[tokio::test]
async fn rejects_invalid_configuration() {
    let invalid = |server: &MultiTransportServer| {
        let error = server.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
        error.to_string()
    };

    let mut server = MultiTransportServer::new(make_server());
    server.set_tcp_port(40400);
    assert!(invalid(&server).contains("bind"));

    server
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .clear_port(TransportKind::Tcp);
    assert!(invalid(&server).contains("no transport"));

    server.set_quic_port(40401);
    assert!(invalid(&server).contains("TLS"));

    server
        .clear_port(TransportKind::Quic)
        .set_tcp_port(40400)
        .set_ws_tcp_port(40400);
    assert!(invalid(&server).contains("same TCP port"));

    let (certificate, key) = identity();
    server
        .clear_port(TransportKind::WebSocketTcp)
        .set_http1_port(40402)
        .set_quic_port(40400)
        .set_tls(vec![certificate], key);
    server.validate().unwrap();
}

#[tokio::test]
async fn closes_started_transports_when_one_cannot_start() {
    let taken = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = taken.local_addr().unwrap().port();
    let server = MultiTransportServer::builder(make_server())
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_tcp_port(0)
        .set_http1_port(port)
        .build();
    let error = server.start().await.err().expect("port is taken");
    assert!(error.to_string().contains("HTTP/1.1"), "{error}");
}

fn protocol_server() -> RpcServer {
    let server = RpcServer::new();
    server
        .register("subtract", |params| async move {
            let values = params.as_array().ok_or_else(RpcError::invalid_params)?;
            let a = values
                .first()
                .and_then(Value::as_i64)
                .ok_or_else(RpcError::invalid_params)?;
            let b = values
                .get(1)
                .and_then(Value::as_i64)
                .ok_or_else(RpcError::invalid_params)?;
            Ok(json!(a - b))
        })
        .unwrap();
    server
}

#[tokio::test]
async fn protocol_handles_positional_and_named_params() {
    let response = protocol_server()
        .handle_message(br#"{"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":0}"#)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response).unwrap(),
        json!({"jsonrpc":"2.0","result":19,"id":0})
    );

    let server = RpcServer::new();
    server
        .register("subtract", |params| async move {
            let object = params.as_object().ok_or_else(RpcError::invalid_params)?;
            let minuend = object
                .get("minuend")
                .and_then(Value::as_i64)
                .ok_or_else(RpcError::invalid_params)?;
            let subtrahend = object
                .get("subtrahend")
                .and_then(Value::as_i64)
                .ok_or_else(RpcError::invalid_params)?;
            Ok(json!(minuend - subtrahend))
        })
        .unwrap();
    let response = server
        .handle_message(
            br#"{"jsonrpc":"2.0","method":"subtract","params":{"subtrahend":23,"minuend":42},"id":"named"}"#,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response).unwrap(),
        json!({"jsonrpc":"2.0","result":19,"id":"named"})
    );
}

#[tokio::test]
async fn protocol_notifications_and_batches_follow_spec() {
    let server = protocol_server();
    assert!(
        server
            .handle_message(br#"{"jsonrpc":"2.0","method":"subtract","params":[1,1]}"#)
            .await
            .is_none()
    );

    let response = server
        .handle_message(
            br#"[{"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":"a"},{"jsonrpc":"2.0","method":"subtract","params":[1,1]}]"#,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response).unwrap(),
        json!([{"jsonrpc":"2.0","result":19,"id":"a"}])
    );
}

#[tokio::test]
async fn protocol_returns_standard_errors_for_invalid_messages() {
    for (input, code) in [
        (b"{".as_slice(), -32700),
        (b"[]".as_slice(), -32600),
        (
            br#"{"jsonrpc":"2.0","method":"absent","id":7}"#.as_slice(),
            -32601,
        ),
        (
            br#"{"jsonrpc":"2.0","method":"subtract","params":null,"id":7}"#.as_slice(),
            -32600,
        ),
    ] {
        let response = protocol_server().handle_message(input).await.unwrap();
        let value: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(value["error"]["code"], code);
    }
}

#[test]
fn typestate_builder_and_registration_rules_work() {
    let _dispatcher = RpcServer::builder()
        .register("echo", |params| async move { Ok(params) })
        .unwrap()
        .build();

    let _multi = MultiTransportServer::builder(make_server())
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_tcp_port(0)
        .build();

    let server = RpcServer::new();
    assert_eq!(
        server.register("rpc.discover", |_| async { Ok(Value::Null) }),
        Err(RegistrationError::ReservedMethod)
    );
    server
        .register("echo", |_| async { Ok(Value::Null) })
        .unwrap();
    assert_eq!(
        server.register("echo", |_| async { Ok(Value::Null) }),
        Err(RegistrationError::DuplicateMethod)
    );
}
