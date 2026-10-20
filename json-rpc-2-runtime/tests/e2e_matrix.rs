use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use json_rpc_2_client::{RpcClient, RpcClientTransport, TransportError};
use json_rpc_2_core::SrtConfig;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::client::transports::dccp::DccpClientTransport;
#[cfg(feature = "rtp")]
use json_rpc_2_datagram::client::transports::rtp::RtpClientTransport;
#[cfg(feature = "rudp")]
use json_rpc_2_datagram::client::transports::rudp::RudpClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::client::transports::sctp::SctpClientTransport;
#[cfg(feature = "srt")]
use json_rpc_2_datagram::client::transports::srt::SrtClientTransport;
use json_rpc_2_datagram::client::transports::udp::UdpClientTransport;
#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
use json_rpc_2_datagram::client::transports::udplite::UdpLiteClientTransport;
#[cfg(feature = "amqp")]
use json_rpc_2_iot::client::transports::amqp::AmqpClientTransport;
#[cfg(feature = "coap")]
use json_rpc_2_iot::client::transports::coap::CoapClientTransport;
#[cfg(feature = "mqtt")]
use json_rpc_2_iot::client::transports::mqtt::MqttClientTransport;
use json_rpc_2_quic::client::transports::{http3::Http3ClientTransport, quic::QuicClientTransport};
use json_rpc_2_runtime::{
    H3_ALPN, MultiTransportServer, QUIC_ALPN, RunningServer, TransportKind, WS_QUIC_ALPN,
};
use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
use json_rpc_2_tcp::client::transports::{http1::Http1ClientTransport, tcp::TcpClientTransport};
#[cfg(all(feature = "websocket", feature = "amqp"))]
use json_rpc_2_websockets::client::transports::amqp::AmqpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "coap"))]
use json_rpc_2_websockets::client::transports::coap::CoapWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::client::transports::dccp::DccpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "mqtt"))]
use json_rpc_2_websockets::client::transports::mqtt::MqttWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "rtp"))]
use json_rpc_2_websockets::client::transports::rtp::RtpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "rudp"))]
use json_rpc_2_websockets::client::transports::rudp::RudpWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::client::transports::sctp::SctpWebSocketClientTransport;
#[cfg(all(feature = "websocket", feature = "srt"))]
use json_rpc_2_websockets::client::transports::srt::SrtWebSocketClientTransport;
#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
use json_rpc_2_websockets::client::transports::udplite::UdpLiteWebSocketClientTransport;
use json_rpc_2_websockets::client::transports::{
    http1::Http1WebSocketClientTransport, http3::Http3WebSocketClientTransport,
    quic::QuicWebSocketClientTransport, tcp::TcpWebSocketClientTransport,
    udp::UdpWebSocketClientTransport,
};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::{Value, json};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

trait DynTransport: RpcClientTransport {}
impl<T: RpcClientTransport> DynTransport for T {}

type DynClient = RpcClient<Arc<dyn DynTransport>>;
type DynTransportConnect =
    std::pin::Pin<Box<dyn Future<Output = Result<Arc<dyn DynTransport>, TransportError>> + Send>>;

fn make_server() -> Arc<RpcServer> {
    let server = Arc::new(RpcServer::new());
    server
        .register("echo", |params| async move { Ok(params) })
        .unwrap();
    server
        .register("delay_echo", |params| async move {
            let delay_ms = params.get("delay_ms").and_then(Value::as_u64).unwrap_or(25);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            Ok(params.get("value").cloned().unwrap_or(Value::Null))
        })
        .unwrap();
    server
}

fn identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let generated =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
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

fn boxed<T: RpcClientTransport>(
    connect: impl Future<Output = Result<T, TransportError>> + Send + 'static,
) -> DynTransportConnect {
    Box::pin(async move { Ok(Arc::new(connect.await?) as Arc<dyn DynTransport>) })
}

async fn connect(
    kind: TransportKind,
    to: SocketAddr,
    certificate: &CertificateDer<'static>,
) -> Arc<dyn DynTransport> {
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
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::Dccp => boxed(DccpClientTransport::connect(to)),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::Sctp => boxed(SctpClientTransport::connect(to)),
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
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketDccp => boxed(DccpWebSocketClientTransport::connect(to)),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketSctp => boxed(SctpWebSocketClientTransport::connect(to)),
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

async fn start_all(
    protocol: Arc<RpcServer>,
) -> (RunningServer, CertificateDer<'static>, Arc<RpcServer>) {
    let (certificate, key) = identity();
    let builder = MultiTransportServer::builder(protocol.clone() as Arc<dyn JsonRpcProtocol>)
        .set_bind([LOCALHOST])
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
    #[cfg(feature = "coap")]
    let builder = builder.set_coap_port(0);
    #[cfg(all(feature = "websocket", feature = "coap"))]
    let builder = builder.set_ws_coap_port(0);
    #[cfg(feature = "srt")]
    let builder = builder
        .set_srt_config(SrtConfig {
            passphrase: "test-passphrase".to_owned(),
        })
        .set_srt_port(0)
        .set_ws_srt_port(0);
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    let builder = builder.set_udplite_port(0).set_ws_udplite_port(0);
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let builder = builder
        .set_dccp_port(0)
        .set_sctp_port(0)
        .set_ws_dccp_port(0)
        .set_ws_sctp_port(0);
    let running = builder
        .set_tls(vec![certificate.clone()], key)
        .start()
        .await
        .unwrap();
    (running, certificate, protocol)
}

#[tokio::test]
async fn black_box_parallel_matrix_handles_all_transports() {
    let (running, certificate, protocol) = start_all(make_server()).await;
    let health = running.health_handle(protocol.clone());
    let tasks = running
        .listeners()
        .map(|(kind, address)| {
            let certificate = certificate.clone();
            async move {
                let transport = connect(kind, address, &certificate).await;
                let client = DynClient::new(transport.clone());
                let mut calls = Vec::new();
                for index in 0..6 {
                    let client = client.clone();
                    calls.push(tokio::spawn(async move {
                        let payload = json!({"transport": kind.id(), "index": index});
                        let response: Value =
                            client.request("echo", payload.clone()).await.unwrap();
                        assert_eq!(response, payload);
                    }));
                }
                for call in calls {
                    call.await.unwrap();
                }
                client.close().await.unwrap();
            }
        })
        .collect::<Vec<_>>();
    futures_util::future::join_all(tasks).await;
    assert!(health.snapshot().server.rpc_calls_total >= 48);
    let health_json: Value = serde_json::from_str(&health.json().unwrap()).unwrap();
    assert!(health_json["server"]["rpc_calls_total"].as_u64().unwrap() >= 48);
    let health_prometheus = health.prometheus();
    assert!(health_prometheus.contains("json_rpc_2_server_rpc_calls_total"));
    running.close().await.unwrap();
}

#[tokio::test]
async fn black_box_reconnect_under_load_recovers_every_transport() {
    let (running, certificate, _protocol) = start_all(make_server()).await;
    let tasks = running
        .listeners()
        .map(|(kind, address)| {
            let certificate = certificate.clone();
            async move {
                let transport = connect(kind, address, &certificate).await;
                let client = DynClient::new(transport.clone());
                let worker = {
                    let client = client.clone();
                    tokio::spawn(async move {
                        let mut successes = 0usize;
                        for index in 0..6 {
                            let result = client
                                .request_value(
                                    "delay_echo",
                                    Some(json!({"value": index, "delay_ms": 20})),
                                )
                                .await;
                            if result.is_ok() {
                                successes += 1;
                            }
                        }
                        successes
                    })
                };
                tokio::time::sleep(Duration::from_millis(35)).await;
                transport.reconnect().await.unwrap();
                let successes = worker.await.unwrap();
                assert!(
                    successes > 0,
                    "{kind:?} lost every in-flight call during reconnect"
                );
                let payload = json!({"transport": kind.id(), "phase": "after-reconnect"});
                let after: Value = match client.request("echo", payload.clone()).await {
                    Ok(response) => response,
                    Err(_) => {
                        transport.reconnect().await.unwrap();
                        client.request("echo", payload.clone()).await.unwrap()
                    }
                };
                assert_eq!(after["phase"], "after-reconnect");
                client.close().await.unwrap();
            }
        })
        .collect::<Vec<_>>();
    futures_util::future::join_all(tasks).await;
    running.close().await.unwrap();
}

#[tokio::test]
async fn black_box_graceful_shutdown_finishes_inflight_requests() {
    let (running, certificate, protocol) = start_all(make_server()).await;
    let accepted = Arc::new(tokio::sync::Barrier::new(running.listeners().count() + 1));
    let release = Arc::new(tokio::sync::Notify::new());
    let handlers_accepted = Arc::clone(&accepted);
    let handlers_release = Arc::clone(&release);
    protocol
        .register("shutdown_echo", move |params| {
            let accepted = Arc::clone(&handlers_accepted);
            let release = Arc::clone(&handlers_release);
            async move {
                let released = release.notified();
                tokio::pin!(released);
                released.as_mut().enable();
                accepted.wait().await;
                released.await;
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok(params["value"].clone())
            }
        })
        .unwrap();
    let clients = running
        .listeners()
        .map(|(kind, address)| {
            let certificate = certificate.clone();
            async move {
                let transport = connect(kind, address, &certificate).await;
                (kind, DynClient::new(transport))
            }
        })
        .collect::<Vec<_>>();
    let clients = futures_util::future::join_all(clients).await;

    let requests = clients
        .iter()
        .map(|(kind, client)| {
            let client = client.clone();
            let kind = *kind;
            tokio::spawn(async move {
                let response: Value = client
                    .request("shutdown_echo", json!({"value": kind.id()}))
                    .await
                    .unwrap_or_else(|error| {
                        panic!("{} graceful response failed: {error}", kind.id())
                    });
                assert_eq!(response, json!(kind.id()));
            })
        })
        .collect::<Vec<_>>();

    tokio::time::timeout(Duration::from_secs(10), accepted.wait())
        .await
        .expect("not all transports entered the request handler");
    release.notify_waiters();
    running.close().await.unwrap();

    for request in requests {
        request.await.unwrap();
    }
    for (_, client) in clients {
        let _ = client.close().await;
    }
}
