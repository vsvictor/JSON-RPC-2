use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};

#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2::client::transports::dccp::DccpClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2::client::transports::sctp::SctpClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2::client::transports::udplite::UdpLiteClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2::client::transports::websocket::dccp::DccpWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2::client::transports::websocket::sctp::SctpWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2::client::transports::websocket::udplite::UdpLiteWebSocketClientTransport;
use json_rpc_2::{
    JsonRpcProtocol, MultiTransportServer, RpcServer, RunningServer, TransportCapabilities,
    TransportKind as Kind,
    client::{
        ClientLivenessConfig, RpcClient, RpcClientConfig, RpcClientTransport,
        TransportLimits as ClientTransportLimits,
        transports::{
            amqp::AmqpClientTransport,
            coap::CoapClientTransport,
            http1::Http1ClientTransport,
            http3::Http3ClientTransport,
            mqtt::MqttClientTransport,
            quic::QuicClientTransport,
            rtp::RtpClientTransport,
            rudp::RudpClientTransport,
            srt::SrtClientTransport,
            tcp::TcpClientTransport,
            tls::TlsClientTransport,
            udp::UdpClientTransport,
            websocket::{
                amqp::AmqpWebSocketClientTransport, coap::CoapWebSocketClientTransport,
                http1::Http1WebSocketClientTransport, http3::Http3WebSocketClientTransport,
                mqtt::MqttWebSocketClientTransport, quic::QuicWebSocketClientTransport,
                rtp::RtpWebSocketClientTransport, rudp::RudpWebSocketClientTransport,
                srt::SrtWebSocketClientTransport, tcp::TcpWebSocketClientTransport,
                udp::UdpWebSocketClientTransport,
            },
        },
    },
};
use json_rpc_2_config::{
    CommandConfig, RuntimeOptions, SERVER_HOST, USAGE, load_env, parse_command,
};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde_json::{Value, json};

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

fn client_call_timeout(kind: Kind, options: &RuntimeOptions) -> Duration {
    if kind.is_broker() {
        let (operation, response) = match kind {
            Kind::Mqtt | Kind::WebSocketMqtt => (
                options.mqtt_operation_timeout,
                options.mqtt_response_timeout,
            ),
            Kind::Amqp | Kind::WebSocketAmqp => (
                options.amqp_operation_timeout,
                options.amqp_response_timeout,
            ),
            _ => unreachable!("only broker kinds use broker timeouts"),
        };
        operation
            .saturating_mul(2)
            .saturating_add(response.saturating_mul(2))
    } else {
        CONNECT_TIMEOUT
    }
}

struct Tls {
    certificates: Vec<CertificateDer<'static>>,
    key: Option<PrivateKeyDer<'static>>,
}

struct Generated {
    tls: Tls,
    certificate_pem: String,
    key_pem: String,
}

impl Tls {
    fn generate() -> AppResult<Generated> {
        let generated = rcgen::generate_simple_self_signed(vec![
            "localhost".into(),
            "127.0.0.1".into(),
            "::1".into(),
        ])?;
        Ok(Generated {
            certificate_pem: generated.cert.pem(),
            key_pem: generated.key_pair.serialize_pem(),
            tls: Tls {
                certificates: vec![generated.cert.into()],
                key: Some(PrivateKeyDer::Pkcs8(
                    generated.key_pair.serialize_der().into(),
                )),
            },
        })
    }

    fn for_server(certificate: &Path, key: Option<&Path>) -> AppResult<Self> {
        let Some(key) = key else {
            let generated = Self::generate()?;
            write_file(certificate, &generated.certificate_pem, false)?;
            println!(
                "TLS: generated a self-signed certificate (in memory), published to {}",
                certificate.display()
            );
            return Ok(generated.tls);
        };
        match (certificate.exists(), key.exists()) {
            (true, true) => {
                let tls = Tls {
                    certificates: read_certificates(certificate)?,
                    key: Some(PrivateKeyDer::from_pem_file(key).map_err(|error| {
                        format!("cannot read private key {}: {error}", key.display())
                    })?),
                };
                println!(
                    "TLS: certificate {}, key {}",
                    certificate.display(),
                    key.display()
                );
                Ok(tls)
            }
            (false, false) => {
                let generated = Self::generate()?;
                write_file(certificate, &generated.certificate_pem, false)?;
                write_file(key, &generated.key_pem, true)?;
                println!(
                    "TLS: generated a self-signed certificate {} and key {}",
                    certificate.display(),
                    key.display()
                );
                Ok(generated.tls)
            }
            (true, false) => Err(format!("private key {} not found", key.display()).into()),
            (false, true) => Err(format!("certificate {} not found", certificate.display()).into()),
        }
    }

    fn for_client(path: &Path) -> AppResult<Self> {
        Ok(Tls {
            certificates: read_certificates(path)?,
            key: None,
        })
    }

    fn client(&self, ip: IpAddr, alpn: &[u8]) -> AppResult<quinn::Endpoint> {
        let mut roots = rustls::RootCertStore::empty();
        for certificate in &self.certificates {
            roots.add(certificate.clone())?;
        }
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![alpn.to_vec()];
        let mut endpoint = quinn::Endpoint::client(SocketAddr::new(ip, 0))?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(tls)?,
        )));
        Ok(endpoint)
    }
}

fn read_certificates(path: &Path) -> AppResult<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_file_iter(path)
        .and_then(|certificates| certificates.collect::<Result<Vec<_>, _>>())
        .map_err(|error| format!("cannot read certificate {}: {error}", path.display()))?;
    if certificates.is_empty() {
        return Err(format!("no certificate in {}", path.display()).into());
    }
    Ok(certificates)
}

fn write_file(path: &Path, contents: &str, private: bool) -> AppResult<()> {
    if let Some(directory) = path
        .parent()
        .filter(|directory| !directory.as_os_str().is_empty())
    {
        std::fs::create_dir_all(directory)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    std::io::Write::write_all(&mut options.open(path)?, contents.as_bytes())?;
    Ok(())
}

fn rpc_server() -> AppResult<Arc<dyn JsonRpcProtocol>> {
    Ok(Arc::new(
        RpcServer::builder()
            .register("echo", |params| async move { Ok(params) })?
            .build(),
    ))
}

fn configured_server(
    options: &RuntimeOptions,
    tls: &Tls,
    rpc: Arc<dyn JsonRpcProtocol>,
) -> AppResult<MultiTransportServer> {
    if let Some(kind) = options.transports.iter().find(|kind| kind.is_iot()) {
        return Err(format!(
            "{} server transport is provided by gateway-iot",
            kind.name()
        )
        .into());
    }
    let mut server = MultiTransportServer::new(rpc);
    server
        .set_bind(options.bind.iter().copied())
        .set_transport_limits(options.server_limits.clone())
        .set_tcp_tuning(options.tcp_tuning.clone())
        .set_udp_reliability(options.udp_reliability.clone())
        .set_udplite_config(options.udplite.clone());
    if let Some(srt) = &options.srt {
        server.set_srt_config(srt.clone());
    }
    for kind in options.transports.iter().copied() {
        let port = options.port(kind);
        match kind {
            Kind::Tcp => {
                server.set_tcp_port(port);
            }
            Kind::Tls => {
                server.set_tls_port(port);
            }
            Kind::Udp => {
                server.set_udp_port(port);
            }
            Kind::Rtp => {
                server.set_rtp_port(port);
            }
            Kind::Rudp => {
                server.set_rudp_port(port);
            }
            Kind::Srt => {
                server.set_srt_port(port);
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Kind::UdpLite => {
                server.set_udplite_port(port);
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Kind::Dccp => {
                server.set_dccp_port(port);
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Kind::Sctp => {
                server.set_sctp_port(port);
            }
            Kind::Quic => {
                server.set_quic_port(port);
            }
            Kind::Http1 => {
                server.set_http1_port(port);
            }
            Kind::Http3 => {
                server.set_http3_port(port);
            }
            Kind::WebSocketTcp => {
                server.set_ws_tcp_port(port);
            }
            Kind::WebSocketUdp => {
                server.set_ws_udp_port(port);
            }
            Kind::WebSocketRtp => {
                server.set_ws_rtp_port(port);
            }
            Kind::WebSocketRudp => {
                server.set_ws_rudp_port(port);
            }
            Kind::WebSocketSrt => {
                server.set_ws_srt_port(port);
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Kind::WebSocketUdpLite => {
                server.set_ws_udplite_port(port);
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Kind::WebSocketDccp => {
                server.set_ws_dccp_port(port);
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Kind::WebSocketSctp => {
                server.set_ws_sctp_port(port);
            }
            Kind::WebSocketQuic => {
                server.set_ws_quic_port(port);
            }
            Kind::WebSocketHttp1 => {
                server.set_ws_http1_port(port);
            }
            Kind::WebSocketTls => {
                server.set_ws_tls_port(port);
            }
            Kind::WebSocketHttp3 => {
                server.set_ws_http3_port(port);
            }
            _ => unreachable!("IoT transports are rejected before server configuration"),
        };
    }
    if let Some(key) = &tls.key {
        server.set_tls(tls.certificates.clone(), key.clone_key());
    }
    Ok(server)
}

async fn start_server(server: MultiTransportServer) -> AppResult<RunningServer> {
    let running = server.start().await?;
    println!("== Listening ==");
    for (kind, address) in running.listeners() {
        println!("  {:<19} listening on {address}", kind.name());
    }
    Ok(running)
}

async fn close_all(running: RunningServer) -> AppResult<()> {
    println!("\n== Closing ==");
    let listeners: Vec<_> = running.listeners().collect();
    running.close().await?;
    for (kind, address) in listeners {
        println!("  {} {address}: closed", kind.name());
    }
    Ok(())
}

async fn demonstrate<T: RpcClientTransport>(
    name: &str,
    transport: T,
    params: Value,
    options: &RuntimeOptions,
) -> AppResult<()> {
    let client = RpcClient::new_with_config(
        transport,
        RpcClientConfig {
            request_timeout: options.request_timeout,
            liveness: ClientLivenessConfig {
                ping_frequency: None,
                max_missed_pongs: options.max_missed_pongs,
            },
            reconnect: options.reconnect.clone(),
            limits: ClientTransportLimits::default(),
        },
    )?;
    client.transport_ping().await?;
    let request = json!({
        "jsonrpc": "2.0",
        "method": "echo",
        "params": params,
    });
    let response: Value = client.request("echo", params).await?;
    client.close().await?;
    println!("  {name}:");
    println!("    => transport ping");
    println!("    <= transport pong");
    println!("    -> {request}");
    println!("    <- {response}");
    Ok(())
}

async fn call(
    kind: Kind,
    tls: &Tls,
    to: SocketAddr,
    server_name: Option<&str>,
    options: &RuntimeOptions,
) -> AppResult<()> {
    let local = unspecified(to);
    let server_name = server_name.map_or_else(|| to.ip().to_string(), str::to_owned);
    let params = if kind.is_broker() {
        json!({"transport": kind.id(), "endpoint": "external broker"})
    } else {
        json!({"transport": kind.id(), "ip": to.ip()})
    };
    let label = if kind.is_broker() {
        format!("{} external broker service", kind.name())
    } else {
        format!("{} {to}", kind.name())
    };
    match kind {
        Kind::Tcp => {
            demonstrate(
                &label,
                TcpClientTransport::connect_with_tuning(to, options.tcp_tuning.clone()).await?,
                params,
                options,
            )
            .await
        }
        Kind::Tls => {
            let transport = TlsClientTransport::connect_with_certificates(
                to,
                &server_name,
                &tls.certificates,
                options.tcp_tuning.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Udp => {
            let transport =
                UdpClientTransport::connect_with_config(to, options.udp_reliability.clone())
                    .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Rtp => {
            let transport =
                RtpClientTransport::connect_with_config(to, options.udp_reliability.clone())
                    .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Rudp => {
            let transport =
                RudpClientTransport::connect_with_config(to, options.udp_reliability.clone())
                    .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Srt => {
            let transport = SrtClientTransport::connect_with_config(
                to,
                options.udp_reliability.clone(),
                options
                    .srt
                    .clone()
                    .ok_or("JSON_RPC_SRT_PASSPHRASE is required for srt")?,
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Kind::UdpLite => {
            let transport = UdpLiteClientTransport::connect_with_configs(
                to,
                options.udp_reliability.clone(),
                options.udplite.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Mqtt => {
            let transport =
                MqttClientTransport::connect_with_config(options.mqtt_config(false)?).await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Coap => {
            let transport =
                CoapClientTransport::connect_with_config(to, options.udp_reliability.clone())
                    .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Amqp => {
            let transport =
                AmqpClientTransport::connect_with_config(options.amqp_config(false)).await?;
            demonstrate(&label, transport, params, options).await
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Kind::Dccp => {
            let transport = DccpClientTransport::connect(to).await?;
            demonstrate(&label, transport, params, options).await
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Kind::Sctp => {
            let transport = SctpClientTransport::connect(to).await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Quic => {
            let endpoint = tls.client(local, json_rpc_2::QUIC_ALPN)?;
            let transport = QuicClientTransport::connect(endpoint, to, &server_name).await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::Http1 => {
            demonstrate(
                &label,
                Http1ClientTransport::connect_with_tuning(to, options.tcp_tuning.clone()).await?,
                params,
                options,
            )
            .await
        }
        Kind::Http3 => {
            let endpoint = tls.client(local, json_rpc_2::H3_ALPN)?;
            let transport = Http3ClientTransport::connect(endpoint, to, &server_name).await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketTcp => {
            let transport = TcpWebSocketClientTransport::connect_with_tuning(
                &format!("ws://{to}"),
                options.tcp_tuning.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketUdp => {
            let transport = UdpWebSocketClientTransport::connect_with_config(
                to,
                options.udp_reliability.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketRtp => {
            let transport = RtpWebSocketClientTransport::connect_with_config(
                to,
                options.udp_reliability.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketRudp => {
            let transport = RudpWebSocketClientTransport::connect_with_config(
                to,
                options.udp_reliability.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketSrt => {
            let transport = SrtWebSocketClientTransport::connect_with_config(
                to,
                options.udp_reliability.clone(),
                options
                    .srt
                    .clone()
                    .ok_or("JSON_RPC_SRT_PASSPHRASE is required for ws-srt")?,
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Kind::WebSocketUdpLite => {
            let transport = UdpLiteWebSocketClientTransport::connect_with_configs(
                to,
                options.udp_reliability.clone(),
                options.udplite.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketMqtt => {
            let transport =
                MqttWebSocketClientTransport::connect_with_config(options.mqtt_config(true)?)
                    .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketCoap => {
            let transport = CoapWebSocketClientTransport::connect_with_config(
                to,
                options.udp_reliability.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketAmqp => {
            let transport =
                AmqpWebSocketClientTransport::connect_with_config(options.amqp_config(true))
                    .await?;
            demonstrate(&label, transport, params, options).await
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Kind::WebSocketDccp => {
            let transport = DccpWebSocketClientTransport::connect(to).await?;
            demonstrate(&label, transport, params, options).await
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Kind::WebSocketSctp => {
            let transport = SctpWebSocketClientTransport::connect(to).await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketQuic => {
            let endpoint = tls.client(local, json_rpc_2::WS_QUIC_ALPN)?;
            let transport =
                QuicWebSocketClientTransport::connect(endpoint, to, &server_name).await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketHttp1 => {
            let transport = Http1WebSocketClientTransport::connect_with_tuning(
                &format!("ws://{to}/ws"),
                options.tcp_tuning.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketTls => {
            let ca_path = options.ca.as_deref().unwrap_or(&options.cert);
            let ca_pem = std::fs::read_to_string(ca_path).map_err(|error| {
                format!(
                    "cannot read WebSocket TLS CA {}: {error}",
                    ca_path.display()
                )
            })?;
            let tls_host = if server_name.parse::<Ipv6Addr>().is_ok() {
                format!("[{server_name}]")
            } else {
                server_name.clone()
            };
            let transport = Http1WebSocketClientTransport::connect_with_ca_pem_to_addr(
                &format!("wss://{tls_host}:{}/ws", to.port()),
                to,
                &ca_pem,
                options.tcp_tuning.clone(),
            )
            .await?;
            demonstrate(&label, transport, params, options).await
        }
        Kind::WebSocketHttp3 => {
            let endpoint = tls.client(local, json_rpc_2::H3_ALPN)?;
            let transport =
                Http3WebSocketClientTransport::connect(endpoint, to, &server_name, "/ws").await?;
            demonstrate(&label, transport, params, options).await
        }
    }
}

fn family(ip: IpAddr) -> &'static str {
    if ip.is_ipv6() { "IPv6" } else { "IPv4" }
}

fn unspecified(to: SocketAddr) -> IpAddr {
    if to.is_ipv6() {
        IpAddr::V6(Ipv6Addr::UNSPECIFIED)
    } else {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }
}

async fn ipv6_examples(
    tls: &Tls,
    ip: Ipv6Addr,
    v6: impl Fn(Kind) -> SocketAddr,
    options: &RuntimeOptions,
) -> AppResult<()> {
    println!("\n== IPv6: address written as text ==");
    let text = format!("[{ip}]:{}", v6(Kind::Tcp).port());
    let tcp: SocketAddr = text.parse()?;
    demonstrate(
        &format!("TCP \"{text}\""),
        TcpClientTransport::connect(tcp).await?,
        json!({"example":"parsed IPv6 socket address"}),
        options,
    )
    .await?;

    let url = format!("ws://[{ip}]:{}/ws", v6(Kind::WebSocketHttp1).port());
    demonstrate(
        &format!("WebSocket/HTTP/1.1 \"{url}\""),
        Http1WebSocketClientTransport::connect(&url).await?,
        json!({"example":"IPv6 URL"}),
        options,
    )
    .await?;

    println!("\n== IPv6: the QUIC client must use an IPv6 socket ==");
    let quic = v6(Kind::Quic);
    let ipv4 = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    match QuicClientTransport::connect(tls.client(ipv4, json_rpc_2::QUIC_ALPN)?, quic, SERVER_HOST)
        .await
    {
        Err(error) => println!("  IPv4 client endpoint -> {quic}: refused ({error})"),
        Ok(transport) => {
            transport.close().await?;
            println!("  IPv4 client endpoint -> {quic}: unexpectedly connected");
        }
    }
    Ok(())
}

async fn demo(options: RuntimeOptions) -> AppResult<()> {
    let key = options.key.as_deref().ok_or("JSON_RPC_KEY not set")?;
    if !options.cert.exists() || !key.exists() {
        return Err(format!(
            "{} or {} not found; generate them: tools/gen-cert.sh",
            options.cert.display(),
            key.display()
        )
        .into());
    }
    let tls = Tls::for_server(&options.cert, Some(key))?;
    let mut ips = Vec::new();
    for address in resolve(SERVER_HOST).await? {
        let link_local = matches!(address, SocketAddr::V6(v6) if v6.scope_id() != 0);
        if !link_local && !ips.contains(&address.ip()) {
            ips.push(address.ip());
        }
    }
    if ips.is_empty() {
        return Err(format!("{SERVER_HOST} has no usable address").into());
    }
    let mut server = configured_server(&options, &tls, rpc_server()?)?;
    server.set_bind(ips.iter().copied());
    let listening = start_server(server).await?;
    for &ip in &ips {
        println!(
            "\n== {SERVER_HOST} ({ip}), {}: client per transport ==",
            family(ip)
        );
        for (kind, address) in listening
            .listeners()
            .filter(|(_, address)| address.ip() == ip)
        {
            call(kind, &tls, address, Some(SERVER_HOST), &options).await?;
        }
    }
    match ips.iter().find_map(|ip| match ip {
        IpAddr::V6(v6) => Some(*v6),
        IpAddr::V4(_) => None,
    }) {
        Some(ip) => {
            let v6 = |kind: Kind| {
                listening
                    .address(kind, IpAddr::V6(ip))
                    .expect("every transport listens on every address")
            };
            ipv6_examples(&tls, ip, v6, &options).await?;
        }
        None => println!("\n{SERVER_HOST} has no IPv6 address, IPv6 examples skipped"),
    }
    close_all(listening).await
}

async fn server(options: RuntimeOptions) -> AppResult<()> {
    let uses_tls = options
        .transports
        .iter()
        .any(|kind| kind.security().is_tls());
    let tls = if uses_tls {
        Tls::for_server(&options.cert, options.key.as_deref())?
    } else {
        Tls {
            certificates: Vec::new(),
            key: None,
        }
    };
    let listening = start_server(configured_server(&options, &tls, rpc_server()?)?).await?;
    println!("\nServing, press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    close_all(listening).await
}

async fn resolve(host: &str) -> AppResult<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse() {
        return Ok(vec![SocketAddr::new(ip, 0)]);
    }
    let mut addresses = Vec::new();
    for address in tokio::net::lookup_host((host, 0))
        .await
        .map_err(|error| format!("cannot resolve {host}: {error}"))?
    {
        if !addresses.contains(&address) {
            addresses.push(address);
        }
    }
    Ok(addresses)
}

async fn client(options: RuntimeOptions) -> AppResult<()> {
    let uses_tls = options
        .transports
        .iter()
        .any(|kind| kind.security().is_tls());
    let trusted = options.ca.as_deref().unwrap_or(&options.cert);
    let tls = if uses_tls {
        Tls::for_client(trusted).map_err(|error| {
            format!(
                "{error}; start the server first (cargo run -p json-rpc-2-cli --bin JSON_RPC_2 -- server) or set JSON_RPC_CA"
            )
        })?
    } else {
        Tls {
            certificates: Vec::new(),
            key: None,
        }
    };
    let mut failed = 0;
    for &kind in options.transports.iter().filter(|kind| kind.is_broker()) {
        let to = SocketAddr::from(([127, 0, 0, 1], 0));
        let result = tokio::time::timeout(
            client_call_timeout(kind, &options),
            call(kind, &tls, to, None, &options),
        )
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failed += 1;
                println!("  {} external broker service: failed: {error}", kind.name());
            }
            Err(_) => {
                failed += 1;
                println!(
                    "  {} external broker service: timed out (broker unavailable, authentication failed, or configured operation/response limit reached)",
                    kind.name()
                );
            }
        }
    }
    for host in &options.hosts {
        let transports: Vec<_> = options
            .transports
            .iter()
            .copied()
            .filter(|kind| !kind.is_broker())
            .collect();
        if transports.is_empty() {
            break;
        }
        let server_name = options.server_name.as_deref().unwrap_or(host);
        for mut to in resolve(host).await? {
            println!(
                "== {host} ({}), {}: client per transport ==",
                to.ip(),
                family(to.ip())
            );
            for &kind in &transports {
                to.set_port(options.port(kind));
                let call = call(kind, &tls, to, Some(server_name), &options);
                let result = match tokio::time::timeout(CONNECT_TIMEOUT, call).await {
                    Ok(result) => result,
                    Err(_) => Err("timed out; is the server running?".into()),
                };
                if let Err(error) = result {
                    failed += 1;
                    println!("  {} {to}: failed: {error}", kind.name());
                }
            }
        }
    }
    if failed > 0 {
        return Err(format!("{failed} call(s) failed").into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    load_env()?;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "json_rpc_2=debug,json_rpc_2_client=debug,json_rpc_2_server=debug".into()
            }),
        )
        .with_target(false)
        .try_init();
    match parse_command(std::env::args().skip(1)) {
        Ok(CommandConfig::Demo(options)) => demo(options).await,
        Ok(CommandConfig::Server(options)) => server(options).await,
        Ok(CommandConfig::Client(options)) => client(options).await,
        Ok(CommandConfig::Help) => {
            println!("{USAGE}");
            Ok(())
        }
        Err(error) => {
            eprintln!("error: {error}\n\n{USAGE}");
            std::process::exit(2);
        }
    }
}
