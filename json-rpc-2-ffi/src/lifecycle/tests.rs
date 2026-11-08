use super::*;
use std::time::Instant;

fn local_options(port: u16) -> ConnectOptions {
    ConnectOptions {
        host: "127.0.0.1".into(),
        port,
        ca_pem: None,
        server_name: None,
        srt_passphrase: None,
        udplite_send_coverage: None,
        udplite_recv_coverage: None,
        broker_url: None,
        broker_route: None,
        broker_ca_pem: None,
        broker_operation_timeout_ms: None,
        broker_response_timeout_ms: None,
    }
}

/// Accepts one TCP connection and keeps it open without ever answering.
fn silent_server() -> (u16, std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, stopped) = std::sync::mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        let _ = stopped.recv_timeout(Duration::from_secs(10));
    });
    (port, stop, server)
}

#[cfg(feature = "tcp")]
#[test]
fn bounded_ping_and_restore_return_within_rust_deadline_on_silent_peer() {
    let (port, stop, server) = silent_server();
    let client = RpcClient::connect(FfiTransportKind::Tcp, local_options(port)).unwrap();

    let started = Instant::now();
    let ping = client.transport_ping_with_timeout(100).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(2), "ping not bounded");
    assert!(
        matches!(&ping, FfiError::Transport { details } if details.contains("deadline exceeded after 100 ms")),
        "{ping:?}"
    );

    let started = Instant::now();
    let restore = client.restore_connection(100).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(2), "restore not bounded");
    assert!(matches!(restore, FfiError::Transport { .. }), "{restore:?}");

    assert!(matches!(
        client.transport_ping_with_timeout(0),
        Err(FfiError::InvalidConfiguration { .. })
    ));
    assert!(matches!(
        client.restore_connection(0),
        Err(FfiError::InvalidConfiguration { .. })
    ));
    let _ = client.disconnect();
    stop.send(()).unwrap();
    server.join().unwrap();
}

#[cfg(all(feature = "tls", any(feature = "quic", feature = "http3")))]
#[test]
fn builder_connect_timeout_bounds_a_stalled_handshake() {
    let (port, stop, server) = silent_server();
    let ca = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .unwrap()
        .cert
        .pem();
    let mut options = local_options(port);
    options.ca_pem = Some(ca);
    options.server_name = Some("localhost".into());
    let builder = RpcClientBuilder::new(FfiTransportKind::Tls, options);
    assert!(matches!(
        builder.set_connect_timeout_ms(0),
        Err(FfiError::InvalidConfiguration { .. })
    ));
    builder.set_connect_timeout_ms(150).unwrap();

    let started = Instant::now();
    let error = builder.connect().err().expect("stalled TLS handshake must fail");
    assert!(started.elapsed() < Duration::from_secs(3), "connect not bounded");
    assert!(
        matches!(&error, FfiError::Transport { details } if details.contains("connect deadline exceeded after 150 ms")),
        "{error:?}"
    );
    stop.send(()).unwrap();
    server.join().unwrap();
}
