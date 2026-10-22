#![cfg(feature = "srt")]

use std::{
    io,
    net::{SocketAddr, UdpSocket},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use json_rpc_2_core::{MAX_MESSAGE_BYTES, SrtConfig, UdpReliabilityConfig};
use json_rpc_2_datagram::{
    client::transports::srt::SrtClientTransport, server::transports::srt::SrtTransport,
};
use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, ProtocolFuture, RpcClientTransport, Transport,
    TransportRuntime, expects_response,
};

const PASSWORD: &str = "interop-shared-secret";

fn config() -> SrtConfig {
    SrtConfig {
        passphrase: PASSWORD.into(),
    }
}
fn localhost() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipv4_and_ipv6_listeners_can_share_a_port_without_cross_family_bind_conflicts() {
    let ipv4 = SrtTransport::bind("0.0.0.0:0".parse().unwrap(), config())
        .await
        .unwrap();
    let port = ipv4.local_addr().unwrap().port();
    let ipv6 = SrtTransport::bind(format!("[::]:{port}").parse().unwrap(), config())
        .await
        .unwrap();
    let running4 = ipv4.start(Arc::new(Echo::default()));
    let running6 = ipv6.start(Arc::new(Echo::default()));
    for address in [format!("127.0.0.1:{port}"), format!("[::1]:{port}")] {
        let client = client(address.parse().unwrap()).await;
        client.transport_ping().await.unwrap();
        client.close().await.unwrap();
    }
    running4.close().await.unwrap();
    running6.close().await.unwrap();
}

#[derive(Default)]
struct Echo(AtomicUsize);

impl JsonRpcProtocol for Echo {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::Relaxed);
            if input.windows(4).any(|part| part == b"slow") {
                tokio::time::sleep(Duration::from_millis(350)).await;
            }
            expects_response(input).then(|| Bytes::copy_from_slice(input))
        })
    }
}

async fn client(address: SocketAddr) -> SrtClientTransport {
    let reliability = UdpReliabilityConfig {
        retransmit_interval: Duration::from_secs(1),
        ..Default::default()
    };
    SrtClientTransport::connect_with_config(address, reliability, config())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_id_selects_before_session_open_and_survives_reconnect() {
    use json_rpc_2_datagram::client::transports::srt::{RAW_SRT_STREAM_ID, WS_SRT_STREAM_ID};
    use json_rpc_2_transport::TransportContext;

    struct Selected {
        opened: tokio::sync::mpsc::UnboundedSender<&'static str>,
        name: &'static str,
    }
    impl JsonRpcProtocol for Selected {
        fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
            Box::pin(async move { Some(Bytes::copy_from_slice(input)) })
        }
        fn session_opened(&self, _context: &TransportContext) {
            self.opened.send(self.name).unwrap();
        }
    }
    let (opened, mut events) = tokio::sync::mpsc::unbounded_channel();
    let raw: Arc<dyn JsonRpcProtocol> = Arc::new(Selected {
        opened: opened.clone(),
        name: "raw",
    });
    let ws: Arc<dyn JsonRpcProtocol> = Arc::new(Selected { opened, name: "ws" });
    let listener = SrtTransport::bind(localhost(), config())
        .await
        .unwrap()
        .with_protocol_selector(move |id| match id {
            "" | RAW_SRT_STREAM_ID => Ok(raw.clone()),
            WS_SRT_STREAM_ID => Ok(ws.clone()),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported SRT profile",
            )),
        });
    let address = listener.local_addr().unwrap();
    // This fallback must never receive session_opened or handle_message.
    let fallback = Arc::new(Echo::default());
    let running = listener.start(fallback.clone());
    let mut clients = Vec::new();
    for (id, name) in [
        ("", "raw"),
        (RAW_SRT_STREAM_ID, "raw"),
        (WS_SRT_STREAM_ID, "ws"),
    ] {
        let client = SrtClientTransport::connect_with_stream_id(
            address,
            UdpReliabilityConfig {
                retransmit_interval: Duration::from_secs(1),
                ..Default::default()
            },
            config(),
            id,
        )
        .await
        .unwrap_or_else(|error| panic!("SRT profile {id:?} failed: {error}"));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap(),
            Some(name)
        );
        client.transport_ping().await.unwrap();
        client.reconnect().await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap(),
            Some(name)
        );
        client.transport_ping().await.unwrap();
        clients.push(client);
    }
    // Selection rejects after native accept but before any protocol lifecycle callback.
    if let Ok(client) = SrtClientTransport::connect_with_stream_id(
        address,
        UdpReliabilityConfig::default(),
        config(),
        "unknown-profile",
    )
    .await
    {
        assert!(client.transport_ping().await.is_err());
        client.close().await.unwrap();
    }
    assert!(events.try_recv().is_err());
    assert_eq!(fallback.0.load(Ordering::Relaxed), 0);
    for invalid in ["x".repeat(513), "bad\0id".into()] {
        assert!(
            SrtClientTransport::connect_with_stream_id(
                address,
                UdpReliabilityConfig::default(),
                config(),
                invalid,
            )
            .await
            .is_err()
        );
    }
    for client in clients {
        client.close().await.unwrap();
    }
    running.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_rpc_notifications_batches_fragmentation_ping_and_reconnect() {
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let echo = Arc::new(Echo::default());
    let running = listener.start(echo.clone());
    let client = client(address).await;
    let rpc = Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#);
    assert_eq!(client.send(rpc.clone()).await.unwrap(), Some(rpc));
    assert_eq!(
        client
            .send(Bytes::from_static(
                br#"{"jsonrpc":"2.0","method":"notify"}"#
            ))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        client
            .send(Bytes::from_static(
                br#"[{"method":"notify"},{"method":"notify"}]"#
            ))
            .await
            .unwrap(),
        None
    );
    let batch = Bytes::from_static(br#"[{"method":"notify"},{"method":"echo","id":2}]"#);
    assert_eq!(client.send(batch.clone()).await.unwrap(), Some(batch));
    let large = Bytes::from(format!(
        r#"{{"method":"echo","id":3,"params":"{}"}}"#,
        "x".repeat(80_000)
    ));
    assert_eq!(client.send(large.clone()).await.unwrap(), Some(large));
    let before_ping = echo.0.load(Ordering::Relaxed);
    client.transport_ping().await.unwrap();
    assert_eq!(echo.0.load(Ordering::Relaxed), before_ping);
    client.reconnect().await.unwrap();
    client.transport_ping().await.unwrap();
    client.close().await.unwrap();
    client.close().await.unwrap();
    assert!(client.send(Bytes::from_static(b"{}")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), running.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_and_dropped_futures_do_not_cross_deliver_late_responses() {
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let running = listener.start(Arc::new(Echo::default()));
    let client = client(address).await;
    let cancel = CancellationToken::new();
    let request = client.send_cancelable(
        Bytes::from_static(br#"{"method":"slow","id":1}"#),
        true,
        cancel.clone(),
    );
    tokio::pin!(request);
    tokio::select! {
        result = &mut request => panic!("request completed before cancellation: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(200)) => cancel.cancel(),
    }
    assert!(request.await.unwrap_err().to_string().contains("cancelled"));
    {
        let dropped = client.send(Bytes::from_static(br#"{"method":"slow","id":2}"#));
        tokio::pin!(dropped);
        tokio::select! {
            result = &mut dropped => panic!("slow request unexpectedly completed: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {},
        }
    }
    let fresh = Bytes::from_static(br#"{"method":"echo","id":99}"#);
    assert_eq!(client.send(fresh.clone()).await.unwrap(), Some(fresh));
    client.close().await.unwrap();
    running.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passphrase_errors_and_handshake_rejection_are_explicit_and_bounded() {
    let error = SrtTransport::bind(
        localhost(),
        SrtConfig {
            passphrase: "short".into(),
        },
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("10..=79"));
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let running = listener.start(Arc::new(Echo::default()));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        SrtClientTransport::connect(address, "wrong-but-valid-secret"),
    )
    .await
    .unwrap();
    assert!(
        result.is_err(),
        "mismatched encryption keys must reject the handshake"
    );
    running.close().await.unwrap();
}

struct Never(tokio::sync::Notify);
impl JsonRpcProtocol for Never {
    fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_aborts_stalled_handlers_and_oversize_send_does_not_break_connection() {
    let mut runtime = TransportRuntime::default();
    runtime.shutdown.close_timeout = Duration::from_millis(50);
    let listener = SrtTransport::bind(localhost(), config())
        .await
        .unwrap()
        .with_runtime(Arc::new(runtime));
    let address = listener.local_addr().unwrap();
    let protocol = Arc::new(Never(tokio::sync::Notify::new()));
    let running = listener.start(protocol.clone());
    let client = Arc::new(client(address).await);
    let error = client
        .send_with_expectation(Bytes::from(vec![0; MAX_MESSAGE_BYTES + 1]), true)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("MAX_MESSAGE_BYTES"));
    client.transport_ping().await.unwrap();
    let sender = client.clone();
    let pending = tokio::spawn(async move {
        sender
            .send(Bytes::from_static(br#"{"method":"hang","id":1}"#))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), protocol.0.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), running.close())
        .await
        .unwrap()
        .unwrap();
    client.close().await.unwrap();
    assert!(pending.await.unwrap().is_err());
}

// This peer uses the native C message API directly, independently of the adapter.
// It implements the documented application frame, not any private SRT handshake.
struct NativePeer(libsrt_sys::SRTSOCKET);
impl Drop for NativePeer {
    fn drop(&mut self) {
        // SAFETY: uniquely owned valid handle.
        unsafe {
            libsrt_sys::srt_close(self.0);
        }
    }
}
impl NativePeer {
    fn connect(address: SocketAddr) -> Self {
        use libsrt_sys as sys;
        // SAFETY: test server initialization has completed; new handle is owned here.
        let socket = Self(unsafe { sys::srt_create_socket() });
        assert_ne!(socket.0, sys::SRT_INVALID_SOCK);
        fn set<T>(socket: &NativePeer, option: sys::SRT_SOCKOPT, value: &T) {
            // SAFETY: callers provide documented scalar option types and initialized storage.
            assert_eq!(
                unsafe {
                    sys::srt_setsockflag(
                        socket.0,
                        option,
                        (value as *const T).cast(),
                        std::mem::size_of::<T>() as i32,
                    )
                },
                0
            );
        }
        set(&socket, sys::SRT_SOCKOPT_SRTO_RCVTIMEO, &2000i32);
        set(&socket, sys::SRT_SOCKOPT_SRTO_SNDTIMEO, &2000i32);
        set(&socket, sys::SRT_SOCKOPT_SRTO_PBKEYLEN, &32i32);
        set(&socket, sys::SRT_SOCKOPT_SRTO_TLPKTDROP, &false);
        // SAFETY: bytes are valid for the documented passphrase length.
        assert_eq!(
            unsafe {
                sys::srt_setsockflag(
                    socket.0,
                    sys::SRT_SOCKOPT_SRTO_PASSPHRASE,
                    PASSWORD.as_ptr().cast(),
                    PASSWORD.len() as i32,
                )
            },
            0
        );
        let address = socket2::SockAddr::from(address);
        // SAFETY: initialized sockaddr with its exact length; blocking caller handshake.
        assert_eq!(
            unsafe { sys::srt_connect(socket.0, address.as_ptr().cast(), address.len() as i32) },
            0
        );
        socket
    }
    fn send(&self, bytes: &[u8]) {
        // SAFETY: initialized input, owned connected socket, message <= live payload size.
        assert_eq!(
            unsafe {
                libsrt_sys::srt_sendmsg(self.0, bytes.as_ptr().cast(), bytes.len() as i32, -1, 1)
            },
            bytes.len() as i32
        );
    }
    fn recv(&self) -> Vec<u8> {
        let mut buffer = vec![0; 1316];
        // SAFETY: writable buffer with the specified capacity.
        let size = unsafe {
            libsrt_sys::srt_recvmsg(self.0, buffer.as_mut_ptr().cast(), buffer.len() as i32)
        };
        assert!(size > 0, "native peer failed to receive a message: {size}");
        buffer.truncate(size as usize);
        buffer
    }
}

fn header(kind: u8, id: u64, len: usize) -> Vec<u8> {
    let mut result = b"JRPC\x01".to_vec();
    result.push(kind);
    result.extend_from_slice(&id.to_be_bytes());
    result.extend_from_slice(&(len as u32).to_be_bytes());
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_native_c_caller_exchanges_rpc_and_control_with_listener() {
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let running = listener.start(Arc::new(Echo::default()));
    tokio::task::spawn_blocking(move || {
        let peer = NativePeer::connect(address);
        let rpc = br#"{"jsonrpc":"2.0","method":"echo","id":7}"#;
        peer.send(&header(1, 41, rpc.len()));
        peer.send(rpc);
        assert_eq!(peer.recv(), header(2, 41, rpc.len()));
        assert_eq!(peer.recv(), rpc);
        peer.send(&header(4, 42, 0));
        assert_eq!(peer.recv(), header(5, 42, 0));
    })
    .await
    .unwrap();
    running.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operation_timeout_never_replays_and_late_response_is_discarded() {
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let echo = Arc::new(Echo::default());
    let running = listener.start(echo.clone());
    let client = SrtClientTransport::connect_with_config(
        address,
        UdpReliabilityConfig {
            retransmit_interval: Duration::from_millis(400),
            max_retransmits: 0,
            ..Default::default()
        },
        config(),
    )
    .await
    .unwrap();
    let error = client
        .send(Bytes::from_static(br#"{"method":"slow","id":1}"#))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    let fresh = Bytes::from_static(br#"{"method":"echo","id":2}"#);
    assert_eq!(client.send(fresh.clone()).await.unwrap(), Some(fresh));
    assert_eq!(
        echo.0.load(Ordering::Relaxed),
        2,
        "timed-out request was replayed"
    );
    client.close().await.unwrap();
    running.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_native_frame_only_closes_its_session() {
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let echo = Arc::new(Echo::default());
    let running = listener.start(echo.clone());
    tokio::task::spawn_blocking(move || {
        let peer = NativePeer::connect(address);
        peer.send(&header(1, 1, MAX_MESSAGE_BYTES + 1));
        let mut buffer = [0u8; 1316];
        // SAFETY: owned socket, writable output capacity. The server must close
        // this invalid session rather than invoking the application handler.
        let result = unsafe {
            libsrt_sys::srt_recvmsg(peer.0, buffer.as_mut_ptr().cast(), buffer.len() as i32)
        };
        assert!(result < 0);
    })
    .await
    .unwrap();
    assert_eq!(echo.0.load(Ordering::Relaxed), 0);
    let client = client(address).await;
    client.transport_ping().await.unwrap();
    client.close().await.unwrap();
    running.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_limit_rejects_excess_sessions_and_releases_metrics_on_close() {
    let mut runtime = TransportRuntime::default();
    runtime.limits.max_connections = 1;
    let runtime = Arc::new(runtime);
    let listener = SrtTransport::bind(localhost(), config())
        .await
        .unwrap()
        .with_runtime(runtime.clone());
    let address = listener.local_addr().unwrap();
    let running = listener.start(Arc::new(Echo::default()));
    let first = client(address).await;
    first.transport_ping().await.unwrap();
    assert_eq!(runtime.metrics.snapshot().active_sessions, 1);
    if let Ok(excess) = SrtClientTransport::connect(address, PASSWORD).await {
        assert!(excess.transport_ping().await.is_err());
        excess.close().await.unwrap();
    }
    first.transport_ping().await.unwrap();
    first.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.metrics.snapshot().active_sessions != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    running.close().await.unwrap();
    assert_eq!(runtime.metrics.snapshot().active_streams, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_abandoned_call_does_not_serialize_echo_with_300ms_deadline() {
    struct Concurrent {
        started: tokio::sync::Notify,
        completed: AtomicUsize,
    }
    impl JsonRpcProtocol for Concurrent {
        fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
            Box::pin(async move {
                if input == b"slow" {
                    self.started.notify_one();
                    tokio::time::sleep(Duration::from_millis(800)).await;
                    self.completed.fetch_add(1, Ordering::Relaxed);
                }
                Some(Bytes::copy_from_slice(input))
            })
        }
    }
    let protocol = Arc::new(Concurrent {
        started: tokio::sync::Notify::new(),
        completed: AtomicUsize::new(0),
    });
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let running = listener.start(protocol.clone());
    let client = Arc::new(
        SrtClientTransport::connect(address, PASSWORD)
            .await
            .unwrap(),
    );
    let sender = client.clone();
    let abandoned = tokio::spawn(async move {
        sender
            .send_with_expectation(Bytes::from_static(b"slow"), true)
            .await
    });
    tokio::time::timeout(Duration::from_millis(700), protocol.started.notified())
        .await
        .unwrap();
    abandoned.abort();
    assert!(abandoned.await.unwrap_err().is_cancelled());
    for _ in 0..2 {
        let echoed = tokio::time::timeout(
            Duration::from_millis(300),
            client.send_with_expectation(Bytes::from_static(b"echo"), true),
        )
        .await
        .expect("echo blocked behind abandoned slow call")
        .unwrap();
        assert_eq!(echoed, Some(Bytes::from_static(b"echo")));
    }
    assert_eq!(
        protocol.completed.load(Ordering::Relaxed),
        0,
        "echo only completed after slow handler finished"
    );
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_millis(1600), running.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_delivers_inflight_response_before_native_close() {
    struct Gated {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    impl JsonRpcProtocol for Gated {
        fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                Some(Bytes::copy_from_slice(input))
            })
        }
    }
    let protocol = Arc::new(Gated {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let listener = SrtTransport::bind(localhost(), config()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(listener.run(protocol.clone(), shutdown.clone()));
    let client = Arc::new(client(address).await);
    let payload = Bytes::from(vec![b'x'; 80_000]);
    let expected = payload.clone();
    let sender = client.clone();
    let request = tokio::spawn(async move { sender.send_with_expectation(payload, true).await });
    tokio::time::timeout(Duration::from_secs(2), protocol.entered.notified())
        .await
        .unwrap();
    shutdown.cancel();
    protocol.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(request.await.unwrap().unwrap(), Some(expected));
    client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_deadline_bounds_native_peer_that_never_acknowledges_drain() {
    let mut runtime = TransportRuntime::default();
    runtime.shutdown.close_timeout = Duration::from_millis(200);
    let listener = SrtTransport::bind(localhost(), config())
        .await
        .unwrap()
        .with_runtime(Arc::new(runtime));
    let address = listener.local_addr().unwrap();
    let running = listener.start(Arc::new(Echo::default()));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let peer = NativePeer::connect(address);
        peer.send(&header(4, 1, 0));
        assert_eq!(peer.recv(), header(5, 1, 0));
        ready_tx.send(()).unwrap();
        // Hold the native socket open, without reading or acknowledging Drain.
        release_rx.recv().unwrap();
    });
    ready_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), running.close())
        .await
        .unwrap()
        .unwrap();
    release_tx.send(()).unwrap();
    peer.await.unwrap();
}

struct External(Child);
impl Drop for External {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn srt_live_transmit_receives_encrypted_standard_srt_messages() -> io::Result<()> {
    let executable = std::env::var("SRT_LIVE_TRANSMIT").unwrap_or_else(|_| {
        if std::path::Path::new("/opt/homebrew/bin/srt-live-transmit").exists() {
            "/opt/homebrew/bin/srt-live-transmit".into()
        } else {
            "srt-live-transmit".into()
        }
    });
    match Command::new(&executable)
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            eprintln!(
                "skipping external CLI test: install srt-live-transmit or set SRT_LIVE_TRANSMIT"
            );
            return Ok(());
        }
        result => {
            assert!(result?.success(), "srt-live-transmit cannot run");
        }
    }
    let capture = tokio::net::UdpSocket::bind(localhost()).await?;
    let reservation = UdpSocket::bind(localhost())?;
    let port = reservation.local_addr()?.port();
    drop(reservation);
    let source = format!(
        "srt://127.0.0.1:{port}?mode=listener&passphrase={PASSWORD}&pbkeylen=32&transtype=live"
    );
    let destination = format!("udp://{}", capture.local_addr()?);
    let mut external = External(
        Command::new(executable)
            .args(["-q", "-t:8", &source, &destination])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        external.0.try_wait()?.is_none(),
        "external SRT listener failed to start"
    );
    let client = client(format!("127.0.0.1:{port}").parse().unwrap()).await;
    let ping = client.transport_ping();
    tokio::pin!(ping);
    let mut buffer = [0u8; 1500];
    let received = tokio::select! {
        result = &mut ping => panic!("peer has not sent a pong: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(3), capture.recv(&mut buffer)) => result.map_err(io::Error::other)??,
    };
    assert_eq!(&buffer[..received], header(4, 1, 0));
    client.close().await.map_err(io::Error::other)?;
    Ok(())
}
