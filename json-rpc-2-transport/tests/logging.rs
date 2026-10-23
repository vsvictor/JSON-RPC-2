use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use json_rpc_2_transport::{log_control, log_json_rpc};
use tracing::Level;

#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn capture(level: Level, run: impl FnOnce()) -> String {
    let output = Output::default();
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, run);
    let bytes = output.0.lock().unwrap().clone();
    String::from_utf8(bytes).unwrap()
}

#[test]
fn debug_logs_full_request_and_response_payloads() {
    let request = br#"{"jsonrpc":"2.0","method":"echo","params":["hello"],"id":1}"#;
    let response = br#"{"jsonrpc":"2.0","result":["hello"],"id":1}"#;
    let notification = br#"{"jsonrpc":"2.0","method":"notify","params":["notice"]}"#;
    let error = br#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}"#;
    let batch = br#"[{"jsonrpc":"2.0","method":"echo","id":2}]"#;
    let batch_response = br#"[{"jsonrpc":"2.0","result":null,"id":2}]"#;
    let output = capture(Level::DEBUG, || {
        for (request, response) in [
            (request.as_slice(), Some(response.as_slice())),
            (notification.as_slice(), None),
            (b"{invalid".as_slice(), Some(error.as_slice())),
            (batch.as_slice(), Some(batch_response.as_slice())),
        ] {
            log_json_rpc("TCP", "127.0.0.1:1234", request, response, Duration::ZERO);
        }
        log_control("TCP", "127.0.0.1:1234", "ping", "pong");
    });
    for payload in [
        request.as_slice(),
        response,
        notification,
        error,
        batch,
        batch_response,
    ] {
        assert!(
            output.contains(std::str::from_utf8(payload).unwrap()),
            "{output}"
        );
    }
    assert!(output.contains("request_direction=\"peer_to_server\""));
    assert!(output.contains("response_direction=\"server_to_peer\""));
    assert!(output.contains(&format!("request_bytes={}", request.len())));
    assert!(output.contains(&format!("response_bytes={}", response.len())));
    assert!(output.contains("notification=true"));
    assert!(output.contains("notification=false"));
    assert!(output.contains("handled control traffic"));
    assert!(output.contains("ping"));
    assert!(output.contains("pong"));
}

#[test]
fn debug_logs_large_payloads() {
    let payload = format!(
        r#"{{"jsonrpc":"2.0","method":"echo","params":["{}"],"id":1}}"#,
        "x".repeat(32_768)
    );
    let output = capture(Level::DEBUG, || {
        log_json_rpc(
            "UDP",
            "peer",
            payload.as_bytes(),
            Some(payload.as_bytes()),
            Duration::ZERO,
        );
    });
    assert!(output.contains(&payload));
    assert!(output.contains(&format!("request_bytes={}", payload.len())));
    assert!(output.contains(&format!("response_bytes={}", payload.len())));
}

#[test]
fn info_does_not_log_debug_payloads_or_control_events() {
    let output = capture(Level::INFO, || {
        log_json_rpc(
            "TCP",
            "peer",
            b"private request",
            Some(b"private response"),
            Duration::ZERO,
        );
        log_control("TCP", "peer", "ping", "pong");
    });
    assert!(output.is_empty(), "{output}");
}
