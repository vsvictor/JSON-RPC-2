use super::*;
use tokio::{net::TcpListener, sync::mpsc};

#[test]
fn encodes_notification_without_id() {
    let text = encode_notification("auth.token", Some(json!({"a": 1}))).unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["jsonrpc"], "2.0");
    assert_eq!(value["method"], "auth.token");
    assert!(value.get("id").is_none());
}

#[test]
fn rejects_reserved_and_scalar_params() {
    assert!(encode_notification("rpc.hidden", None).is_err());
    assert!(encode_notification("", None).is_err());
    assert!(encode_notification("m", Some(json!(5))).is_err());
}

#[test]
fn decode_accepts_notifications_only() {
    let message = decode_notification(br#"{"jsonrpc":"2.0","method":"m","params":{"x":1}}"#).unwrap();
    assert_eq!(message.method, "m");
    assert_eq!(message.params, Some(json!({"x": 1})));
    assert!(decode_notification(br#"{"jsonrpc":"2.0","method":"m","id":1}"#).is_err());
    assert!(decode_notification(br#"{"jsonrpc":"2.0","result":1,"id":1}"#).is_err());
    assert!(decode_notification(br#"{"jsonrpc":"1.0","method":"m"}"#).is_err());
    assert!(decode_notification(br#"{"jsonrpc":"2.0","method":"m","params":3}"#).is_err());
    assert!(decode_notification(b"nonsense").is_err());
}

async fn fake_control(reply: &'static str) -> (String, mpsc::UnboundedReceiver<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (seen, received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut line = String::new();
            BufReader::new(read).read_line(&mut line).await.unwrap();
            seen.send(serde_json::from_str(&line).unwrap()).unwrap();
            write.write_all(reply.as_bytes()).await.unwrap();
            write.write_all(b"\n").await.unwrap();
        }
    });
    (address, received)
}

#[tokio::test]
async fn control_sends_register_and_push_commands() {
    let (address, mut seen) = fake_control(r#"{"success":true}"#).await;
    let control = GatewayControl::new(address);
    control.register_client("conn-1", "user-1").await.unwrap();
    control
        .notify_client("user-1", "auth.passwordResetToken", Some(json!({"t": "x"})))
        .await
        .unwrap();
    let register = seen.recv().await.unwrap();
    assert_eq!(register["action"], "register_client");
    assert_eq!(register["connection_id"], "conn-1");
    assert_eq!(register["client_id"], "user-1");
    let push = seen.recv().await.unwrap();
    assert_eq!(push["action"], "push_unicast");
    assert_eq!(push["client_id"], "user-1");
    let payload: Value = serde_json::from_str(push["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload["method"], "auth.passwordResetToken");
}

#[tokio::test]
async fn control_rejection_is_reported() {
    let (address, _seen) = fake_control(r#"{"success":false,"error":"unknown client"}"#).await;
    let error = GatewayControl::new(address)
        .notify_client("nobody", "m", None)
        .await
        .unwrap_err();
    assert!(matches!(error, CallbackError::Rejected(reason) if reason == "unknown client"));
}

#[tokio::test]
async fn unreachable_control_plane_is_an_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let error = GatewayControl::new(address)
        .register_client("c", "u")
        .await
        .unwrap_err();
    assert!(matches!(error, CallbackError::Control(_)));
}

struct Queue(tokio::sync::Mutex<mpsc::UnboundedReceiver<io::Result<Bytes>>>);

impl PushSource for Queue {
    fn recv_push(&self) -> PushFuture<'_> {
        Box::pin(async move {
            self.0
                .lock()
                .await
                .recv()
                .await
                .unwrap_or_else(|| Err(io::Error::other("closed")))
        })
    }
}

struct Collect {
    messages: mpsc::UnboundedSender<CallbackMessage>,
    closed: mpsc::UnboundedSender<()>,
}

impl CallbackHandler for Collect {
    fn on_message(&self, message: CallbackMessage) {
        let _ = self.messages.send(message);
    }

    fn on_closed(&self) {
        let _ = self.closed.send(());
    }
}

#[tokio::test]
async fn listener_delivers_valid_messages_skips_invalid_and_reports_close() {
    let (push, queue) = mpsc::unbounded_channel();
    let (messages, mut delivered) = mpsc::unbounded_channel();
    let (closed, mut closures) = mpsc::unbounded_channel();
    let listener = CallbackListener::spawn(
        Arc::new(Queue(tokio::sync::Mutex::new(queue))),
        Arc::new(Collect { messages, closed }),
    );
    push.send(Ok(Bytes::from_static(b"garbage"))).unwrap();
    push.send(Ok(Bytes::from_static(
        br#"{"jsonrpc":"2.0","method":"hello","params":[1]}"#,
    )))
    .unwrap();
    let message = delivered.recv().await.unwrap();
    assert_eq!(message.method, "hello");
    assert!(listener.is_running());
    push.send(Err(io::Error::other("gone"))).unwrap();
    closures.recv().await.unwrap();
}

#[tokio::test]
async fn stopped_listener_does_not_report_close() {
    let (_push, queue) = mpsc::unbounded_channel();
    let (messages, _delivered) = mpsc::unbounded_channel();
    let (closed, mut closures) = mpsc::unbounded_channel();
    let listener = CallbackListener::spawn(
        Arc::new(Queue(tokio::sync::Mutex::new(queue))),
        Arc::new(Collect { messages, closed }),
    );
    listener.stop();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!listener.is_running());
    assert!(closures.try_recv().is_err());
}
