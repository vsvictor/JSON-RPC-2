use std::sync::Arc;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use json_rpc_2_core::WebSocketFuture;
use json_rpc_2_transport::{CancellationToken, TransportContext, TransportRuntime};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig},
};

use super::{Role, WebSocketMessage, WebSocketProtocol, serve};

struct SessionEcho {
    opened: tokio::sync::mpsc::UnboundedSender<TransportContext>,
}

impl WebSocketProtocol for SessionEcho {
    fn handle_message<'a>(&'a self, message: WebSocketMessage) -> WebSocketFuture<'a> {
        Box::pin(async move { Some(message) })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        message: WebSocketMessage,
        context: &'a TransportContext,
    ) -> WebSocketFuture<'a> {
        Box::pin(async move {
            match message {
                WebSocketMessage::Text(text) => Some(WebSocketMessage::Text(format!(
                    "{}:{}:{text}",
                    context.transport, context.connection_id
                ))),
                message => Some(message),
            }
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        let _ = self.opened.send(context.clone());
        if let Some(sender) = &context.push_sender {
            let _ = sender.send(Bytes::from_static(b"server-push"));
        }
    }
}

#[tokio::test]
async fn websocket_tcp_preserves_session_context_and_delivers_push() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (opened, mut sessions) = tokio::sync::mpsc::unbounded_channel();
    let protocol = Arc::new(SessionEcho { opened });
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(serve(
        listener,
        protocol,
        shutdown.clone(),
        Arc::new(TransportRuntime::default()),
    ));

    let stream = TcpStream::connect(address).await.unwrap();
    let mut client: WebSocketStream<TcpStream> =
        WebSocketStream::from_raw_socket(stream, Role::Client, Some(WebSocketConfig::default()))
            .await;
    let context = tokio::time::timeout(std::time::Duration::from_secs(2), sessions.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.transport, "ws-tcp");
    let connection_id = context.connection_id;

    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::Text("server-push".into())
    );
    client.send(Message::Text("request".into())).await.unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::Text(format!("ws-tcp:{connection_id}:request").into())
    );

    drop(client);
    shutdown.cancel();
    server.await.unwrap().unwrap();
}
