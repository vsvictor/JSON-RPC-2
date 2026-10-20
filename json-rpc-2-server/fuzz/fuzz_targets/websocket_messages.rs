#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use serde_json::json;

use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
use json_rpc_2_websockets::server::transports::{
    JsonRpcWebSocket, WebSocketMessage, WebSocketProtocol,
};

#[derive(Arbitrary, Debug)]
enum InputMessage {
    Text(String),
    Binary(Vec<u8>),
}

fn protocol() -> JsonRpcWebSocket {
    let server = Arc::new(RpcServer::new());
    server
        .register("echo", |params| async move { Ok(params) })
        .unwrap();
    server
        .register("sum", |params| async move {
            let total = params
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|value| value.as_i64())
                .sum::<i64>();
            Ok(json!(total))
        })
        .unwrap();
    JsonRpcWebSocket::new(server as Arc<dyn JsonRpcProtocol>)
}

use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    let Ok(message) = Unstructured::new(data).arbitrary::<InputMessage>() else {
        return;
    };
    let message = match message {
        InputMessage::Text(text) => WebSocketMessage::Text(text),
        InputMessage::Binary(bytes) => WebSocketMessage::Binary(Bytes::from(bytes)),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let _ = protocol().handle_message(message).await;
    });
});
