#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::{Decoder, LinesCodec};

use json_rpc_2_server::{CONTROL_PING, JsonRpcProtocol, RpcServer};
use serde_json::json;

fn server() -> RpcServer {
    let server = RpcServer::new();
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
    server
}

fuzz_target!(|data: &[u8]| {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut codec = LinesCodec::new_with_max_length(json_rpc_2_server::MAX_MESSAGE_BYTES);
        let mut buffer = BytesMut::from(data);
        while let Ok(Some(frame)) = codec.decode(&mut buffer) {
            if frame.as_bytes() == CONTROL_PING {
                continue;
            }
            let _ = server().handle_message(frame.as_bytes()).await;
        }
    });
});
