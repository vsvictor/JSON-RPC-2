#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::json;

use json_rpc_2_server::{CONTROL_PING, JsonRpcProtocol, RpcServer};

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
    if data == CONTROL_PING {
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let _ = server().handle_message(data).await;
    });
});
