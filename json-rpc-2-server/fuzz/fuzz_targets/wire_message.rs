#![no_main]

use libfuzzer_sys::fuzz_target;

use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
use serde_json::json;

fn server() -> RpcServer {
    let server = RpcServer::new();
    server
        .register("echo", |params| async move { Ok(params) })
        .unwrap();
    server
        .register("sum", |params| async move {
            let values = params.as_array().cloned().unwrap_or_default();
            let total: i64 = values.into_iter().filter_map(|value| value.as_i64()).sum();
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
        let _ = server().handle_message(data).await;
    });
});
