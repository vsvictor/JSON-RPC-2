#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::json;

use json_rpc_2_server::{CONTROL_PATH, JsonRpcProtocol, RpcServer};

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
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut headers);
    let Ok(status) = request.parse(data) else {
        return;
    };
    let httparse::Status::Complete(header_len) = status else {
        return;
    };
    let method = request.method.unwrap_or_default();
    let path = request.path.unwrap_or_default();
    let content_length = request
        .headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case("content-length"))
        .and_then(|header| std::str::from_utf8(header.value).ok())
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let body = data.get(header_len..header_len.saturating_add(content_length));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        match (method, path, body) {
            ("GET", CONTROL_PATH, _) => {}
            ("POST", "/", Some(body)) if body.len() <= json_rpc_2_server::MAX_MESSAGE_BYTES => {
                let _ = server().handle_message(body).await;
            }
            _ => {}
        }
    });
});
