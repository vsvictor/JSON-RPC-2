use std::net::{IpAddr, Ipv4Addr};

use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use serde_json::{Value, json};

#[tokio::test]
async fn external_jsonrpsee_client_interops_with_our_server() {
    let rpc = ArcServer::new();
    let running = json_rpc_2_server::MultiTransportServer::builder(rpc.protocol.clone())
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_http1_port(0)
        .start()
        .await
        .unwrap();
    let address = running
        .listeners()
        .find(|(kind, _)| matches!(kind, json_rpc_2_server::TransportKind::Http1))
        .map(|(_, address)| address)
        .unwrap();

    let client = HttpClientBuilder::default()
        .build(format!("http://{address}"))
        .unwrap();
    let value: Value = client
        .request("echo", rpc_params![json!({"value": 11})])
        .await
        .unwrap();
    assert_eq!(value, json!([{"value": 11}]));

    let body = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "method": "echo",
        "params": {"value": 7},
        "id": 1
    }))
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("http://{address}/"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["result"], json!({"value": 7}));

    running.close().await.unwrap();
}

struct ArcServer {
    protocol: std::sync::Arc<dyn JsonRpcProtocol>,
}

impl ArcServer {
    fn new() -> Self {
        let rpc = std::sync::Arc::new(
            RpcServer::builder()
                .register("echo", |params| async move { Ok(params) })
                .unwrap()
                .build(),
        );
        Self { protocol: rpc }
    }
}
