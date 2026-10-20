#![cfg(feature = "http1")]

use std::{net::Ipv4Addr, time::Duration};

use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
use json_rpc_2_tcp::server::transports::http1::Http1Transport;
use json_rpc_2_transport::Transport;
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use serde_json::{Value, json};

#[tokio::test]
async fn jsonrpc_normative_response_shapes_and_notification_rules() {
    let rpc = ArcServer::new();
    for (request, expected_code, expected_id) in [
        ("{", -32700, Value::Null),
        ("[]", -32600, Value::Null),
        ("true", -32600, Value::Null),
        (
            r#"{"jsonrpc":"1.0","method":"echo"}"#,
            -32600,
            Value::Null,
        ),
        (
            r#"{"jsonrpc":"2.0","method":"missing","id":"x"}"#,
            -32601,
            json!("x"),
        ),
        (
            r#"{"jsonrpc":"2.0","method":"echo","id":[]}"#,
            -32600,
            Value::Null,
        ),
    ] {
        let bytes = rpc
            .protocol
            .handle_message(request.as_bytes())
            .await
            .unwrap();
        let response: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(response["jsonrpc"], "2.0", "{request}");
        assert_eq!(response["id"], expected_id, "{request}");
        assert_eq!(response["error"]["code"], expected_code, "{request}");
        assert!(response["error"]["message"].is_string(), "{request}");
        assert!(
            !response.as_object().unwrap().contains_key("result"),
            "{request}"
        );
    }

    for id in [json!(null), json!("rpc-1"), json!(7), json!(-7), json!(1.5)] {
        let request = json!({"jsonrpc": "2.0", "method": "echo", "params": [42], "id": id});
        let bytes = rpc
            .protocol
            .handle_message(&serde_json::to_vec(&request).unwrap())
            .await
            .unwrap();
        let response: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            response,
            json!({"jsonrpc": "2.0", "result": [42], "id": id})
        );
    }

    for request in [
        json!({"jsonrpc": "2.0", "method": "missing"}),
        json!([
            {"jsonrpc": "2.0", "method": "echo", "params": [1]},
            {"jsonrpc": "2.0", "method": "missing"}
        ]),
    ] {
        assert!(
            rpc.protocol
                .handle_message(&serde_json::to_vec(&request).unwrap())
                .await
                .is_none()
        );
    }
}

#[tokio::test]
async fn jsonrpc_mixed_batch_correlates_by_id_and_omits_notifications() {
    let rpc = ArcServer::new();
    let request = json!([
        {"jsonrpc": "2.0", "method": "echo", "params": [42], "id": "first"},
        {"jsonrpc": "2.0", "method": "echo"},
        {"jsonrpc": "2.0", "method": "missing", "id": 2},
        1
    ]);
    let bytes = rpc
        .protocol
        .handle_message(&serde_json::to_vec(&request).unwrap())
        .await
        .unwrap();
    let response: Value = serde_json::from_slice(&bytes).unwrap();
    let responses = response.as_array().unwrap();
    assert_eq!(responses.len(), 3);
    let success = responses.iter().find(|item| item["id"] == "first").unwrap();
    assert_eq!(
        success,
        &json!({"jsonrpc": "2.0", "result": [42], "id": "first"})
    );
    let missing = responses.iter().find(|item| item["id"] == 2).unwrap();
    assert_eq!(missing["error"]["code"], -32601);
    let invalid = responses.iter().find(|item| item["id"].is_null()).unwrap();
    assert_eq!(invalid["error"]["code"], -32600);
}

#[tokio::test]
async fn external_jsonrpsee_client_interops_with_our_server() {
    let rpc = ArcServer::new();
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let running = Http1Transport::new(listener).start(rpc.protocol.clone());

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(3))
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
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let response = http
        .post(format!("http://{address}/"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["result"], json!({"value": 7}));

    let notification = http
        .post(format!("http://{address}/"))
        .json(&json!({"jsonrpc": "2.0", "method": "echo", "params": [9]}))
        .send()
        .await
        .unwrap();
    assert_eq!(notification.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(notification.bytes().await.unwrap().is_empty());

    let batch: Value = http
        .post(format!("http://{address}/"))
        .json(&json!([
            {"jsonrpc": "2.0", "method": "echo", "params": [1], "id": "a"},
            {"jsonrpc": "2.0", "method": "missing", "id": "b"},
            {"jsonrpc": "2.0", "method": "echo"}
        ]))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let responses = batch.as_array().unwrap();
    assert_eq!(responses.len(), 2);
    assert_eq!(
        responses.iter().find(|item| item["id"] == "a").unwrap()["result"],
        json!([1])
    );
    assert_eq!(
        responses.iter().find(|item| item["id"] == "b").unwrap()["error"]["code"],
        -32601
    );

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
