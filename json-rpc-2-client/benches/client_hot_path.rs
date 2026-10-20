use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use json_rpc_2_client::{RpcClient, RpcClientTransport, expects_response};
use serde_json::{Value, json};

struct LoopbackTransport;

impl RpcClientTransport for LoopbackTransport {
    fn send<'a>(&'a self, message: Bytes) -> json_rpc_2_client::ClientTransportFuture<'a> {
        Box::pin(async move {
            let request: Value = serde_json::from_slice(&message).unwrap();
            let response = json!({
                "jsonrpc": "2.0",
                "result": request.get("params").cloned().unwrap_or(Value::Null),
                "id": request.get("id").cloned().unwrap_or(Value::Null),
            });
            Ok(Some(Bytes::from(serde_json::to_vec(&response).unwrap())))
        })
    }

    fn transport_ping<'a>(&'a self) -> json_rpc_2_client::ClientPingFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn reconnect<'a>(&'a self) -> json_rpc_2_client::ClientRecoverFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(&'a self) -> json_rpc_2_client::ClientCloseFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

fn client_hot_path(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let client = RpcClient::new(LoopbackTransport);
    drop(_guard);
    let payload = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "method": "echo",
        "params": {"bench": true},
        "id": 1
    }))
    .unwrap();

    c.bench_function("client_expects_response", |b| {
        b.iter(|| {
            assert!(expects_response(&payload));
        });
    });

    c.bench_function("client_request_roundtrip_loopback", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let value = client
                    .request_value("echo", Some(json!({"bench": true})))
                    .await
                    .unwrap();
                let _ = value;
            });
        });
    });
}

criterion_group!(benches, client_hot_path);
criterion_main!(benches);
