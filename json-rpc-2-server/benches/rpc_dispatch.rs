use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
use serde_json::json;
use tokio::runtime::Runtime;

fn bench_server() -> Arc<dyn JsonRpcProtocol> {
    Arc::new(
        RpcServer::builder()
            .register("echo", |params| async move { Ok(params) })
            .unwrap()
            .register("sum", |params| async move {
                let total = params
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|value| value.as_i64())
                    .sum::<i64>();
                Ok(json!(total))
            })
            .unwrap()
            .build(),
    )
}

fn rpc_dispatch(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let protocol = bench_server();

    c.bench_function("server_single_call", |b| {
        let request = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "method": "echo",
            "params": {"value": 42},
            "id": 1
        }))
        .unwrap();
        b.iter(|| {
            runtime.block_on(async {
                protocol.handle_message(&request).await.unwrap();
            });
        });
    });

    let mut group = c.benchmark_group("server_batch");
    for size in [4usize, 16, 64] {
        let batch = (0..size)
            .map(|index| {
                json!({
                    "jsonrpc": "2.0",
                    "method": "sum",
                    "params": [index as i64, 1, 2, 3],
                    "id": index,
                })
            })
            .collect::<Vec<_>>();
        let request = serde_json::to_vec(&batch).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), &request, |b, request| {
            b.iter(|| {
                runtime.block_on(async {
                    protocol.handle_message(request).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, rpc_dispatch);
criterion_main!(benches);
