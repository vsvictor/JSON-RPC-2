#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

use json_rpc_2_server::{JsonRpcProtocol, RpcServer};

#[derive(Arbitrary, Debug)]
struct BatchShape {
    items: Vec<BatchItem>,
}

#[derive(Arbitrary, Debug)]
enum BatchItem {
    Call {
        method: String,
        id: IdShape,
        positional: Vec<i64>,
    },
    Notification {
        method: String,
        named_key: String,
        named_value: i64,
    },
    Malformed {
        method: Option<String>,
        id: Option<IdShape>,
        jsonrpc: Option<String>,
    },
}

#[derive(Arbitrary, Debug)]
enum IdShape {
    Null,
    Number(i64),
    String(String),
    Bool(bool),
}

fn make_server() -> RpcServer {
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

fn to_value(id: IdShape) -> Value {
    match id {
        IdShape::Null => Value::Null,
        IdShape::Number(value) => json!(value),
        IdShape::String(value) => json!(value),
        IdShape::Bool(value) => json!(value),
    }
}

fn normalize_method(method: String) -> String {
    match method.as_str() {
        "echo" | "sum" => method,
        _ => "echo".to_owned(),
    }
}

fn batch_json(shape: BatchShape) -> Vec<u8> {
    let batch = shape
        .items
        .into_iter()
        .map(|item| match item {
            BatchItem::Call {
                method,
                id,
                positional,
            } => json!({
                "jsonrpc": "2.0",
                "method": normalize_method(method),
                "params": positional,
                "id": to_value(id),
            }),
            BatchItem::Notification {
                method,
                named_key,
                named_value,
            } => json!({
                "jsonrpc": "2.0",
                "method": normalize_method(method),
                "params": { named_key: named_value },
            }),
            BatchItem::Malformed {
                method,
                id,
                jsonrpc,
            } => {
                let mut value = serde_json::Map::new();
                if let Some(jsonrpc) = jsonrpc {
                    value.insert("jsonrpc".to_owned(), json!(jsonrpc));
                }
                if let Some(method) = method {
                    value.insert("method".to_owned(), json!(method));
                }
                if let Some(id) = id {
                    value.insert("id".to_owned(), to_value(id));
                }
                Value::Object(value)
            }
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&batch).unwrap()
}

fuzz_target!(|data: &[u8]| {
    let Ok(shape) = Unstructured::new(data).arbitrary::<BatchShape>() else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let payload = batch_json(shape);
        let _ = make_server().handle_message(&payload).await;
    });
});
