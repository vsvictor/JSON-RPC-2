use std::sync::Arc;

use json_rpc_2_server::{JsonRpcProtocol, RpcServer};
use proptest::prelude::*;
use serde_json::{Value, json};

fn protocol() -> Arc<dyn JsonRpcProtocol> {
    Arc::new(
        RpcServer::builder()
            .register("echo", |params| async move { Ok(params) })
            .unwrap()
            .build(),
    )
}

proptest! {
    #[test]
    fn invalid_id_shapes_become_invalid_request(invalid in prop_oneof![
        any::<bool>().prop_map(Value::Bool),
        proptest::collection::vec(any::<i64>(), 0..4).prop_map(|items| json!(items)),
        proptest::collection::btree_map(".*", any::<i64>(), 0..4).prop_map(|items| json!(items)),
    ]) {
        let protocol = protocol();
        let request = json!({
            "jsonrpc": "2.0",
            "method": "echo",
            "params": {"value": 1},
            "id": invalid,
        });
        let response = tokio_test::block_on(protocol.handle_message(request.to_string().as_bytes()))
            .expect("invalid request must produce a response");
        let value: Value = serde_json::from_slice(&response).unwrap();
        prop_assert_eq!(value["error"]["code"].clone(), json!(-32600));
    }

    #[test]
    fn mixed_batch_returns_only_call_responses(notification_flags in proptest::collection::vec(any::<bool>(), 1..8)) {
        let protocol = protocol();
        let batch: Vec<Value> = notification_flags
            .iter()
            .enumerate()
            .map(|(index, notification)| {
                if *notification {
                    json!({"jsonrpc":"2.0","method":"echo","params":{"index":index}})
                } else {
                    json!({"jsonrpc":"2.0","method":"echo","params":{"index":index},"id":index})
                }
            })
            .collect();
        let response = tokio_test::block_on(protocol.handle_message(serde_json::to_vec(&batch).unwrap().as_slice()));
        let expected_calls = notification_flags.iter().filter(|notification| !**notification).count();
        match (expected_calls, response) {
            (0, None) => {}
            (count, Some(response)) => {
                let value: Value = serde_json::from_slice(&response).unwrap();
                let items = value.as_array().expect("batch response array");
                prop_assert_eq!(items.len(), count);
                for item in items {
                    prop_assert_eq!(item["jsonrpc"].clone(), json!("2.0"));
                    prop_assert!(item.get("id").is_some());
                }
            }
            (_, other) => prop_assert!(other.is_some(), "expected response for call batch"),
        }
    }
}
