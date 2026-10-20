use json_rpc_2_core::{RpcError, RpcId, RpcRequest, VERSION};
use serde_json::json;

#[test]
fn core_wire_types_serialize() {
    assert_eq!(
        serde_json::to_value(RpcRequest::call(
            "sum",
            Some(json!([1, 2])),
            RpcId::number(7)
        ))
        .unwrap(),
        json!({"jsonrpc": VERSION, "method":"sum","params":[1,2],"id":7})
    );
    assert_eq!(
        serde_json::to_value(RpcError::invalid_request()).unwrap(),
        json!({"code":-32600,"message":"Invalid Request"})
    );
}
