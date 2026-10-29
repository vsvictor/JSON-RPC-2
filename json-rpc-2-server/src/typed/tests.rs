use serde_json::json;

use super::*;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = json!({"name": " Ada ", "time": 1, "extra": null}))]
struct Params {
    name: NonBlank,
    time: NonNull,
    #[serde(default, deserialize_with = "keep_null")]
    extra: Option<Value>,
}

#[test]
fn parse_validates_blank_null_and_keeps_explicit_null() {
    let params: Params = parse_params(json!({"name": " Ada ", "time": "t"})).unwrap();
    assert_eq!(params.name.as_str(), " Ada ");
    assert_eq!(*params.time, json!("t"));
    assert_eq!(params.extra, None);
    let params: Params = parse_params(json!({"name": "a", "time": 0, "extra": null})).unwrap();
    assert_eq!(params.extra, Some(Value::Null));
    for invalid in [
        json!({"name": " ", "time": 1}),
        json!({"name": "a", "time": null}),
        json!({"name": "a"}),
        json!({"name": 1, "time": 1}),
        json!([]),
    ] {
        let error = parse_params::<Params>(invalid).unwrap_err();
        assert_eq!(error, RpcError::invalid_params());
    }
}

#[test]
fn documentation_uses_schema_and_example() {
    let (schema, example) = params_documentation::<Params>();
    assert_eq!(example, json!({"name": " Ada ", "time": 1, "extra": null}));
    assert_eq!(
        schema["properties"]["name"],
        json!({"type": "string", "pattern": r"\S"})
    );
    assert_eq!(
        schema["properties"]["time"],
        json!({"not": {"type": "null"}})
    );
    assert_eq!(schema["required"], json!(["name", "time"]));
}

#[tokio::test]
async fn register_typed_dispatches_and_documents() {
    let server = RpcServer::new();
    server
        .register_typed("hello", 2u8, |factor, params: Params| async move {
            Ok(json!({"name": params.name, "factor": factor}))
        })
        .unwrap();
    server.register_alias("rpc.hello", "hello").unwrap();
    let call = |method: &str, params: Value| {
        let server = server.clone();
        let request = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        async move {
            let response = server
                .handle_bytes(request.to_string().as_bytes())
                .await
                .unwrap();
            serde_json::from_slice::<Value>(&response).unwrap()
        }
    };
    let ok = call("rpc.hello", json!({"name": "x", "time": 1})).await;
    assert_eq!(ok["result"], json!({"name": "x", "factor": 2}));
    let bad = call("hello", json!({"name": "", "time": 1})).await;
    assert_eq!(bad["error"]["code"], -32602);
    let described = server
        .method_descriptions()
        .into_iter()
        .find(|d| d.name == "rpc.hello")
        .unwrap();
    assert_eq!(
        described.example_request.unwrap()["params"],
        json!({"name": " Ada ", "time": 1, "extra": null})
    );
}

#[tokio::test]
async fn register_limited_charges_raw_key_before_validation() {
    let server = RpcServer::new();
    let limiter = Arc::new(RateLimiter::new(crate::RateLimitConfig {
        per_minute: 1,
        burst: 1,
    }));
    server
        .register_limited(
            "hello",
            (),
            limiter,
            "name",
            |(), params: Params| async move { Ok(params.name) },
        )
        .unwrap();
    let call = |params: Value| {
        let server = server.clone();
        let request = json!({"jsonrpc":"2.0","id":1,"method":"hello","params":params});
        async move {
            let response = server
                .handle_bytes(request.to_string().as_bytes())
                .await
                .unwrap();
            serde_json::from_slice::<Value>(&response).unwrap()
        }
    };
    assert_eq!(call(json!({"name": "a"})).await["error"]["code"], -32602);
    assert_eq!(
        call(json!({"name": "a", "time": 1})).await["error"]["code"],
        crate::RATE_LIMIT_ERROR_CODE
    );
    assert_eq!(call(json!({"name": "b", "time": 1})).await["result"], "b");
    assert!(
        server
            .method_descriptions()
            .into_iter()
            .any(|d| d.name == "hello" && d.example_request.is_some())
    );
}

#[derive(JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Item {
    id: u32,
}

#[test]
fn one_or_many_schema_accepts_object_or_bounded_array() {
    let schema = one_or_many_schema::<Item>(Some(3));
    let item = &schema["anyOf"][0];
    assert_eq!(item["type"], "object");
    assert_eq!(item["required"], json!(["id"]));
    assert_eq!(item["additionalProperties"], false);
    assert_eq!(
        schema["anyOf"][1],
        json!({"type": "array", "items": item, "minItems": 1, "maxItems": 3})
    );
    assert!(schema.get("type").is_none() && schema.get("properties").is_none());
    assert!(
        one_or_many_schema::<Item>(None)["anyOf"][1]
            .get("maxItems")
            .is_none()
    );
}
