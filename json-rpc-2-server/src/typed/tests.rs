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

mod rpc_methods_macro {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde_json::{Value, json};

    use crate::{NonBlank, RateLimitConfig, RateLimiter, RpcResult, RpcServer, rpc_methods};

    #[derive(Deserialize, JsonSchema)]
    #[schemars(example = json!({"name": "Ada"}))]
    struct Name {
        name: NonBlank,
    }

    #[derive(Deserialize, JsonSchema)]
    struct Empty {}

    #[derive(Default)]
    struct Greeter {
        calls: AtomicU32,
    }

    #[rpc_methods(alias_prefix = "rpc.")]
    impl Greeter {
        #[rpc]
        async fn hello(&self, params: Name) -> RpcResult<String> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(format!("Hello, {}", params.name.as_str()))
        }

        #[rpc(name = "greeter.count", alias = "count")]
        async fn count(&self, _params: Empty) -> RpcResult<u32> {
            Ok(self.calls.load(Ordering::Relaxed))
        }

        #[rpc(name = "limited", limit = "name")]
        async fn limited(&self, params: Name) -> RpcResult<Value> {
            Ok(json!(params.name))
        }

        fn helper(&self) -> u32 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    async fn call(server: &RpcServer, method: &str, params: Value) -> Value {
        let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response = server
            .handle_bytes(request.to_string().as_bytes())
            .await
            .unwrap();
        serde_json::from_slice(&response).unwrap()
    }

    #[tokio::test]
    async fn registers_names_aliases_limits_and_docs() {
        let server = RpcServer::new();
        let greeter = Arc::new(Greeter::default());
        let limiter = Arc::new(RateLimiter::new(RateLimitConfig {
            per_minute: 1,
            burst: 1,
        }));
        greeter.clone().register_rpc(&server, &limiter).unwrap();

        let ok = call(&server, "rpc.hello", json!({"name": "Ada"})).await;
        assert_eq!(ok["result"], "Hello, Ada");
        assert_eq!(
            call(&server, "hello", json!({"name": " "})).await["error"]["code"],
            -32602
        );
        for name in ["greeter.count", "rpc.greeter.count", "count"] {
            assert_eq!(call(&server, name, json!({})).await["result"], 1);
        }
        assert_eq!(greeter.helper(), 1);

        assert_eq!(
            call(&server, "limited", json!({"name": "x"})).await["result"],
            "x"
        );
        assert_eq!(
            call(&server, "rpc.limited", json!({"name": "x"})).await["error"]["code"],
            crate::RATE_LIMIT_ERROR_CODE
        );

        let hello = server
            .method_descriptions()
            .into_iter()
            .find(|d| d.name == "rpc.hello")
            .unwrap();
        assert_eq!(
            hello.example_request.unwrap()["params"],
            json!({"name": "Ada"})
        );
        assert!(
            server.register_alias("hello", "count").is_err(),
            "names are taken"
        );
    }
}

mod rpc_api_macro {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use serde_json::{Value, json};

    use crate::{RateLimitConfig, RateLimiter, RpcResult, RpcServer, rpc_api};

    #[rpc_api(alias_prefix = "rpc.")]
    mod api {
        use crate::NonBlank;

        #[params(method = "getShift", result = Shift)]
        #[schemars(example = serde_json::json!({"driverId": "d1"}))]
        pub struct GetShift {
            pub driver_id: NonBlank,
        }

        #[result]
        pub struct Shift {
            pub driver_id: String,
            pub is_open: bool,
        }

        #[params(method = "tracking.sendLocation", result = u32, limit = "phone", alias = "sendLocation")]
        #[serde(rename_all = "snake_case")]
        pub struct SendLocation {
            pub phone: NonBlank,
            pub point_count: u32,
        }

        #[params(method = "session.info", result = String, handler = "info")]
        #[serde(tag = "kind")]
        pub enum Info {
            Short,
            Long,
        }
    }

    #[rpc_api(trait_name = "CounterApi")]
    mod counter {
        #[params(method = "count", result = u32)]
        pub struct Count {}
    }

    #[derive(Default)]
    struct Logic {
        calls: AtomicU32,
    }

    impl api::Api for Logic {
        async fn get_shift(&self, params: api::GetShift) -> RpcResult<api::Shift> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(api::Shift {
                driver_id: params.driver_id.as_str().to_owned(),
                is_open: true,
            })
        }

        async fn tracking_send_location(&self, params: api::SendLocation) -> RpcResult<u32> {
            Ok(params.point_count + u32::from(params.phone.as_str().is_empty()))
        }

        async fn info(&self, params: api::Info) -> RpcResult<String> {
            Ok(match params {
                api::Info::Short => "short".into(),
                api::Info::Long => "long".into(),
            })
        }
    }

    impl counter::CounterApi for Logic {
        async fn count(&self, _params: counter::Count) -> RpcResult<u32> {
            Ok(self.calls.load(Ordering::Relaxed))
        }
    }

    async fn call(server: &RpcServer, method: &str, params: Value) -> Value {
        let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response = server
            .handle_bytes(request.to_string().as_bytes())
            .await
            .unwrap();
        serde_json::from_slice(&response).unwrap()
    }

    #[tokio::test]
    async fn generates_trait_registration_derives_and_aliases() {
        let server = RpcServer::new();
        let logic = Arc::new(Logic::default());
        let limiter = Arc::new(RateLimiter::new(RateLimitConfig {
            per_minute: 1,
            burst: 1,
        }));
        api::Api::register_rpc(logic.clone(), &server, &limiter).unwrap();
        counter::CounterApi::register_rpc(logic.clone(), &server).unwrap();

        let shift = call(&server, "rpc.getShift", json!({"driverId": "d1"})).await;
        assert_eq!(shift["result"], json!({"driverId": "d1", "isOpen": true}));
        assert_eq!(
            call(&server, "getShift", json!({"driverId": " "})).await["error"]["code"],
            -32602
        );
        assert_eq!(call(&server, "count", json!({})).await["result"], 1);
        assert_eq!(
            call(&server, "rpc.count", json!({})).await["error"]["code"],
            -32601,
            "alias_prefix is per module"
        );

        let location = json!({"phone": "+380", "point_count": 3});
        assert_eq!(
            call(&server, "sendLocation", location.clone()).await["result"],
            3
        );
        assert_eq!(
            call(&server, "rpc.tracking.sendLocation", location).await["error"]["code"],
            crate::RATE_LIMIT_ERROR_CODE
        );
        assert_eq!(
            call(&server, "session.info", json!({"kind": "Long"})).await["result"],
            "long"
        );

        let described = server
            .method_descriptions()
            .into_iter()
            .find(|d| d.name == "getShift")
            .unwrap();
        assert_eq!(
            described.example_request.unwrap()["params"],
            json!({"driverId": "d1"})
        );
    }
}
