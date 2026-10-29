use super::*;

#[tokio::test]
async fn discovery_tracks_registration_aliases_and_builder() {
    let server = RpcServer::builder()
        .register("z", |p| async { Ok(p) })
        .unwrap()
        .build();
    let clone = server.clone();
    clone.register("a", |p| async { Ok(p) }).unwrap();
    server
        .document_method("a", json!({"type":"object"}), json!({"message":"mock"}))
        .unwrap();
    server.register_alias("rpc.a", "a").unwrap();
    assert_eq!(
        server.method_names(),
        ["a", "rpc.a", "rpc.list_methods", "z"]
    );
    assert_eq!(
        server.register_alias("missing", "absent"),
        Err(RegistrationError::UnknownMethod)
    );
    assert_eq!(
        server.register_alias("rpc.list_methods", "a"),
        Err(RegistrationError::ReservedMethod)
    );
    assert_eq!(
        server.register_alias("a", "z"),
        Err(RegistrationError::DuplicateMethod)
    );
    let response = server
        .handle_bytes(br#"{"jsonrpc":"2.0","id":1,"method":"rpc.list_methods"}"#)
        .await
        .unwrap();
    let response: Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response["result"], json!(server.method_names()));
    let response = server
        .handle_bytes(
            br#"{"jsonrpc":"2.0","id":3,"method":"rpc.list_methods","params":{"describe":true}}"#,
        )
        .await
        .unwrap();
    let response: Value = serde_json::from_slice(&response).unwrap();
    let descriptions: Vec<MethodDescription> =
        serde_json::from_value(response["result"].clone()).unwrap();
    assert_eq!(
        descriptions[0].params_schema,
        Some(json!({"type":"object"}))
    );
    assert_eq!(
        descriptions[1].example_request.as_ref().unwrap()["method"],
        "rpc.a"
    );
    assert_eq!(
        descriptions[1].example_request.as_ref().unwrap()["params"],
        json!({"message":"mock"})
    );
    assert!(descriptions[3].params_schema.is_none());
    assert_eq!(
        server.document_method("absent", json!({}), json!({})),
        Err(RegistrationError::UnknownMethod)
    );
    let response = server
        .handle_bytes(br#"{"jsonrpc":"2.0","id":2,"method":"rpc.list_methods","params":[1]}"#)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response).unwrap()["error"]["code"],
        -32602
    );
    assert!(
        server
            .handle_bytes(br#"{"jsonrpc":"2.0","method":"rpc.list_methods"}"#)
            .await
            .is_none()
    );
}
