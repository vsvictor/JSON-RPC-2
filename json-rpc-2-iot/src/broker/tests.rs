use super::*;

#[test]
fn envelope_preserves_gateway_session_wire_format() {
    let envelope = Envelope {
        payload: vec![0, 255],
        reply_to: Some("json-rpc-2/replies/client".into()),
        correlation_id: "request-1".into(),
        control: false,
        metadata: Metadata {
            session: Some(SessionIdentity {
                id: "550e8400-e29b-41d4-a716-446655440000".into(),
                push_to: "json-rpc-2/replies/client".into(),
            }),
            kind: MessageKind::Open,
        },
    };

    let value = serde_json::to_value(envelope).unwrap();
    assert_eq!(value["payload"], serde_json::json!([0, 255]));
    assert_eq!(value["correlation_id"], "request-1");
    assert_eq!(value["metadata"]["kind"], "open");
    assert_eq!(
        value["metadata"]["session"]["id"],
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(
        value["metadata"]["session"]["push_to"],
        "json-rpc-2/replies/client"
    );
}
