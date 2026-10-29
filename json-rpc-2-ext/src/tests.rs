use super::*;

fn request(method: &str, params: Value, id: Value) -> Value {
    json!({"jsonrpc": VERSION, "method": method, "params": params, "id": id})
}

fn response_result(response: Option<Value>) -> Value {
    response.unwrap()["result"].clone()
}

struct DenyEvents;

impl EventAuthorizer for DenyEvents {
    fn can_subscribe(&self, _: &str, _: &str, _: &str) -> bool {
        false
    }
    fn can_publish_topic(&self, _: &str, _: &str, _: &str) -> bool {
        false
    }
    fn can_send_to_client(&self, _: &str, _: &str, _: &str) -> bool {
        false
    }
}

#[test]
fn target_uses_client_id_wire_casing() {
    assert_eq!(
        serde_json::to_value(EventTarget::Client {
            client_id: "client-17".into()
        })
        .unwrap(),
        json!({"clientId":"client-17"})
    );
}

#[tokio::test]
async fn publishes_one_group_notification_for_multiple_matching_subscriptions() {
    let server = EventServer::with_default_capacity(Arc::new(AllowAllEvents));
    let mut alice = server.connect_with_id("alice").unwrap();
    let _bob = server.connect_with_id("bob").unwrap();
    let first = response_result(server.handle_message(
        "alice",
        request(
            "events.subscribe",
            json!({"topic":"room:42","event":"chat.message"}),
            json!(1),
        ),
    ))["subscription"]
        .as_str()
        .unwrap()
        .to_owned();
    let second = response_result(server.handle_message(
        "alice",
        request(
            "events.subscribe",
            json!({"topic":"room:42","event":"chat.message"}),
            json!(2),
        ),
    ))["subscription"]
        .as_str()
        .unwrap()
        .to_owned();
    response_result(server.handle_message(
        "bob",
        request(
            "events.subscribe",
            json!({"topic":"room:42","event":"chat.message"}),
            json!(3),
        ),
    ));
    let result = response_result(server.handle_message(
        "alice",
        request(
            "events.publish",
            json!({"target":{"topic":"room:42"},"event":"chat.message","data":{"text":"hello"}}),
            json!(4),
        ),
    ));
    assert_eq!(result, json!({"accepted":true,"recipients":2}));
    let event = alice.recv().await.unwrap();
    assert_eq!(event.topic.as_deref(), Some("room:42"));
    assert_eq!(event.subscriptions, vec![first, second]);
    assert_eq!(event.data, json!({"text":"hello"}));
    assert!(alice.receiver.try_recv().is_err());
}

#[tokio::test]
async fn direct_publish_omits_topic_and_disconnect_cleans_up() {
    let server = EventServer::with_default_capacity(Arc::new(AllowAllEvents));
    let mut client = server.connect_with_id("client-17").unwrap();
    let _sender = server.connect_with_id("sender").unwrap();
    response_result(server.handle_message(
        "sender",
        request(
            "events.publish",
            json!({"target":{"clientId":"client-17"},"event":"notification","data":null}),
            json!(1),
        ),
    ));
    let event = client.recv().await.unwrap();
    assert_eq!(event.topic, None);
    assert!(event.subscriptions.is_empty());
    drop(client);
    assert_eq!(
        response_result(server.handle_message(
            "sender",
            request(
                "events.publish",
                json!({"target":{"clientId":"client-17"},"event":"notification","data":null}),
                json!(2),
            )
        )),
        json!({"accepted":true,"recipients":0})
    );
}

#[tokio::test]
async fn unsubscribe_and_notifications_follow_json_rpc_response_rules() {
    let server = EventServer::with_default_capacity(Arc::new(AllowAllEvents));
    let mut client = server.connect_with_id("client").unwrap();
    let result = response_result(server.handle_message(
        "client",
        request(
            "events.subscribe",
            json!({"topic":"Case","event":"changed"}),
            json!("s"),
        ),
    ));
    let sub = result["subscription"].as_str().unwrap();
    assert!(
        server
            .handle_message(
                "client",
                json!({
                    "jsonrpc":VERSION,"method":"events.unsubscribe","params":{"subscription":sub}
                })
            )
            .is_none()
    );
    let publish = server.handle_message(
        "client",
        request(
            "events.publish",
            json!({"target":{"topic":"Case"},"event":"changed","data":true}),
            json!(5),
        ),
    );
    assert_eq!(response_result(publish)["recipients"], 0);
    assert!(client.receiver.try_recv().is_err());
}

#[test]
fn authorization_is_checked_for_subscribe_and_publish() {
    let server = EventServer::with_default_capacity(Arc::new(DenyEvents));
    let _client = server.connect_with_id("client").unwrap();
    for (method, params) in [
        (
            "events.subscribe",
            json!({"topic":"room","event":"message"}),
        ),
        (
            "events.publish",
            json!({"target":{"topic":"room"},"event":"message","data":null}),
        ),
        (
            "events.publish",
            json!({"target":{"clientId":"client"},"event":"message","data":null}),
        ),
    ] {
        let response = server
            .handle_message("client", request(method, params, json!(1)))
            .unwrap();
        assert_eq!(response["error"]["code"], ERR_FORBIDDEN);
    }
}

#[tokio::test]
async fn client_helper_builds_requests_and_dispatches_notifications() {
    let client = EventClient::new(|method, params| async move {
        Ok(match method.as_str() {
            "events.subscribe" => json!({"subscription":"sub-1","topic":params["topic"]}),
            "events.unsubscribe" => json!({"unsubscribed":true}),
            "events.publish" => json!({"accepted":true,"recipients":1}),
            _ => return Err(RpcError::method_not_found()),
        })
    });
    assert_eq!(client.subscribe("room", "message").await.unwrap(), "sub-1");
    assert!(client.unsubscribe("sub-1").await.unwrap());
    assert_eq!(
        client
            .publish(
                EventTarget::Topic {
                    topic: "room".into()
                },
                "message",
                json!(1)
            )
            .await
            .unwrap(),
        PublishResult {
            accepted: true,
            recipients: 1
        }
    );
    assert!(client.handle_notification(&json!({
        "jsonrpc":"2.0","method":"events.event",
        "params":{"event":"message","data":1,"subscriptions":["sub-1"]}
    })));
    assert_eq!(client.next_event().await.unwrap().data, json!(1));
}
