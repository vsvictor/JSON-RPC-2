use std::{sync::Arc, time::Duration};

use bytes::Bytes;

use super::*;

#[test]
fn classifies_server_initiated_messages() {
    assert!(is_incoming_message(
        br#"{"jsonrpc":"2.0","method":"events.event","params":{}}"#
    ));
    assert!(is_incoming_message(
        br#"{"jsonrpc":"2.0","id":7,"method":"client.confirm"}"#
    ));
    assert!(is_incoming_message(
        br#"  [{"jsonrpc":"2.0","method":"a"},{"jsonrpc":"2.0","method":"b"}]"#
    ));
}

#[test]
fn does_not_classify_responses_or_control_frames() {
    assert!(!is_incoming_message(
        br#"{"jsonrpc":"2.0","id":1,"result":{"method":"x"}}"#
    ));
    assert!(!is_incoming_message(
        br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"m"}}"#
    ));
    assert!(!is_incoming_message(
        br#"[{"jsonrpc":"2.0","method":"a"},{"jsonrpc":"2.0","id":1,"result":1}]"#
    ));
    assert!(!is_incoming_message(b"[]"));
    assert!(!is_incoming_message(b"pong"));
    assert!(!is_incoming_message(b""));
    assert!(!is_incoming_message(b"{not json"));
}

#[test]
fn bounded_queue_drops_oldest_and_reports_it() {
    let queue = IncomingQueue::with_capacity(2);
    queue.push(Bytes::from_static(b"1"));
    queue.push(Bytes::from_static(b"2"));
    queue.push(Bytes::from_static(b"3"));
    assert_eq!(queue.take_dropped(), 1);
    assert_eq!(queue.take_dropped(), 0);
    assert_eq!(queue.try_pop().as_deref(), Some(&b"2"[..]));
    assert_eq!(queue.try_pop().as_deref(), Some(&b"3"[..]));
    assert_eq!(queue.try_pop(), None);
}

#[tokio::test]
async fn recv_waits_for_push_and_ends_after_close_is_drained() {
    let queue = Arc::new(IncomingQueue::default());
    let consumer = {
        let queue = Arc::clone(&queue);
        tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(message) = queue.recv().await {
                received.push(message);
            }
            received
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    queue.push(Bytes::from_static(b"a"));
    queue.push(Bytes::from_static(b"b"));
    queue.close();
    let received = tokio::time::timeout(Duration::from_secs(1), consumer)
        .await
        .expect("consumer finished")
        .expect("consumer did not panic");
    assert_eq!(
        received,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
}
