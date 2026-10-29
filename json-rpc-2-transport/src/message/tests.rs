use super::*;
use http_body::Body;
use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

#[test]
fn http_body_transfers_segments_without_copying() {
    let header = Bytes::from(vec![1; 17]);
    let payload = Bytes::from(vec![2; 1024]);
    let expected = [
        (header.as_ptr(), header.len()),
        (payload.as_ptr(), payload.len()),
    ];
    let mut message = TransportMessage {
        header,
        payload,
        binary: false,
    };
    assert_eq!(message.size_hint().exact(), Some(1041));
    let mut context = Context::from_waker(Waker::noop());
    for (pointer, len) in expected {
        let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut message).poll_frame(&mut context) else {
            panic!("expected an owned HTTP body segment");
        };
        let data = frame.into_data().unwrap();
        assert_eq!(data.as_ptr(), pointer);
        assert_eq!(data.len(), len);
    }
    assert!(message.is_end_stream());
    assert_eq!(message.size_hint().exact(), Some(0));
    assert!(matches!(
        Pin::new(&mut message).poll_frame(&mut context),
        Poll::Ready(None)
    ));
}

#[test]
fn unframed_message_retains_its_allocation() {
    let payload = Bytes::from(vec![3; 1024]);
    let pointer = payload.as_ptr();
    assert_eq!(
        TransportMessage::from(payload).into_bytes().as_ptr(),
        pointer
    );
}
