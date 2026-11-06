//! Server-initiated (push) messages received by persistent client transports.

use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use serde::Deserialize;
use serde::de::IgnoredAny;
use tokio::sync::Notify;

/// Default number of undelivered incoming messages kept per transport.
pub const DEFAULT_INCOMING_CAPACITY: usize = 1024;

/// Bounded FIFO of server-initiated messages shared by a transport's reader
/// and the application consumer.
///
/// When the consumer falls behind, the oldest message is dropped so memory
/// stays bounded; [`IncomingQueue::take_dropped`] reports how many were lost.
pub struct IncomingQueue {
    items: Mutex<VecDeque<Bytes>>,
    capacity: usize,
    closed: AtomicBool,
    dropped: AtomicU64,
    changed: Notify,
}

impl Default for IncomingQueue {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_INCOMING_CAPACITY)
    }
}

impl IncomingQueue {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
            closed: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }

    /// Enqueue one message and wake every waiting consumer.
    pub fn push(&self, message: Bytes) {
        {
            let mut items = self.items.lock().expect("incoming queue lock poisoned");
            if items.len() == self.capacity {
                items.pop_front();
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            items.push_back(message);
        }
        self.changed.notify_waiters();
    }

    pub fn try_pop(&self) -> Option<Bytes> {
        self.items
            .lock()
            .expect("incoming queue lock poisoned")
            .pop_front()
    }

    /// Wake waiting consumers without enqueuing, e.g. after the transport
    /// connection became available again.
    pub fn wake(&self) {
        self.changed.notify_waiters();
    }

    /// Mark the queue closed: once drained, [`IncomingQueue::recv`] returns `None`.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Number of messages dropped because the queue was full since the last call.
    pub fn take_dropped(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }

    /// Wake-up future for [`IncomingQueue::push`], [`IncomingQueue::wake`] and
    /// [`IncomingQueue::close`]. Pin it and call `enable` before checking state
    /// so a concurrent wake-up is never missed.
    pub fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    /// Wait for the next message. Returns `None` once closed and drained.
    pub async fn recv(&self) -> Option<Bytes> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(message) = self.try_pop() {
                return Some(message);
            }
            if self.is_closed() {
                return None;
            }
            changed.await;
        }
    }
}

#[derive(Deserialize)]
struct MessageProbe {
    method: Option<IgnoredAny>,
    result: Option<IgnoredAny>,
    error: Option<IgnoredAny>,
}

impl MessageProbe {
    fn is_server_initiated(&self) -> bool {
        self.method.is_some() && self.result.is_none() && self.error.is_none()
    }
}

/// Whether a frame read from a JSON-lines stream is a server-initiated
/// JSON-RPC message (notification or request carrying `method`) rather than a
/// response to the client's own request. Batches qualify only when every item
/// is server-initiated. Non-JSON control frames (for example pong) are not.
pub fn is_incoming_message(frame: &[u8]) -> bool {
    let Some(first) = frame.iter().find(|byte| !byte.is_ascii_whitespace()) else {
        return false;
    };
    match first {
        b'{' => serde_json::from_slice::<MessageProbe>(frame)
            .is_ok_and(|probe| probe.is_server_initiated()),
        b'[' => serde_json::from_slice::<Vec<MessageProbe>>(frame).is_ok_and(|items| {
            !items.is_empty() && items.iter().all(MessageProbe::is_server_initiated)
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
