//! Shared multiplexor for transports that fan out independent stream sessions.

use std::{
    future::Future,
    io,
    sync::{Arc, atomic::Ordering},
};

use tokio::sync::Semaphore;
use tokio_util::task::TaskTracker;

use super::RuntimeMetrics;

/// Shared session multiplexer for stream-oriented transports.
///
/// Each accepted connection or stream is spawned as an independent task while
/// the semaphore keeps a bounded notion of concurrent in-flight sessions.
pub struct StreamMultiplexor {
    sessions: TaskTracker,
    limit: Arc<Semaphore>,
    metrics: Arc<RuntimeMetrics>,
}

impl StreamMultiplexor {
    /// Create a multiplexor with an explicit maximum number of concurrent
    /// sessions.
    pub fn with_limit(max_sessions: usize, metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            sessions: TaskTracker::new(),
            limit: Arc::new(Semaphore::new(max_sessions)),
            metrics,
        }
    }

    /// Cloneable handle used by accept loops and upgrade callbacks.
    pub fn handle(&self) -> StreamMultiplexorHandle {
        StreamMultiplexorHandle {
            sessions: self.sessions.clone(),
            limit: self.limit.clone(),
            metrics: self.metrics.clone(),
        }
    }

    /// Stop accepting new work and wait until every tracked session ends.
    pub async fn close(self) {
        self.sessions.close();
        self.sessions.wait().await;
    }
}

/// Handle that dispatches accepted sessions into the shared multiplexor.
#[derive(Clone)]
pub struct StreamMultiplexorHandle {
    sessions: TaskTracker,
    limit: Arc<Semaphore>,
    metrics: Arc<RuntimeMetrics>,
}

impl StreamMultiplexorHandle {
    /// Spawn one session into the multiplexor.
    pub async fn spawn<F>(&self, session: F) -> io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let permit = self
            .limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io::Error::other("session multiplexor closed"))?;
        self.metrics.active_sessions.fetch_add(1, Ordering::Relaxed);
        self.metrics.active_streams.fetch_add(1, Ordering::Relaxed);
        let metrics = self.metrics.clone();
        self.sessions.spawn(async move {
            let _permit = permit;
            session.await;
            metrics.active_streams.fetch_sub(1, Ordering::Relaxed);
            metrics.active_sessions.fetch_sub(1, Ordering::Relaxed);
        });
        Ok(())
    }
}
