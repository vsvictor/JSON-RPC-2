use std::io;

use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::EnvFilter;

pub struct LoggingGuard {
    worker: Option<WorkerGuard>,
    errors: ErrorCounter,
}

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        drop(self.worker.take());
        let dropped = self.errors.dropped_lines();
        if dropped > 0 {
            eprintln!("logging: dropped {dropped} records because the log queue was full");
        }
    }
}

fn writer(output: impl io::Write + Send + 'static, capacity: usize) -> (NonBlocking, WorkerGuard) {
    NonBlockingBuilder::default()
        .buffered_lines_limit(capacity)
        .lossy(true)
        .thread_name("json-rpc-log-writer")
        .finish(output)
}

pub fn init() -> Result<LoggingGuard, Box<dyn std::error::Error + Send + Sync>> {
    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => EnvFilter::try_new(value)?,
        Err(std::env::VarError::NotPresent) => {
            EnvFilter::new("json_rpc_2=debug,json_rpc_2_client=debug,json_rpc_2_server=debug")
        }
        Err(error) => return Err(error.into()),
    };
    let (writer, worker) = writer(io::stdout(), 1024);
    let errors = writer.error_counter();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(writer)
        .try_init()?;
    Ok(LoggingGuard {
        worker: Some(worker),
        errors,
    })
}

#[cfg(test)]
mod tests;
