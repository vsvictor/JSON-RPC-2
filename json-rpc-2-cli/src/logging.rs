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
mod tests {
    use super::*;
    use std::{
        io::Write,
        sync::{Arc, Mutex, mpsc},
        time::Duration,
    };

    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn shutdown_drains_queued_records() {
        let buffer = Buffer(Arc::default());
        let (mut output, worker) = writer(buffer.clone(), 8);
        let guard = LoggingGuard {
            errors: output.error_counter(),
            worker: Some(worker),
        };
        output.write_all(b"first\n").unwrap();
        output.write_all(b"last\n").unwrap();
        drop(guard);
        assert_eq!(&*buffer.0.lock().unwrap(), b"first\nlast\n");
    }

    #[test]
    fn full_queue_drops_records_without_waiting_for_output() {
        struct SlowWriter(mpsc::Sender<()>, mpsc::Receiver<()>, bool);
        impl Write for SlowWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if !self.2 {
                    self.2 = true;
                    self.0.send(()).unwrap();
                    self.1
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(io::Error::other)?;
                }
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (mut output, guard) = writer(SlowWriter(entered_tx, release_rx, false), 2);
        let errors = output.error_counter();
        output.write_all(b"blocked\n").unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            for _ in 0..10 {
                output.write_all(b"overflow\n").unwrap();
            }
            done_tx.send(()).unwrap();
        });
        let completed = done_rx.recv_timeout(Duration::from_secs(2));
        release_tx.send(()).unwrap();
        producer.join().unwrap();
        completed.expect("producer must finish before output resumes");
        assert_eq!(errors.dropped_lines(), 8);
        drop(guard);
    }
}
