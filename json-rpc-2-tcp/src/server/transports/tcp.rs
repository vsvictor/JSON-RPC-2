//! JSON lines and application-defined binary frames over TCP.

use std::{io, sync::Arc, time::Instant};

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[cfg(feature = "tcp")]
use crate::client::transports::tcp::{read_frame, write_message};
#[cfg(not(feature = "tcp"))]
use crate::client::transports::tls::{read_frame, write_message};
use bytes::{Bytes, BytesMut};
use json_rpc_2_transport::JsonRpcProtocol;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, CancellationToken, ProtocolSessionGuard, Transport,
    TransportFuture, TransportRuntime, TransportSession, apply_tcp_stream_tuning, log_control,
    log_json_rpc, log_transport_error,
};

/// TCP transport accepting JSON lines and application-defined binary frames.
pub struct TcpTransport {
    listener: TcpListener,
    runtime: Arc<TransportRuntime>,
}

impl TcpTransport {
    pub fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl Transport for TcpTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.listener, protocol, shutdown, self.runtime))
    }
}

/// Accept TCP connections and delegate each complete message to the protocol.
///
/// After `shutdown` is cancelled no new connections are accepted, every
/// connection finishes the message it is processing and is closed with FIN.
pub async fn serve(
    listener: TcpListener,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let connections = multiplexor.handle();
    let result = loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => break Err(error),
            },
            _ = shutdown.cancelled() => break Ok(()),
        };
        let protocol = protocol.clone();
        let shutdown = shutdown.clone();
        let runtime = runtime.clone();
        connections
            .spawn(async move {
                if let Err(error) = handle_connection(stream, protocol, shutdown, runtime).await {
                    log_transport_error("TCP", "listener", "connection", &error);
                }
            })
            .await?;
    };
    drop(listener);
    shutdown.cancel();
    multiplexor.close().await;
    result
}

async fn handle_connection(
    stream: TcpStream,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> Result<(), io::Error> {
    apply_tcp_stream_tuning(&stream, &runtime.tcp_tuning)?;
    let peer = stream.peer_addr()?;
    let (read_half, write_half) = stream.into_split();
    serve_stream(read_half, write_half, protocol, shutdown, peer, "tcp").await
}

/// Serve the JSON-lines/custom binary framing on an already accepted stream.
///
/// The caller owns the stream and is responsible for connection limits,
/// cancellation, and waiting for this future before shutting down its listener.
/// `kind` is copied into request and push session contexts.
pub async fn serve_stream<R, W>(
    mut reader: R,
    mut write_half: W,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    peer: std::net::SocketAddr,
    kind: &'static str,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = BytesMut::new();
    let TransportSession {
        context,
        mut push_receiver,
    } = TransportSession::new(kind, true);
    let session = ProtocolSessionGuard::new(protocol.clone(), context);
    let serving = async {
        loop {
            let event = tokio::select! {
                line = read_frame(&mut reader, &mut buffer, protocol.stream_framing()) => ReadEvent::Line(line),
                push = push_receiver.recv() => ReadEvent::Push(push),
                _ = shutdown.cancelled() => break,
            };
            let line = match event {
                ReadEvent::Push(Some(push)) => {
                    write_half.write_all(&push).await?;
                    write_half.write_all(b"\n").await?;
                    write_half.flush().await?;
                    continue;
                }
                ReadEvent::Push(None) | ReadEvent::Line(Ok(None)) => break,
                ReadEvent::Line(Err(error)) => return Err(error),
                ReadEvent::Line(Ok(Some(line))) => line,
            };
            if line == CONTROL_PING {
                log_control(
                    if kind == "tls" { "TLS" } else { "TCP" },
                    peer,
                    "ping",
                    "pong",
                );
                write_half.write_all(CONTROL_PONG).await?;
                write_half.write_all(b"\n").await?;
                write_half.flush().await?;
                continue;
            }
            process_message(
                &protocol,
                &mut write_half,
                peer,
                line,
                session.context(),
                kind,
            )
            .await?;
        }
        write_half.shutdown().await
    };
    tokio::select! {
        biased;
        _ = protocol.session_cancelled(session.context()) => Ok(()),
        result = serving => result,
    }
}

enum ReadEvent {
    Line(io::Result<Option<Bytes>>),
    Push(Option<bytes::Bytes>),
}

async fn process_message<W: AsyncWrite + Unpin>(
    protocol: &Arc<dyn JsonRpcProtocol>,
    writer: &mut W,
    peer: std::net::SocketAddr,
    payload: Bytes,
    context: &json_rpc_2_transport::TransportContext,
    kind: &'static str,
) -> io::Result<()> {
    let started = Instant::now();
    let response = protocol
        .handle_owned_message(payload.clone(), context)
        .await;
    log_json_rpc(
        if kind == "tls" { "TLS" } else { "TCP" },
        peer,
        &payload,
        response.as_ref().map(|message| message.payload.as_ref()),
        started.elapsed(),
    );
    if let Some(response) = response {
        write_message(writer, &response, protocol.stream_framing()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use json_rpc_2_transport::ProtocolFuture;

    #[tokio::test]
    async fn cancellation_closes_an_idle_stream_and_releases_its_session() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Gate {
            disabled: CancellationToken,
            opened: tokio::sync::Notify,
            closed: AtomicUsize,
        }
        impl JsonRpcProtocol for Gate {
            fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
                panic!("idle stream must not dispatch")
            }
            fn session_opened(&self, _: &json_rpc_2_transport::TransportContext) {
                self.opened.notify_one();
            }
            fn session_closed(&self, _: &json_rpc_2_transport::TransportContext) {
                self.closed.fetch_add(1, Ordering::SeqCst);
            }
            fn session_cancelled<'a>(
                &'a self,
                _: &'a json_rpc_2_transport::TransportContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
                Box::pin(async move { self.disabled.cancelled().await })
            }
        }
        let protocol = Arc::new(Gate {
            disabled: CancellationToken::new(),
            opened: tokio::sync::Notify::new(),
            closed: AtomicUsize::new(0),
        });
        let (mut client, server) = tokio::io::duplex(128);
        let (reader, writer) = tokio::io::split(server);
        let task = tokio::spawn(serve_stream(
            reader,
            writer,
            protocol.clone(),
            CancellationToken::new(),
            "127.0.0.1:12345".parse().unwrap(),
            "tcp",
        ));
        protocol.opened.notified().await;
        protocol.disabled.cancel();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                tokio::io::AsyncReadExt::read(&mut client, &mut byte)
            )
            .await
            .unwrap()
            .unwrap(),
            0
        );
        task.await.unwrap().unwrap();
        assert_eq!(protocol.closed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dispatch_preserves_the_owned_input_allocation() {
        struct OwnedOnly(Bytes);
        impl JsonRpcProtocol for OwnedOnly {
            fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
                panic!("stream dispatch must use the owned protocol entry point");
            }

            fn handle_owned_message<'a>(
                &'a self,
                input: Bytes,
                _: &'a json_rpc_2_transport::TransportContext,
            ) -> json_rpc_2_transport::ProtocolMessageFuture<'a> {
                Box::pin(async move {
                    assert_eq!(input.as_ptr(), self.0.as_ptr());
                    None
                })
            }
        }
        let input = Bytes::from_static(b"\xffopaque\npayload");
        let protocol: Arc<dyn JsonRpcProtocol> = Arc::new(OwnedOnly(input.clone()));
        let session = TransportSession::new("tcp", true);
        process_message(
            &protocol,
            &mut tokio::io::sink(),
            "127.0.0.1:12345".parse().unwrap(),
            input,
            &session.context,
            "tcp",
        )
        .await
        .unwrap();
    }
}
