//! Newline-delimited JSON over TCP (one compact JSON document per line).

use std::{io, sync::Arc, time::Instant};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::JsonRpcProtocol;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, CancellationToken, GatewayRequestMetadata, ProtocolSessionGuard,
    Transport, TransportFuture, TransportRuntime, TransportSession, apply_tcp_stream_tuning,
    log_control, log_json_rpc, log_transport_error,
};

/// TCP transport using one compact JSON message per newline-delimited line.
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

/// Accept TCP connections and delegate each complete line to the protocol.
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
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let TransportSession {
        context,
        mut push_receiver,
    } = TransportSession::new("tcp", true);
    let session = ProtocolSessionGuard::new(protocol.clone(), context);
    let mut gateway_metadata = None;
    loop {
        let event = tokio::select! {
            line = read_limited_line(&mut reader) => ReadEvent::Line(line),
            push = push_receiver.recv() => ReadEvent::Push(push),
            _ = shutdown.cancelled() => break,
        };
        let mut line = match event {
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
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        let metadata = std::str::from_utf8(&line)
            .ok()
            .map(GatewayRequestMetadata::parse)
            .transpose()?
            .flatten();
        if let Some(metadata) = metadata {
            if let Some(payload_length) = metadata.payload_length {
                let mut payload = vec![0; payload_length];
                reader.read_exact(&mut payload).await?;
                let message_context = metadata.context(None);
                process_message(&protocol, &mut write_half, peer, &payload, &message_context)
                    .await?;
            } else {
                gateway_metadata = Some(metadata);
            }
            continue;
        }
        if line == CONTROL_PING {
            log_control("TCP", peer, "ping", "pong");
            write_half.write_all(&CONTROL_PONG).await?;
            write_half.write_all(b"\n").await?;
            write_half.flush().await?;
            continue;
        }
        let message_context = gateway_metadata
            .take()
            .map(|metadata| metadata.context(None))
            .unwrap_or_else(|| session.context().clone());
        process_message(&protocol, &mut write_half, peer, &line, &message_context).await?;
    }
    write_half.shutdown().await
}

enum ReadEvent {
    Line(io::Result<Option<Vec<u8>>>),
    Push(Option<bytes::Bytes>),
}

async fn read_limited_line<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let read = reader
        .take((MAX_MESSAGE_BYTES + 2) as u64)
        .read_until(b'\n', &mut line)
        .await?;
    if read == 0 {
        return Ok(None);
    }
    if read > MAX_MESSAGE_BYTES + 1 || line.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TCP line exceeds the maximum message size",
        ));
    }
    Ok(Some(line))
}

async fn process_message(
    protocol: &Arc<dyn JsonRpcProtocol>,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    peer: std::net::SocketAddr,
    payload: &[u8],
    context: &json_rpc_2_transport::TransportContext,
) -> io::Result<()> {
    let started = Instant::now();
    let response = protocol.handle_message_with_context(payload, context).await;
    log_json_rpc("TCP", peer, payload, response.as_deref(), started.elapsed());
    if let Some(response) = response {
        writer.write_all(&response).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}
