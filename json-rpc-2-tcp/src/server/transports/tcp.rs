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
mod tests;
