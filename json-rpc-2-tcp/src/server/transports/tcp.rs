//! JSON lines and binary gateway frames over TCP.

use std::{io, sync::Arc, time::Instant};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[cfg(feature = "tcp")]
use crate::client::transports::tcp::{read_frame, write_message};
#[cfg(not(feature = "tcp"))]
use crate::client::transports::tls::{read_frame, write_message};
use bytes::{Bytes, BytesMut};
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::JsonRpcProtocol;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, CancellationToken, GatewayRequestMetadata, ProtocolSessionGuard,
    Transport, TransportFuture, TransportRuntime, TransportSession, apply_tcp_stream_tuning,
    log_control, log_json_rpc, log_transport_error,
};

/// TCP transport accepting JSON lines and binary gateway frames.
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

/// Serve the JSON-lines/binary gateway protocol on an already accepted stream.
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
    let mut gateway_metadata = None;
    loop {
        let event = tokio::select! {
            line = read_frame(&mut reader, &mut buffer) => ReadEvent::Line(line),
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
        let metadata = std::str::from_utf8(&line)
            .ok()
            .map(GatewayRequestMetadata::parse)
            .transpose()?
            .flatten();
        if let Some(metadata) = metadata {
            if let Some(payload_length) = metadata.payload_length {
                if payload_length > MAX_MESSAGE_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "legacy gateway payload exceeds maximum message size",
                    ));
                }
                let mut payload = vec![0; payload_length];
                let buffered = payload_length.min(buffer.len());
                payload[..buffered].copy_from_slice(&buffer.split_to(buffered));
                reader.read_exact(&mut payload[buffered..]).await?;
                let message_context = metadata.context(None);
                process_message(
                    &protocol,
                    &mut write_half,
                    peer,
                    Bytes::from(payload),
                    &message_context,
                    kind,
                )
                .await?;
            } else {
                gateway_metadata = Some(metadata);
            }
            continue;
        }
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
        let message_context = gateway_metadata
            .take()
            .map(|metadata| metadata.context(None))
            .unwrap_or_else(|| session.context().clone());
        process_message(
            &protocol,
            &mut write_half,
            peer,
            line,
            &message_context,
            kind,
        )
        .await?;
    }
    write_half.shutdown().await
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
        write_message(writer, &response).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use json_rpc_2_transport::{
        ProtocolFuture,
        gateway::{GatewayBackendProtocol, decode_response, encode_request},
    };

    struct Echo;

    impl JsonRpcProtocol for Echo {
        fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
            Box::pin(async move { Some(Bytes::copy_from_slice(input)) })
        }
    }

    #[tokio::test]
    async fn binary_exchanges_can_be_followed_by_legacy_control_lines() {
        for kind in ["tcp", "tls"] {
            let (mut client, server) = tokio::io::duplex(256);
            let (reader, writer) = tokio::io::split(server);
            let shutdown = CancellationToken::new();
            let task = tokio::spawn(serve_stream(
                reader,
                writer,
                Arc::new(GatewayBackendProtocol::new(Arc::new(Echo))),
                shutdown.clone(),
                "127.0.0.1:12345".parse().unwrap(),
                kind,
            ));
            let payload = Bytes::from_static(b"\xffopaque\npayload\r\n\0");
            let request = encode_request(&payload, "client-1", kind).unwrap();
            write_message(&mut client, &request).await.unwrap();
            let mut buffer = BytesMut::new();
            let response = read_frame(&mut client, &mut buffer).await.unwrap().unwrap();
            assert_eq!(decode_response(response).unwrap().unwrap(), payload);
            write_message(&mut client, &Bytes::from_static(CONTROL_PING).into())
                .await
                .unwrap();
            assert_eq!(
                read_frame(&mut client, &mut buffer).await.unwrap().unwrap(),
                CONTROL_PONG
            );
            shutdown.cancel();
            task.await.unwrap().unwrap();
        }
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
