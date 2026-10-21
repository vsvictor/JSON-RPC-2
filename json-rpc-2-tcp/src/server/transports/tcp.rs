//! Newline-delimited JSON over TCP (one compact JSON document per line).

use std::{io, sync::Arc, time::Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::{Framed, LinesCodec};

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
    let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(MAX_MESSAGE_BYTES));
    let TransportSession {
        context,
        mut push_receiver,
    } = TransportSession::new("tcp", true);
    let session = ProtocolSessionGuard::new(protocol.clone(), context);
    let mut gateway_metadata = None;
    loop {
        let event = tokio::select! {
            line = framed.next() => (line, None),
            push = push_receiver.recv() => (None, Some(push)),
            _ = shutdown.cancelled() => break,
        };
        if let Some(push) = event.1 {
            let Some(push) = push else { break };
            let push = String::from_utf8(push.to_vec())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            framed
                .send(push)
                .await
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            continue;
        }
        // `None` means the client closed its side.
        let Some(line) = event.0 else { break };
        let line = line.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if let Some(metadata) = GatewayRequestMetadata::parse(&line)? {
            gateway_metadata = Some(metadata);
            continue;
        }
        if line.as_bytes() == CONTROL_PING {
            log_control("TCP", peer, "ping", "pong");
            framed
                .send(String::from_utf8(CONTROL_PONG.to_vec()).expect("ASCII control payload"))
                .await
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            continue;
        }
        let started = Instant::now();
        let message_context = gateway_metadata
            .take()
            .map(|metadata| metadata.context(None))
            .unwrap_or_else(|| session.context().clone());
        let response = protocol
            .handle_message_with_context(line.as_bytes(), &message_context)
            .await;
        log_json_rpc(
            "TCP",
            peer,
            line.as_bytes(),
            response.as_deref(),
            started.elapsed(),
        );
        if let Some(response) = response {
            let response = String::from_utf8(response.to_vec())
                .expect("serialized JSON-RPC responses are valid UTF-8");
            framed
                .send(response)
                .await
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        }
    }
    let mut stream = framed.into_inner();
    match stream.shutdown().await {
        Err(error) if error.kind() != io::ErrorKind::NotConnected => Err(error),
        _ => Ok(()),
    }
}
