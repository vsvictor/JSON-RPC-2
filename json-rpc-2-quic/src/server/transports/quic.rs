//! Raw QUIC transport: each bidirectional stream carries one complete JSON
//! document and, unless it is a notification, one complete response.

use std::{io, sync::Arc, time::Instant};

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::JsonRpcProtocol;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, CancellationToken, QUIC_NO_ERROR, Transport, TransportFuture,
    TransportRuntime, log_control, log_json_rpc, log_transport_error,
};
use quinn::{Connection, Endpoint};

/// Raw QUIC transport using one JSON-RPC message per bidirectional stream.
pub struct QuicTransport {
    endpoint: Endpoint,
    runtime: Arc<TransportRuntime>,
}

impl QuicTransport {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl Transport for QuicTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.endpoint, protocol, shutdown, self.runtime))
    }
}

/// Accept QUIC connections on an endpoint configured by the caller.
///
/// After `shutdown` is cancelled the endpoint stops accepting, in-flight
/// streams are answered, connections are closed with code 0 and the endpoint
/// waits until the peers have acknowledged the close.
pub async fn serve(
    endpoint: Endpoint,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let connections = multiplexor.handle();
    loop {
        let incoming = tokio::select! {
            incoming = endpoint.accept() => match incoming {
                Some(incoming) => incoming,
                None => break,
            },
            _ = shutdown.cancelled() => break,
        };
        let protocol = protocol.clone();
        let shutdown = shutdown.clone();
        let runtime = runtime.clone();
        connections
            .spawn(async move {
                if let Ok(connection) = incoming.await {
                    let peer = connection.remote_address();
                    serve_connection(connection, peer, protocol, shutdown, runtime).await;
                }
            })
            .await?;
    }
    endpoint.set_server_config(None);
    shutdown.cancel();
    multiplexor.close().await;
    endpoint.wait_idle().await;
    Ok(())
}

/// Serve an accepted connection; the caller owns endpoint admission and shutdown.
pub async fn serve_connection(
    connection: Connection,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) {
    let mut session = json_rpc_2_transport::TransportSession::new("quic", true);
    let guard =
        json_rpc_2_transport::ProtocolSessionGuard::new(protocol.clone(), session.context.clone());
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let streams = multiplexor.handle();
    loop {
        let accepted = tokio::select! {
            accepted = connection.accept_bi() => accepted,
            Some(push) = session.push_receiver.recv() => {
                let connection = connection.clone();
                let stopping = shutdown.clone();
                if streams.spawn(async move {
                    let result = async {
                        let mut send = connection.open_uni().await.map_err(io::Error::other)?;
                        send.write_all(&push).await.map_err(io::Error::other)?;
                        send.finish().map_err(io::Error::other)?;
                        Ok::<_, io::Error>(())
                    };
                    tokio::select! {
                        result = result => if let Err(error) = result {
                            log_transport_error("QUIC", connection.remote_address(), "push stream", &error);
                        },
                        _ = stopping.cancelled() => {},
                        _ = connection.closed() => {},
                    }
                }).await.is_err() {
                    break;
                }
                continue;
            }
            _ = shutdown.cancelled() => break,
        };
        // An error means the client closed the connection.
        let Ok((send, recv)) = accepted else { break };
        let protocol = protocol.clone();
        let context = session.context.clone();
        if streams
            .spawn(async move {
                if let Err(error) = handle_stream(send, recv, peer, protocol, context).await {
                    log_transport_error("QUIC", peer, "stream", error.as_ref());
                }
            })
            .await
            .is_err()
        {
            break;
        }
    }
    drop(guard);
    drop(session.push_receiver);
    multiplexor.close().await;
    connection.close(QUIC_NO_ERROR.into(), b"server shutdown");
}

async fn handle_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    peer: std::net::SocketAddr,
    protocol: Arc<dyn JsonRpcProtocol>,
    context: json_rpc_2_transport::TransportContext,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request = recv.read_to_end(MAX_MESSAGE_BYTES).await?;
    if request == CONTROL_PING {
        log_control("QUIC", peer, "ping", "pong");
        send.write_all(CONTROL_PONG).await?;
        send.finish()?;
        let _ = send.stopped().await;
        return Ok(());
    }
    let started = Instant::now();
    let request = bytes::Bytes::from(request);
    let request_log = request.clone();
    let response = protocol.handle_owned_message(request, &context).await;
    log_json_rpc(
        "QUIC",
        peer,
        &request_log,
        response.as_ref().map(|message| {
            if message.header.is_empty() {
                message.payload.as_ref()
            } else {
                message.header.as_ref()
            }
        }),
        started.elapsed(),
    );
    if let Some(response) = response {
        send.write_all(&response.header).await?;
        send.write_all(&response.payload).await?;
    }
    send.finish()?;
    // Wait until the peer has received the whole response before the
    // connection may be closed.
    let _ = send.stopped().await;
    Ok(())
}
