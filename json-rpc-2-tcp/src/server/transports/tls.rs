//! JSON lines and application-defined binary frames over TLS-wrapped TCP.

use std::{io, sync::Arc};

use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

use json_rpc_2_transport::JsonRpcProtocol;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    CancellationToken, Transport, TransportFuture, TransportRuntime, apply_tcp_stream_tuning,
    log_transport_error,
};

#[cfg(not(feature = "tcp"))]
#[allow(dead_code)]
#[path = "tcp.rs"]
mod tcp;
#[cfg(feature = "tcp")]
use super::tcp;

/// TLS transport accepting JSON lines and application-defined binary frames.
pub struct TlsTransport {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    runtime: Arc<TransportRuntime>,
}

impl TlsTransport {
    pub fn new(listener: TcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            listener,
            acceptor: TlsAcceptor::from(config),
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl Transport for TlsTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(
            self.listener,
            self.acceptor,
            protocol,
            shutdown,
            self.runtime,
        ))
    }
}

pub async fn serve(
    listener: TcpListener,
    acceptor: TlsAcceptor,
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
        let acceptor = acceptor.clone();
        connections
            .spawn(async move {
                if let Err(error) =
                    handle_connection(stream, acceptor, protocol, shutdown, runtime).await
                {
                    log_transport_error("TLS", "listener", "connection", &error);
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
    acceptor: TlsAcceptor,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    apply_tcp_stream_tuning(&stream, &runtime.tcp_tuning)?;
    let peer = stream.peer_addr()?;
    let stream = tokio::select! {
        result = acceptor.accept(stream) => result?,
        _ = shutdown.cancelled() => return Ok(()),
    };
    let (reader, writer) = tokio::io::split(stream);
    match tcp::serve_stream(reader, writer, protocol, shutdown, peer, "tls").await {
        Err(error) if error.kind() != io::ErrorKind::NotConnected => Err(error),
        _ => Ok(()),
    }
}
