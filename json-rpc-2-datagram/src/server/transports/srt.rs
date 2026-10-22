//! JSON-RPC over standard libsrt listener connections.

use std::{io, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use json_rpc_2_core::SrtConfig;
use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, Transport, TransportFuture, TransportRuntime, log_json_rpc,
    log_transport_error,
};
use tokio::{sync::Semaphore, task::JoinSet};

use crate::client::transports::srt::native::{Frame, Kind, NativeSocket};

/// Standard SRT listener. A Tokio UDP socket cannot be adopted: libsrt owns
/// its UDP endpoint and performs the SRT handshake, encryption, ARQ and pacing.
pub struct SrtTransport {
    listener: NativeSocket,
    runtime: Arc<TransportRuntime>,
}

impl SrtTransport {
    pub async fn bind(address: SocketAddr, config: SrtConfig) -> io::Result<Self> {
        Ok(Self {
            listener: NativeSocket::bind(address, &config)?,
            runtime: Arc::new(TransportRuntime::default()),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl Transport for SrtTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(async move {
            let mut sessions = JoinSet::new();
            let permits = Arc::new(Semaphore::new(self.runtime.limits.max_connections));
            let requests = Arc::new(Semaphore::new(self.runtime.limits.max_concurrent_requests));
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    Some(_) = sessions.join_next(), if !sessions.is_empty() => {},
                    accepted = self.listener.accept() => {
                        let socket = accepted?;
                        let Ok(permit) = permits.clone().try_acquire_owned() else {
                            socket.close();
                            continue;
                        };
                        let protocol = protocol.clone();
                        let runtime = self.runtime.clone();
                        let shutdown = shutdown.child_token();
                        let requests = requests.clone();
                        sessions.spawn(async move {
                            let _permit = permit;
                            runtime.metrics.session_started();
                            struct SessionMetric(Arc<TransportRuntime>);
                            impl Drop for SessionMetric {
                                fn drop(&mut self) { self.0.metrics.session_finished(); }
                            }
                            let _metric = SessionMetric(runtime.clone());
                            if let Err(error) = serve_session(socket, protocol, shutdown, runtime, requests).await {
                                log_transport_error("SRT", "peer", "session", &error);
                            }
                        });
                    }
                }
            }
            self.listener.close();
            if tokio::time::timeout(self.runtime.close_timeout(), async {
                while sessions.join_next().await.is_some() {}
            })
            .await
            .is_err()
            {
                sessions.abort_all();
                while sessions.join_next().await.is_some() {}
            }
            Ok(())
        })
    }
}

async fn serve_session(
    socket: NativeSocket,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
    requests: Arc<Semaphore>,
) -> io::Result<()> {
    let socket = Arc::new(socket);
    let mut session = json_rpc_2_transport::TransportSession::new("srt", true);
    let guard =
        json_rpc_2_transport::ProtocolSessionGuard::new(protocol.clone(), session.context.clone());
    let mut handlers = JoinSet::new();
    let result = async {
        'receiving: loop {
            let receive = socket.recv();
            tokio::pin!(receive);
            let frame = loop { tokio::select! {
                biased;
                _ = shutdown.cancelled() => break 'receiving,
                Some(result) = handlers.join_next(), if !handlers.is_empty() => {
                    result.map_err(io::Error::other)??;
                    if protocol.session_close_requested(&session.context) {
                        break 'receiving;
                    }
                    continue;
                }
                Some(payload) = session.push_receiver.recv() => {
                    socket.send(Frame { kind: Kind::Push, id: 0, payload }).await?;
                    continue;
                }
                result = &mut receive => break result?,
            }};
            match frame.kind {
                Kind::Ping => {
                    socket.send(Frame { kind: Kind::Pong, id: frame.id, payload: Bytes::new() }).await?;
                }
                Kind::Request => {
                    // Do not queue unbounded tasks when the global dispatcher is saturated.
                    let permit = tokio::select! {
                        _ = shutdown.cancelled() => break,
                        permit = requests.clone().acquire_owned() => permit.map_err(io::Error::other)?,
                    };
                    let socket = socket.clone();
                    let protocol = protocol.clone();
                    let context = session.context.clone();
                    let metrics = runtime.metrics.clone();
                    handlers.spawn(async move {
                        let _permit = permit;
                        metrics.stream_started();
                        struct RequestMetric(Arc<json_rpc_2_transport::RuntimeMetrics>);
                        impl Drop for RequestMetric {
                            fn drop(&mut self) { self.0.stream_finished(); }
                        }
                        let _metric = RequestMetric(metrics);
                        let started = std::time::Instant::now();
                        let response = protocol.handle_owned_message(frame.payload.clone(), &context)
                            .await.map(json_rpc_2_transport::TransportMessage::into_bytes);
                        log_json_rpc("SRT", "peer", &frame.payload, response.as_deref(), started.elapsed());
                        let (kind, payload) = match response {
                            Some(payload) => (Kind::Response, payload),
                            None => (Kind::Ack, Bytes::new()),
                        };
                        socket.send(Frame { kind, id: frame.id, payload }).await
                    });
                }
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected SRT client frame")),
            }
        }
        Ok(())
    }.await;
    drop(guard);
    drop(session.push_receiver);
    let result = match result {
        Ok(()) => {
            tokio::time::timeout(runtime.close_timeout(), async {
                while let Some(result) = handlers.join_next().await {
                    result.map_err(io::Error::other)??;
                }
                // Native send completion/transport ACK does not mean the peer
                // has consumed its TSBPD-delayed receive buffer. Wait for an
                // application barrier before sending libsrt's shutdown packet.
                socket
                    .send(Frame {
                        kind: Kind::Drain,
                        id: 0,
                        payload: Bytes::new(),
                    })
                    .await?;
                loop {
                    let frame = socket.recv().await?;
                    match frame.kind {
                        Kind::Drained if frame.id == 0 => return Ok(()),
                        Kind::Ping => {
                            socket
                                .send(Frame {
                                    kind: Kind::Pong,
                                    id: frame.id,
                                    payload: Bytes::new(),
                                })
                                .await?
                        }
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "unexpected SRT client frame during shutdown drain",
                            ));
                        }
                    }
                }
            })
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "SRT shutdown drain acknowledgment timed out",
                ))
            })
        }
        Err(error) => Err(error),
    };
    handlers.abort_all();
    socket.close();
    while handlers.join_next().await.is_some() {}
    result
}
