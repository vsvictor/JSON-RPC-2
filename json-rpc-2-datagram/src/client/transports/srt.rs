//! JSON-RPC over standard, encrypted libsrt caller connections.

#[path = "../../srt_native.rs"]
pub(crate) mod native;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use json_rpc_2_core::{SrtConfig, UdpReliabilityConfig};
use json_rpc_2_transport::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, closed, expects_response,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use native::{Frame, Kind, NativeSocket};

type Pending = Arc<StdMutex<HashMap<u64, oneshot::Sender<Result<Frame, String>>>>>;

/// Reserved native SRTO_STREAMID profile for WebSocket frames over SRT.
pub const WS_SRT_STREAM_ID: &str = "json-rpc-2/ws-srt/1";
/// Optional explicit raw profile. An empty stream ID remains the legacy raw profile.
pub const RAW_SRT_STREAM_ID: &str = "json-rpc-2/srt/1";

struct Connection {
    socket: Arc<NativeSocket>,
    pending: Pending,
    stop: CancellationToken,
    push_receiver: Mutex<mpsc::Receiver<Bytes>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.stop.cancel();
        self.socket.close();
        self.pending.lock().unwrap().clear();
    }
}

struct Registration {
    pending: Pending,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

/// Multiplexed caller. Reliability and encryption are provided exclusively by libsrt.
pub struct SrtClientTransport {
    address: SocketAddr,
    config: SrtConfig,
    stream_id: String,
    timeout: Duration,
    next_id: AtomicU64,
    state: Mutex<Option<Arc<Connection>>>,
}

impl SrtClientTransport {
    pub async fn connect(
        address: SocketAddr,
        passphrase: impl Into<String>,
    ) -> Result<Self, TransportError> {
        Self::connect_with_config(
            address,
            // Live SRT's default 120ms playout latency applies in each
            // direction. UDP's 300ms ARQ budget is not a useful RPC default.
            UdpReliabilityConfig {
                retransmit_interval: Duration::from_secs(1),
                ..Default::default()
            },
            SrtConfig {
                passphrase: passphrase.into(),
            },
        )
        .await
    }

    /// The legacy reliability configuration supplies the operation timeout only.
    /// libsrt handles retransmissions; application requests are never replayed.
    pub async fn connect_with_config(
        address: SocketAddr,
        reliability: UdpReliabilityConfig,
        config: SrtConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_stream_id(address, reliability, config, "").await
    }

    /// Set native SRTO_STREAMID before the encrypted handshake, retaining it on
    /// reconnect. Shared listeners use it to select a protocol before session_opened.
    pub async fn connect_with_stream_id(
        address: SocketAddr,
        reliability: UdpReliabilityConfig,
        config: SrtConfig,
        stream_id: impl Into<String>,
    ) -> Result<Self, TransportError> {
        let stream_id = stream_id.into();
        let timeout = reliability
            .retransmit_interval
            .checked_mul(reliability.max_retransmits.saturating_add(1))
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| {
                TransportError::new("SRT operation timeout must be positive and finite")
            })?;
        let connection = Self::open(address, &config, &stream_id).await?;
        Ok(Self {
            address,
            config,
            stream_id,
            timeout,
            next_id: AtomicU64::new(1),
            state: Mutex::new(Some(connection)),
        })
    }

    async fn open(
        address: SocketAddr,
        config: &SrtConfig,
        stream_id: &str,
    ) -> Result<Arc<Connection>, TransportError> {
        let socket = Arc::new(
            NativeSocket::connect(address, config, stream_id)
                .await
                .map_err(native::transport_error)?,
        );
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let stop = CancellationToken::new();
        let (pushes, push_receiver) = mpsc::channel(1024);
        let connection = Arc::new(Connection {
            socket: socket.clone(),
            pending: pending.clone(),
            stop: stop.clone(),
            push_receiver: Mutex::new(push_receiver),
        });
        tokio::spawn(async move {
            let error = loop {
                let result = tokio::select! {
                    _ = stop.cancelled() => break "SRT connection closed".to_owned(),
                    result = socket.recv() => result,
                };
                match result {
                    Ok(frame) if frame.kind == Kind::Push && frame.id == 0 => {
                        if let Err(error) = pushes.try_send(frame.payload) {
                            break format!("SRT push receiver unavailable: {error}");
                        }
                    }
                    Ok(frame) if frame.kind == Kind::Drain => {
                        // Ordered SRT messages ensure all prior responses have
                        // reached their pending callers before this barrier ACK.
                        if let Err(error) = socket
                            .send(Frame {
                                kind: Kind::Drained,
                                id: frame.id,
                                payload: Bytes::new(),
                            })
                            .await
                        {
                            break error.to_string();
                        }
                    }
                    Ok(frame) if matches!(frame.kind, Kind::Response | Kind::Ack | Kind::Pong) => {
                        if let Some(sender) = pending.lock().unwrap().remove(&frame.id) {
                            let _ = sender.send(Ok(frame));
                        }
                    }
                    Ok(_) => break "unexpected SRT server application frame".to_owned(),
                    Err(error) => break error.to_string(),
                }
            };
            socket.close();
            stop.cancel();
            for (_, sender) in pending.lock().unwrap().drain() {
                let _ = sender.send(Err(error.clone()));
            }
        });
        Ok(connection)
    }

    /// Receive a push independently of correlated request acknowledgements.
    pub async fn recv_push(&self) -> Result<Bytes, TransportError> {
        let connection = self.state.lock().await.clone().ok_or_else(closed)?;
        let mut receiver = connection.push_receiver.lock().await;
        tokio::select! {
            payload = receiver.recv() => payload.ok_or_else(closed),
            _ = connection.stop.cancelled() => Err(closed()),
        }
    }

    async fn exchange(
        &self,
        message: Bytes,
        kind: Kind,
        expects_response: bool,
        cancel: CancellationToken,
    ) -> Result<Option<Bytes>, TransportError> {
        let connection = self.state.lock().await.clone().ok_or_else(closed)?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        // Bound outstanding requests independently of remote peer behavior.
        {
            let mut pending = connection.pending.lock().unwrap();
            if pending.len() >= 1024 {
                return Err(TransportError::new("SRT outstanding request limit reached"));
            }
            pending.insert(id, tx);
        }
        let _registration = Registration {
            pending: connection.pending.clone(),
            id,
        };
        let operation = async {
            connection
                .socket
                .send(Frame {
                    kind,
                    id,
                    payload: message,
                })
                .await
                .map_err(native::transport_error)?;
            let frame = rx
                .await
                .map_err(|_| closed())?
                .map_err(TransportError::new)?;
            match (kind, frame.kind, expects_response) {
                (Kind::Ping, Kind::Pong, _) | (Kind::Request, Kind::Ack, false) => Ok(None),
                (Kind::Request, Kind::Response, true) => Ok(Some(frame.payload)),
                (Kind::Request, Kind::Response, false) => Ok(None),
                (Kind::Request, Kind::Ack, true) => Ok(None),
                _ => Err(TransportError::new(
                    "unexpected SRT application control frame",
                )),
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(TransportError::new("request cancelled")),
            _ = connection.stop.cancelled() => Err(closed()),
            result = tokio::time::timeout(self.timeout, operation) => {
                result.map_err(|_| TransportError::new("SRT operation timed out (request was not replayed)"))?
            }
        }
    }
}

impl RpcClientTransport for SrtClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        let expected = expects_response(&message);
        self.send_with_expectation(message, expected)
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expected: bool,
    ) -> ClientTransportFuture<'a> {
        self.send_cancelable(message, expected, CancellationToken::new())
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        expected: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        Box::pin(self.exchange(message, Kind::Request, expected, cancel))
    }

    fn kind(&self) -> &'static str {
        "srt"
    }
    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            self.exchange(Bytes::new(), Kind::Ping, false, CancellationToken::new())
                .await
                .map(|_| ())
        })
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            // Serialize reconnect/close; closing the old generation fails its pending calls.
            let mut state = self.state.lock().await;
            if let Some(old) = state.take() {
                old.stop.cancel();
                old.socket.close();
                old.pending.lock().unwrap().clear();
            }
            *state = Some(Self::open(self.address, &self.config, &self.stream_id).await?);
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            if let Some(old) = self.state.lock().await.take() {
                old.stop.cancel();
                old.socket.close();
                old.pending.lock().unwrap().clear();
            }
            Ok(())
        })
    }
}
