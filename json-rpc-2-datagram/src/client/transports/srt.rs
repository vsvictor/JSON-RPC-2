//! Secure reliable JSON-RPC over SRT-style UDP datagrams.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use tokio::{
    net::UdpSocket,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use json_rpc_2_core::{closed, transport_error};
use json_rpc_2_core::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError,
};
use json_rpc_2_core::{SrtConfig, SrtCrypto, SrtPacket, SrtPacketKind, UdpReliabilityConfig};

#[derive(Clone, Debug)]
enum PendingEvent {
    Ack,
    Response(Bytes),
    Pong,
}

struct ReceiverState {
    socket: Arc<UdpSocket>,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

/// SRT client transport with encrypted payloads and application-level ARQ.
pub struct SrtClientTransport {
    address: SocketAddr,
    reliability: UdpReliabilityConfig,
    crypto: Arc<SrtCrypto>,
    next_sequence: AtomicU32,
    next_packet_id: AtomicU64,
    pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
    state: Mutex<Option<ReceiverState>>,
}

impl SrtClientTransport {
    pub async fn connect(
        address: SocketAddr,
        passphrase: impl Into<String>,
    ) -> Result<Self, TransportError> {
        Self::connect_with_config(
            address,
            UdpReliabilityConfig::default(),
            SrtConfig {
                passphrase: passphrase.into(),
            },
        )
        .await
    }

    pub async fn connect_with_config(
        address: SocketAddr,
        reliability: UdpReliabilityConfig,
        srt_config: SrtConfig,
    ) -> Result<Self, TransportError> {
        validate_config(&reliability)?;
        srt_config.validate().map_err(TransportError::new)?;
        let crypto = Arc::new(SrtCrypto::new(&srt_config).map_err(TransportError::new)?);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let state = Some(
            Self::open(
                address,
                reliability.clone(),
                Arc::clone(&pending),
                Arc::clone(&crypto),
            )
            .await?,
        );
        Ok(Self {
            address,
            reliability,
            crypto,
            next_sequence: AtomicU32::new(1),
            next_packet_id: AtomicU64::new(1),
            pending,
            state: Mutex::new(state),
        })
    }

    async fn open(
        address: SocketAddr,
        reliability: UdpReliabilityConfig,
        pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
        crypto: Arc<SrtCrypto>,
    ) -> Result<ReceiverState, TransportError> {
        let socket = Arc::new(open_socket(address).await?);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_receiver(
            Arc::clone(&socket),
            pending,
            shutdown.clone(),
            reliability,
            crypto,
        ));
        Ok(ReceiverState {
            socket,
            shutdown,
            task,
        })
    }

    async fn current_socket(&self) -> Result<Arc<UdpSocket>, TransportError> {
        self.state
            .lock()
            .await
            .as_ref()
            .map(|state| Arc::clone(&state.socket))
            .ok_or_else(closed)
    }

    fn next_sequence(&self) -> u32 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn next_packet_id(&self) -> u64 {
        self.next_packet_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_pending(&self, packet_id: u64) -> mpsc::UnboundedReceiver<PendingEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending.lock().await.insert(packet_id, tx);
        rx
    }

    async fn clear_pending(&self, packet_id: u64) {
        self.pending.lock().await.remove(&packet_id);
    }

    async fn fail_all_pending(&self) {
        self.pending.lock().await.clear();
    }

    async fn send_reliably(
        &self,
        packet: SrtPacket,
        terminal: SrtPacketKind,
        cancel: Option<CancellationToken>,
    ) -> Result<Option<Bytes>, TransportError> {
        let packet_id = packet.packet_id;
        let encoded = self
            .crypto
            .encode_packet(&packet)
            .map_err(TransportError::new)?;
        if encoded.len() > self.reliability.max_packet_size {
            return Err(TransportError::new(format!(
                "SRT packet exceeds configured packet limit ({} > {})",
                encoded.len(),
                self.reliability.max_packet_size
            )));
        }
        let mut pending = self.register_pending(packet_id).await;
        let result = self
            .send_reliably_inner(encoded, terminal, cancel.as_ref(), &mut pending)
            .await;
        self.clear_pending(packet_id).await;
        result
    }

    async fn send_reliably_inner(
        &self,
        encoded: Vec<u8>,
        terminal: SrtPacketKind,
        cancel: Option<&CancellationToken>,
        pending: &mut mpsc::UnboundedReceiver<PendingEvent>,
    ) -> Result<Option<Bytes>, TransportError> {
        let socket = self.current_socket().await?;
        let interval = self.reliability.retransmit_interval;
        for attempt in 0..=self.reliability.max_retransmits {
            socket.send(&encoded).await.map_err(transport_error)?;
            let deadline = Instant::now() + interval;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let event = match cancel {
                    Some(cancel) => {
                        tokio::select! {
                            _ = cancel.cancelled() => return Err(TransportError::new("request cancelled")),
                            event = tokio::time::timeout(remaining, pending.recv()) => event,
                        }
                    }
                    None => tokio::time::timeout(remaining, pending.recv()).await,
                };
                match event {
                    Ok(Some(PendingEvent::Ack)) if terminal == SrtPacketKind::Ack => {
                        return Ok(None);
                    }
                    Ok(Some(PendingEvent::Response(response)))
                        if terminal == SrtPacketKind::Response =>
                    {
                        return Ok(Some(response));
                    }
                    Ok(Some(PendingEvent::Pong)) if terminal == SrtPacketKind::Pong => {
                        return Ok(None);
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => return Err(TransportError::new("SRT transport receiver stopped")),
                    Err(_) => break,
                }
            }
            if attempt == self.reliability.max_retransmits {
                return Err(TransportError::new(format!(
                    "SRT transport timed out waiting for {} after {} send attempt(s)",
                    packet_kind_name(terminal),
                    attempt + 1,
                )));
            }
        }
        Err(TransportError::new("SRT transport exhausted retries"))
    }
}

impl RpcClientTransport for SrtClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects_response = crate::expects_response(&message);
            self.send_with_expectation(message, expects_response).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let packet_id = self.next_packet_id();
            let packet = SrtPacket::request(self.next_sequence(), packet_id, message.to_vec());
            self.send_reliably(
                packet,
                if expects_response {
                    SrtPacketKind::Response
                } else {
                    SrtPacketKind::Ack
                },
                None,
            )
            .await
        })
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let packet_id = self.next_packet_id();
            let packet = SrtPacket::request(self.next_sequence(), packet_id, message.to_vec());
            self.send_reliably(
                packet,
                if expects_response {
                    SrtPacketKind::Response
                } else {
                    SrtPacketKind::Ack
                },
                Some(cancel),
            )
            .await
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let packet_id = self.next_packet_id();
            let packet = SrtPacket::ping(self.next_sequence(), packet_id);
            self.send_reliably(packet, SrtPacketKind::Pong, None)
                .await
                .map(|_| ())
        })
    }

    fn kind(&self) -> &'static str {
        "srt"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = Self::open(
                self.address,
                self.reliability.clone(),
                Arc::clone(&self.pending),
                Arc::clone(&self.crypto),
            )
            .await?;
            let previous = self.state.lock().await.replace(replacement);
            self.fail_all_pending().await;
            if let Some(state) = previous {
                state.shutdown.cancel();
                let _ = state.task.await;
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let previous = self.state.lock().await.take();
            self.fail_all_pending().await;
            if let Some(state) = previous {
                state.shutdown.cancel();
                let _ = state.task.await;
            }
            Ok(())
        })
    }
}

async fn open_socket(address: SocketAddr) -> Result<UdpSocket, TransportError> {
    let local = SocketAddr::new(unspecified(address.ip()), 0);
    let socket = UdpSocket::bind(local).await.map_err(transport_error)?;
    socket.connect(address).await.map_err(transport_error)?;
    Ok(socket)
}

async fn run_receiver(
    socket: Arc<UdpSocket>,
    pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
    shutdown: CancellationToken,
    reliability: UdpReliabilityConfig,
    crypto: Arc<SrtCrypto>,
) {
    let mut buffer = vec![0u8; reliability.max_packet_size];
    loop {
        let received = tokio::select! {
            received = socket.recv(&mut buffer) => Some(received),
            _ = shutdown.cancelled() => None,
        };
        let Some(received) = received else { break };
        let size = match received {
            Ok(size) => size,
            Err(_) => break,
        };
        let packet = match crypto.decode_packet(&buffer[..size]) {
            Ok(packet) => packet,
            Err(_) => continue,
        };
        let event = match packet.kind {
            SrtPacketKind::Ack => PendingEvent::Ack,
            SrtPacketKind::Response => PendingEvent::Response(Bytes::from(packet.payload)),
            SrtPacketKind::Pong => PendingEvent::Pong,
            SrtPacketKind::Request | SrtPacketKind::Ping => continue,
        };
        if let Some(sender) = pending.lock().await.get(&packet.packet_id).cloned() {
            let _ = sender.send(event);
        }
    }
}

fn validate_config(config: &UdpReliabilityConfig) -> Result<(), TransportError> {
    if config.max_retransmits == 0 {
        return Err(TransportError::new(
            "SRT max_retransmits must be at least 1",
        ));
    }
    if config.retransmit_interval.is_zero() {
        return Err(TransportError::new(
            "SRT retransmit_interval must be greater than zero",
        ));
    }
    if config.max_packet_size == 0 {
        return Err(TransportError::new(
            "SRT max_packet_size must be greater than zero",
        ));
    }
    Ok(())
}

fn packet_kind_name(kind: SrtPacketKind) -> &'static str {
    match kind {
        SrtPacketKind::Request => "request",
        SrtPacketKind::Response => "response",
        SrtPacketKind::Ack => "ack",
        SrtPacketKind::Ping => "pong",
        SrtPacketKind::Pong => "pong",
    }
}

fn unspecified(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}
