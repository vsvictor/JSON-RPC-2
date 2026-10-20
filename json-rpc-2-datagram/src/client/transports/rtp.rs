//! JSON-RPC over RTP data packets with RTCP APP control messages.

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

use json_rpc_2_core::{
    RtcpControlPacket, RtcpControlPacketKind, RtpPacket, RtpPacketKind, UdpReliabilityConfig,
    decode_rtcp_control_packet, decode_rtp_packet, encode_rtcp_control_packet, encode_rtp_packet,
};
use json_rpc_2_transport::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError,
};
use json_rpc_2_transport::{closed, transport_error};

#[derive(Clone, Debug)]
enum PendingEvent {
    Ack,
    Response(Bytes),
    Pong,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingTerminal {
    Ack,
    Response,
    Pong,
}

struct ReceiverState {
    socket: Arc<UdpSocket>,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

struct EndpointState {
    ssrc: u32,
    next_sequence: AtomicU32,
    next_timestamp: AtomicU32,
}

impl EndpointState {
    fn new() -> Self {
        static NEXT_SSRC: AtomicU32 = AtomicU32::new(1);
        let seed = NEXT_SSRC.fetch_add(1, Ordering::Relaxed);
        Self {
            ssrc: 0x4A52_5032 ^ seed,
            next_sequence: AtomicU32::new(seed & u16::MAX as u32),
            next_timestamp: AtomicU32::new(seed.rotate_left(16)),
        }
    }

    fn data(&self, kind: RtpPacketKind, message_id: u64, payload: impl Into<Vec<u8>>) -> RtpPacket {
        RtpPacket {
            sequence_number: self.next_sequence.fetch_add(1, Ordering::Relaxed) as u16,
            timestamp: self.next_timestamp.fetch_add(3_000, Ordering::Relaxed),
            ssrc: self.ssrc,
            kind,
            message_id,
            payload: payload.into(),
        }
    }

    fn control(&self, kind: RtcpControlPacketKind, message_id: u64) -> RtcpControlPacket {
        RtcpControlPacket {
            kind,
            ssrc: self.ssrc,
            message_id,
        }
    }
}

/// RTP/RTCP client transport over a connected UDP socket.
pub struct RtpClientTransport {
    address: SocketAddr,
    config: UdpReliabilityConfig,
    endpoint: Arc<EndpointState>,
    next_message_id: AtomicU64,
    pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
    state: Mutex<Option<ReceiverState>>,
}

impl RtpClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_config(address, UdpReliabilityConfig::default()).await
    }

    pub async fn connect_with_config(
        address: SocketAddr,
        config: UdpReliabilityConfig,
    ) -> Result<Self, TransportError> {
        validate_config(&config)?;
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let endpoint = Arc::new(EndpointState::new());
        let state = Some(
            Self::open(
                address,
                config.clone(),
                Arc::clone(&pending),
                Arc::clone(&endpoint),
            )
            .await?,
        );
        Ok(Self {
            address,
            config,
            endpoint,
            next_message_id: AtomicU64::new(1),
            pending,
            state: Mutex::new(state),
        })
    }

    async fn open(
        address: SocketAddr,
        config: UdpReliabilityConfig,
        pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
        endpoint: Arc<EndpointState>,
    ) -> Result<ReceiverState, TransportError> {
        let socket = Arc::new(open_socket(address).await?);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_receiver(
            Arc::clone(&socket),
            pending,
            shutdown.clone(),
            config,
            endpoint,
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

    fn next_message_id(&self) -> u64 {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_pending(&self, message_id: u64) -> mpsc::UnboundedReceiver<PendingEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending.lock().await.insert(message_id, tx);
        rx
    }

    async fn clear_pending(&self, message_id: u64) {
        self.pending.lock().await.remove(&message_id);
    }

    async fn fail_all_pending(&self) {
        self.pending.lock().await.clear();
    }

    async fn send_reliably(
        &self,
        message_id: u64,
        encoded: Vec<u8>,
        terminal: PendingTerminal,
        cancel: Option<CancellationToken>,
    ) -> Result<Option<Bytes>, TransportError> {
        if encoded.len() > self.config.max_packet_size {
            return Err(TransportError::new(format!(
                "RTP/RTCP packet exceeds configured packet limit ({} > {})",
                encoded.len(),
                self.config.max_packet_size
            )));
        }
        let mut pending = self.register_pending(message_id).await;
        let result = self
            .send_reliably_inner(encoded, terminal, cancel.as_ref(), &mut pending)
            .await;
        self.clear_pending(message_id).await;
        result
    }

    async fn send_reliably_inner(
        &self,
        encoded: Vec<u8>,
        terminal: PendingTerminal,
        cancel: Option<&CancellationToken>,
        pending: &mut mpsc::UnboundedReceiver<PendingEvent>,
    ) -> Result<Option<Bytes>, TransportError> {
        let socket = self.current_socket().await?;
        let interval = self.config.retransmit_interval;
        for attempt in 0..=self.config.max_retransmits {
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
                    Ok(Some(PendingEvent::Ack)) if terminal == PendingTerminal::Ack => {
                        return Ok(None);
                    }
                    Ok(Some(PendingEvent::Response(response)))
                        if terminal == PendingTerminal::Response =>
                    {
                        return Ok(Some(response));
                    }
                    Ok(Some(PendingEvent::Pong)) if terminal == PendingTerminal::Pong => {
                        return Ok(None);
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => return Err(TransportError::new("RTP transport receiver stopped")),
                    Err(_) => break,
                }
            }
            if attempt == self.config.max_retransmits {
                return Err(TransportError::new(format!(
                    "RTP/RTCP transport timed out waiting for {} after {} send attempt(s)",
                    terminal_name(terminal),
                    attempt + 1,
                )));
            }
        }
        Err(TransportError::new("RTP/RTCP transport exhausted retries"))
    }
}

impl RpcClientTransport for RtpClientTransport {
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
            let message_id = self.next_message_id();
            let packet = self
                .endpoint
                .data(RtpPacketKind::Request, message_id, message.to_vec());
            let encoded = encode_rtp_packet(&packet).map_err(TransportError::new)?;
            self.send_reliably(
                message_id,
                encoded,
                if expects_response {
                    PendingTerminal::Response
                } else {
                    PendingTerminal::Ack
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
            let message_id = self.next_message_id();
            let packet = self
                .endpoint
                .data(RtpPacketKind::Request, message_id, message.to_vec());
            let encoded = encode_rtp_packet(&packet).map_err(TransportError::new)?;
            self.send_reliably(
                message_id,
                encoded,
                if expects_response {
                    PendingTerminal::Response
                } else {
                    PendingTerminal::Ack
                },
                Some(cancel),
            )
            .await
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let message_id = self.next_message_id();
            let packet = self
                .endpoint
                .control(RtcpControlPacketKind::Ping, message_id);
            let encoded = encode_rtcp_control_packet(&packet).map_err(TransportError::new)?;
            self.send_reliably(message_id, encoded, PendingTerminal::Pong, None)
                .await
                .map(|_| ())
        })
    }

    fn kind(&self) -> &'static str {
        "rtp"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = Self::open(
                self.address,
                self.config.clone(),
                Arc::clone(&self.pending),
                Arc::clone(&self.endpoint),
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
    config: UdpReliabilityConfig,
    endpoint: Arc<EndpointState>,
) {
    let mut buffer = vec![0u8; config.max_packet_size];
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
        if let Ok(packet) = decode_rtcp_control_packet(&buffer[..size]) {
            let event = match packet.kind {
                RtcpControlPacketKind::Ack => PendingEvent::Ack,
                RtcpControlPacketKind::Pong => PendingEvent::Pong,
                RtcpControlPacketKind::Ping => {
                    let pong = match encode_rtcp_control_packet(
                        &endpoint.control(RtcpControlPacketKind::Pong, packet.message_id),
                    ) {
                        Ok(pong) => pong,
                        Err(_) => continue,
                    };
                    let _ = socket.send(&pong).await;
                    continue;
                }
            };
            if let Some(waiter) = pending.lock().await.get(&packet.message_id).cloned() {
                let _ = waiter.send(event);
            }
            continue;
        }
        let packet = match decode_rtp_packet(&buffer[..size]) {
            Ok(packet) => packet,
            Err(_) => continue,
        };
        if packet.kind != RtpPacketKind::Response {
            continue;
        }
        if let Some(waiter) = pending.lock().await.get(&packet.message_id).cloned() {
            let _ = waiter.send(PendingEvent::Response(Bytes::from(packet.payload)));
        }
    }
}

fn validate_config(config: &UdpReliabilityConfig) -> Result<(), TransportError> {
    if config.retransmit_interval.is_zero() {
        return Err(TransportError::new(
            "RTP/RTCP retransmit_interval must be greater than zero",
        ));
    }
    if config.max_packet_size > 65_507 {
        return Err(TransportError::new(
            "RTP/RTCP max_packet_size must not exceed 65507 bytes",
        ));
    }
    if config.max_packet_size < 64 {
        return Err(TransportError::new(
            "RTP/RTCP max_packet_size must be at least 64 bytes",
        ));
    }
    if config.dedup_cache_capacity == 0 {
        return Err(TransportError::new(
            "RTP/RTCP dedup_cache_capacity must be at least 1",
        ));
    }
    if config.dedup_cache_ttl.is_zero() {
        return Err(TransportError::new(
            "RTP/RTCP dedup_cache_ttl must be greater than zero",
        ));
    }
    Ok(())
}

fn unspecified(ip: IpAddr) -> IpAddr {
    if ip.is_ipv6() {
        IpAddr::V6(Ipv6Addr::UNSPECIFIED)
    } else {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }
}

fn terminal_name(terminal: PendingTerminal) -> &'static str {
    match terminal {
        PendingTerminal::Ack => "ack",
        PendingTerminal::Response => "response",
        PendingTerminal::Pong => "pong",
    }
}
