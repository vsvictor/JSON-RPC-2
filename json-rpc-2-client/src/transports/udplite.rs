//! Reliable JSON-RPC over UDP-Lite datagrams.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    net::UdpSocket,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{closed, transport_error};
use crate::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError,
};
use json_rpc_2_core::{
    ReliableUdpPacket, ReliableUdpPacketKind, UdpLiteConfig, UdpReliabilityConfig,
    decode_udp_packet, encode_udp_packet,
};

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

/// UDP-Lite client transport with application-level retransmit/ack logic.
pub struct UdpLiteClientTransport {
    address: SocketAddr,
    config: UdpReliabilityConfig,
    udp_lite: UdpLiteConfig,
    next_packet_id: AtomicU64,
    pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
    state: Mutex<Option<ReceiverState>>,
}

impl UdpLiteClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_configs(
            address,
            UdpReliabilityConfig::default(),
            UdpLiteConfig::default(),
        )
        .await
    }

    pub async fn connect_with_config(
        address: SocketAddr,
        config: UdpReliabilityConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_configs(address, config, UdpLiteConfig::default()).await
    }

    pub async fn connect_with_configs(
        address: SocketAddr,
        config: UdpReliabilityConfig,
        udp_lite: UdpLiteConfig,
    ) -> Result<Self, TransportError> {
        validate_config(&config)?;
        udp_lite.validate().map_err(TransportError::new)?;
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let state = Some(
            Self::open(address, config.clone(), udp_lite.clone(), Arc::clone(&pending)).await?,
        );
        Ok(Self {
            address,
            config,
            udp_lite,
            next_packet_id: AtomicU64::new(1),
            pending,
            state: Mutex::new(state),
        })
    }

    async fn open(
        address: SocketAddr,
        config: UdpReliabilityConfig,
        udp_lite: UdpLiteConfig,
        pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
    ) -> Result<ReceiverState, TransportError> {
        let socket = Arc::new(open_socket(address, &udp_lite)?);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_receiver(
            Arc::clone(&socket),
            pending,
            shutdown.clone(),
            config,
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
        packet: ReliableUdpPacket,
        terminal: ReliableUdpPacketKind,
        cancel: Option<CancellationToken>,
    ) -> Result<Option<Bytes>, TransportError> {
        let packet_id = packet.packet_id;
        let encoded = encode_udp_packet(&packet).map_err(TransportError::new)?;
        if encoded.len() > self.config.max_packet_size {
            return Err(TransportError::new(format!(
                "UDP-Lite packet exceeds configured packet limit ({} > {})",
                encoded.len(),
                self.config.max_packet_size
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
        terminal: ReliableUdpPacketKind,
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
                    Ok(Some(PendingEvent::Ack)) if terminal == ReliableUdpPacketKind::Ack => {
                        return Ok(None);
                    }
                    Ok(Some(PendingEvent::Response(response)))
                        if terminal == ReliableUdpPacketKind::Response =>
                    {
                        return Ok(Some(response));
                    }
                    Ok(Some(PendingEvent::Pong)) if terminal == ReliableUdpPacketKind::Pong => {
                        return Ok(None);
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => {
                        return Err(TransportError::new("UDP-Lite transport receiver stopped"));
                    }
                    Err(_) => break,
                }
            }
            if attempt == self.config.max_retransmits {
                return Err(TransportError::new(format!(
                    "UDP-Lite transport timed out waiting for {} after {} send attempt(s)",
                    packet_kind_name(terminal),
                    attempt + 1,
                )));
            }
        }
        Err(TransportError::new("UDP-Lite transport exhausted retries"))
    }
}

impl RpcClientTransport for UdpLiteClientTransport {
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
            let packet = ReliableUdpPacket::request(packet_id, message.to_vec());
            self.send_reliably(
                packet,
                if expects_response {
                    ReliableUdpPacketKind::Response
                } else {
                    ReliableUdpPacketKind::Ack
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
            let packet = ReliableUdpPacket::request(packet_id, message.to_vec());
            self.send_reliably(
                packet,
                if expects_response {
                    ReliableUdpPacketKind::Response
                } else {
                    ReliableUdpPacketKind::Ack
                },
                Some(cancel),
            )
            .await
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let packet_id = self.next_packet_id();
            let packet = ReliableUdpPacket::ping(packet_id);
            self.send_reliably(packet, ReliableUdpPacketKind::Pong, None)
                .await
                .map(|_| ())
        })
    }

    fn kind(&self) -> &'static str {
        "udplite"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = Self::open(
                self.address,
                self.config.clone(),
                self.udp_lite.clone(),
                Arc::clone(&self.pending),
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

fn open_socket(address: SocketAddr, udp_lite: &UdpLiteConfig) -> Result<UdpSocket, TransportError> {
    let local = SocketAddr::new(unspecified(address.ip()), 0);
    let socket = Socket::new(
        Domain::for_address(address),
        Type::DGRAM,
        Some(Protocol::from(libc::IPPROTO_UDPLITE)),
    )
    .map_err(transport_error)?;
    if address.is_ipv6() {
        socket.set_only_v6(true).map_err(transport_error)?;
    }
    apply_udp_lite_coverage(&socket, udp_lite).map_err(transport_error)?;
    socket.bind(&local.into()).map_err(transport_error)?;
    socket.connect(&address.into()).map_err(transport_error)?;
    socket.set_nonblocking(true).map_err(transport_error)?;
    UdpSocket::from_std(socket.into()).map_err(transport_error)
}

fn apply_udp_lite_coverage(socket: &Socket, udp_lite: &UdpLiteConfig) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::fd::AsRawFd;

        set_udp_lite_option(
            socket.as_raw_fd(),
            libc::UDPLITE_SEND_CSCOV,
            udp_lite.send_checksum_coverage,
        )?;
        set_udp_lite_option(
            socket.as_raw_fd(),
            libc::UDPLITE_RECV_CSCOV,
            udp_lite.recv_checksum_coverage,
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = (socket, udp_lite);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "UDP-Lite is only supported on linux/android in this crate",
        ))
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn set_udp_lite_option(
    fd: std::os::fd::RawFd,
    name: libc::c_int,
    coverage: u16,
) -> std::io::Result<()> {
    let value: libc::c_int = coverage.into();
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_UDPLITE,
            name,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

async fn run_receiver(
    socket: Arc<UdpSocket>,
    pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>>,
    shutdown: CancellationToken,
    config: UdpReliabilityConfig,
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
        let packet = match decode_udp_packet(&buffer[..size]) {
            Ok(packet) => packet,
            Err(_) => continue,
        };
        let event = match packet.kind {
            ReliableUdpPacketKind::Ack => PendingEvent::Ack,
            ReliableUdpPacketKind::Response => PendingEvent::Response(Bytes::from(packet.payload)),
            ReliableUdpPacketKind::Pong => PendingEvent::Pong,
            ReliableUdpPacketKind::Ping => {
                let pong = match encode_udp_packet(&ReliableUdpPacket::pong(packet.packet_id)) {
                    Ok(pong) => pong,
                    Err(_) => continue,
                };
                let _ = socket.send(&pong).await;
                continue;
            }
            ReliableUdpPacketKind::Request => continue,
        };
        if let Some(waiter) = pending.lock().await.get(&packet.packet_id).cloned() {
            let _ = waiter.send(event);
        }
    }
}

fn validate_config(config: &UdpReliabilityConfig) -> Result<(), TransportError> {
    if config.retransmit_interval.is_zero() {
        return Err(TransportError::new(
            "UDP-Lite retransmit_interval must be greater than zero",
        ));
    }
    if config.max_packet_size > 65_507 {
        return Err(TransportError::new(
            "UDP-Lite max_packet_size must not exceed 65507 bytes",
        ));
    }
    if config.max_packet_size < 64 {
        return Err(TransportError::new(
            "UDP-Lite max_packet_size must be at least 64 bytes",
        ));
    }
    if config.dedup_cache_capacity == 0 {
        return Err(TransportError::new(
            "UDP-Lite dedup_cache_capacity must be at least 1",
        ));
    }
    if config.dedup_cache_ttl.is_zero() {
        return Err(TransportError::new(
            "UDP-Lite dedup_cache_ttl must be greater than zero",
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

fn packet_kind_name(kind: ReliableUdpPacketKind) -> &'static str {
    match kind {
        ReliableUdpPacketKind::Request => "request",
        ReliableUdpPacketKind::Response => "response",
        ReliableUdpPacketKind::Ack => "ack",
        ReliableUdpPacketKind::Ping => "ping",
        ReliableUdpPacketKind::Pong => "pong",
    }
}
