//! JSON-RPC over CoAP-style confirmable UDP messages.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
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
use json_rpc_2_core::{CoapMessage, CoapMessageType, UdpReliabilityConfig, decode_coap_message, encode_coap_message};

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

/// CoAP-inspired client transport using confirmable UDP exchanges.
pub struct CoapClientTransport {
    address: SocketAddr,
    config: UdpReliabilityConfig,
    next_message_id: AtomicU32,
    pending: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<PendingEvent>>>>,
    state: Mutex<Option<ReceiverState>>,
}

impl CoapClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_config(address, UdpReliabilityConfig::default()).await
    }

    pub async fn connect_with_config(
        address: SocketAddr,
        config: UdpReliabilityConfig,
    ) -> Result<Self, TransportError> {
        validate_config(&config)?;
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let state = Some(Self::open(address, config.clone(), Arc::clone(&pending)).await?);
        Ok(Self {
            address,
            config,
            next_message_id: AtomicU32::new(1),
            pending,
            state: Mutex::new(state),
        })
    }

    async fn open(
        address: SocketAddr,
        config: UdpReliabilityConfig,
        pending: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<PendingEvent>>>>,
    ) -> Result<ReceiverState, TransportError> {
        let socket = Arc::new(open_socket(address).await?);
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

    fn next_message_id(&self) -> u16 {
        self.next_message_id.fetch_add(1, Ordering::Relaxed) as u16
    }

    async fn current_socket(&self) -> Result<Arc<UdpSocket>, TransportError> {
        self.state
            .lock()
            .await
            .as_ref()
            .map(|state| Arc::clone(&state.socket))
            .ok_or_else(closed)
    }

    async fn register_pending(&self, message_id: u16) -> mpsc::UnboundedReceiver<PendingEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending.lock().await.insert(message_id, tx);
        rx
    }

    async fn clear_pending(&self, message_id: u16) {
        self.pending.lock().await.remove(&message_id);
    }

    async fn fail_all_pending(&self) {
        self.pending.lock().await.clear();
    }

    async fn send_reliably(
        &self,
        message: CoapMessage,
        expects_response: bool,
        ping: bool,
        cancel: Option<CancellationToken>,
    ) -> Result<Option<Bytes>, TransportError> {
        let encoded = encode_coap_message(&message).map_err(TransportError::new)?;
        let socket = self.current_socket().await?;
        let mut pending = self.register_pending(message.message_id).await;
        for attempt in 0..=self.config.max_retransmits {
            socket.send(&encoded).await.map_err(transport_error)?;
            let deadline = Instant::now() + self.config.retransmit_interval;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let event = match cancel.as_ref() {
                    Some(cancel) => tokio::select! {
                        _ = cancel.cancelled() => return Err(TransportError::new("request cancelled")),
                        result = tokio::time::timeout(remaining, pending.recv()) => result,
                    },
                    None => tokio::time::timeout(remaining, pending.recv()).await,
                };
                match event {
                    Ok(Some(PendingEvent::Response(response))) if expects_response => {
                        self.clear_pending(message.message_id).await;
                        return Ok(Some(response));
                    }
                    Ok(Some(PendingEvent::Ack)) if !expects_response && !ping => {
                        self.clear_pending(message.message_id).await;
                        return Ok(None);
                    }
                    Ok(Some(PendingEvent::Pong)) if ping => {
                        self.clear_pending(message.message_id).await;
                        return Ok(None);
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => {
                        self.clear_pending(message.message_id).await;
                        return Err(TransportError::new("CoAP transport receiver stopped"));
                    }
                    Err(_) => break,
                }
            }
            if attempt == self.config.max_retransmits {
                self.clear_pending(message.message_id).await;
                return Err(TransportError::new("CoAP transport timed out"));
            }
        }
        self.clear_pending(message.message_id).await;
        Err(TransportError::new("CoAP transport exhausted retries"))
    }
}

impl RpcClientTransport for CoapClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects = crate::expects_response(&message);
            self.send_with_expectation(message, expects).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let message_id = self.next_message_id();
            let token = u64::from(message_id).to_be_bytes().to_vec();
            self.send_reliably(
                CoapMessage::request(message_id, token, message.to_vec()),
                expects_response,
                false,
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
            let token = u64::from(message_id).to_be_bytes().to_vec();
            self.send_reliably(
                CoapMessage::request(message_id, token, message.to_vec()),
                expects_response,
                false,
                Some(cancel),
            )
            .await
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            self.send_reliably(CoapMessage::ping(self.next_message_id()), false, true, None)
                .await
                .map(|_| ())
        })
    }

    fn kind(&self) -> &'static str {
        "coap"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement =
                Self::open(self.address, self.config.clone(), Arc::clone(&self.pending)).await?;
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
    pending: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<PendingEvent>>>>,
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
        let message = match decode_coap_message(&buffer[..size]) {
            Ok(message) => message,
            Err(_) => continue,
        };
        let event = match (message.ty, message.code, message.payload.is_empty()) {
            (CoapMessageType::Acknowledgement, 0, true) if message.token.is_empty() => PendingEvent::Pong,
            (CoapMessageType::Acknowledgement, 0, true) => PendingEvent::Ack,
            (CoapMessageType::Acknowledgement, _, false) => PendingEvent::Response(Bytes::from(message.payload)),
            (CoapMessageType::Confirmable, 0, true) => {
                let pong = match encode_coap_message(&CoapMessage::pong(message.message_id)) {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                };
                let _ = socket.send(&pong).await;
                continue;
            }
            _ => continue,
        };
        if let Some(waiter) = pending.lock().await.get(&message.message_id).cloned() {
            let _ = waiter.send(event);
        }
    }
}

fn validate_config(config: &UdpReliabilityConfig) -> Result<(), TransportError> {
    if config.retransmit_interval.is_zero() {
        return Err(TransportError::new("CoAP retransmit_interval must be greater than zero"));
    }
    if config.max_packet_size > 65_507 {
        return Err(TransportError::new("CoAP max_packet_size must not exceed 65507 bytes"));
    }
    if config.max_packet_size < 64 {
        return Err(TransportError::new("CoAP max_packet_size must be at least 64 bytes"));
    }
    if config.dedup_cache_capacity == 0 {
        return Err(TransportError::new("CoAP dedup_cache_capacity must be at least 1"));
    }
    if config.dedup_cache_ttl.is_zero() {
        return Err(TransportError::new("CoAP dedup_cache_ttl must be greater than zero"));
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
