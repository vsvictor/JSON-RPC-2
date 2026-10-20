//! WebSocket-style framing over the RUDP transport.

use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Instant,
};

use tokio::{net::UdpSocket, sync::Mutex, task::JoinSet};

use super::{
    CancellationToken, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
    WebSocketTransportFuture,
};
use json_rpc_2_core::{
    MAX_MESSAGE_BYTES, TransportRuntime, log_control, log_json_rpc, log_transport_error,
};
use json_rpc_2_core::{
    RudpPacket, RudpPacketKind, decode_rudp_packet, decode_websocket_message, encode_rudp_packet,
    encode_websocket_message, websocket_message_bytes,
};

struct EndpointState {
    next_sequence: AtomicU32,
}

impl EndpointState {
    fn new() -> Self {
        Self {
            next_sequence: AtomicU32::new(1),
        }
    }

    fn next_sequence(&self) -> u32 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn response(&self, packet_id: u64, payload: impl Into<Vec<u8>>) -> RudpPacket {
        RudpPacket::response(self.next_sequence(), packet_id, payload)
    }

    fn control(&self, kind: RudpPacketKind, packet_id: u64) -> RudpPacket {
        match kind {
            RudpPacketKind::Ack => RudpPacket::ack(self.next_sequence(), packet_id),
            RudpPacketKind::Ping => RudpPacket::ping(self.next_sequence(), packet_id),
            RudpPacketKind::Pong => RudpPacket::pong(self.next_sequence(), packet_id),
            RudpPacketKind::Request | RudpPacketKind::Response => unreachable!(),
        }
    }
}

/// Dedicated WebSocket-style transport over reliable RUDP datagrams.
pub struct RudpWebSocketTransport {
    socket: UdpSocket,
    runtime: Arc<TransportRuntime>,
}

impl RudpWebSocketTransport {
    pub fn new(socket: UdpSocket) -> Self {
        Self {
            socket,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl WebSocketTransport for RudpWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn WebSocketProtocol>,
        shutdown: CancellationToken,
    ) -> WebSocketTransportFuture {
        Box::pin(serve(self.socket, protocol, shutdown, self.runtime))
    }
}

#[derive(Default)]
struct PeerSession {
    entries: HashMap<u64, CachedRequest>,
    order: VecDeque<u64>,
    last_seen: Option<Instant>,
}

#[derive(Clone)]
enum CachedRequest {
    InFlight {
        seen_at: Instant,
    },
    Complete {
        seen_at: Instant,
        response: Option<Vec<u8>>,
    },
}

enum RequestAction {
    Process,
    Ack,
    Response(Vec<u8>),
    Drop,
}

pub async fn serve(
    socket: UdpSocket,
    protocol: Arc<dyn WebSocketProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let socket = Arc::new(socket);
    let endpoint = Arc::new(EndpointState::new());
    let sessions = Arc::new(Mutex::new(HashMap::<SocketAddr, PeerSession>::new()));
    let mut tasks = JoinSet::new();
    let mut buffer = vec![0u8; runtime.udp_reliability.max_packet_size];

    loop {
        let received = tokio::select! {
            received = socket.recv_from(&mut buffer) => Some(received),
            _ = shutdown.cancelled() => None,
        };
        let Some(received) = received else { break };
        let (size, peer) = match received {
            Ok(received) => received,
            Err(error) => return Err(error),
        };
        let packet = match decode_rudp_packet(&buffer[..size]) {
            Ok(packet) => packet,
            Err(error) => {
                log_transport_error("WebSocket/RUDP", peer, "packet", &error);
                continue;
            }
        };
        handle_packet(
            Arc::clone(&socket),
            Arc::clone(&endpoint),
            Arc::clone(&protocol),
            Arc::clone(&sessions),
            Arc::clone(&runtime),
            &mut tasks,
            peer,
            packet,
        )
        .await;
    }

    while tasks.join_next().await.is_some() {}
    Ok(())
}

async fn handle_packet(
    socket: Arc<UdpSocket>,
    endpoint: Arc<EndpointState>,
    protocol: Arc<dyn WebSocketProtocol>,
    sessions: Arc<Mutex<HashMap<SocketAddr, PeerSession>>>,
    runtime: Arc<TransportRuntime>,
    tasks: &mut JoinSet<()>,
    peer: SocketAddr,
    packet: RudpPacket,
) {
    match packet.kind {
        RudpPacketKind::Request => {
            let action =
                cache_request_state(&sessions, &runtime, peer, packet.packet_id, None, false).await;
            match action {
                RequestAction::Process => {
                    if let Err(error) = send_packet(
                        &socket,
                        peer,
                        endpoint.control(RudpPacketKind::Ack, packet.packet_id),
                        &runtime,
                    )
                    .await
                    {
                        log_transport_error("WebSocket/RUDP", peer, "ack", &error);
                        return;
                    }
                    let inbound = match decode_websocket_message(&packet.payload) {
                        Ok(inbound) => inbound,
                        Err(error) => {
                            log_transport_error("WebSocket/RUDP", peer, "frame", &error);
                            let _ = cache_request_state(
                                &sessions,
                                &runtime,
                                peer,
                                packet.packet_id,
                                None,
                                true,
                            )
                            .await;
                            return;
                        }
                    };
                    runtime
                        .metrics
                        .active_streams
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tasks.spawn(async move {
                        let request_bytes = inbound
                            .as_ref()
                            .map(websocket_message_bytes)
                            .unwrap_or_default()
                            .to_vec();
                        let started = Instant::now();
                        let mut response_bytes_for_log = None;
                        let encoded_response = match inbound {
                            Some(message) => match protocol.handle_message(message).await {
                                Some(response) => {
                                    response_bytes_for_log =
                                        Some(websocket_message_bytes(&response).to_vec());
                                    send_response(
                                        &socket,
                                        peer,
                                        packet.packet_id,
                                        response,
                                        &runtime,
                                        &endpoint,
                                        "WebSocket/RUDP",
                                    )
                                    .await
                                }
                                None => None,
                            },
                            None => {
                                log_control("WebSocket/RUDP", peer, "close", "ack");
                                None
                            }
                        };
                        log_json_rpc(
                            "WebSocket/RUDP",
                            peer,
                            &request_bytes,
                            response_bytes_for_log.as_deref(),
                            started.elapsed(),
                        );
                        let _ = cache_request_state(
                            &sessions,
                            &runtime,
                            peer,
                            packet.packet_id,
                            encoded_response,
                            true,
                        )
                        .await;
                        runtime
                            .metrics
                            .active_streams
                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    });
                }
                RequestAction::Ack => {
                    if let Err(error) = send_packet(
                        &socket,
                        peer,
                        endpoint.control(RudpPacketKind::Ack, packet.packet_id),
                        &runtime,
                    )
                    .await
                    {
                        log_transport_error("WebSocket/RUDP", peer, "ack", &error);
                    }
                }
                RequestAction::Response(response) => {
                    if let Err(error) = socket.send_to(&response, peer).await {
                        log_transport_error("WebSocket/RUDP", peer, "response", &error);
                    }
                }
                RequestAction::Drop => {}
            }
        }
        RudpPacketKind::Ping => {
            log_control("WebSocket/RUDP", peer, "ping", "pong");
            if let Err(error) = send_packet(
                &socket,
                peer,
                endpoint.control(RudpPacketKind::Pong, packet.packet_id),
                &runtime,
            )
            .await
            {
                log_transport_error("WebSocket/RUDP", peer, "pong", &error);
            }
        }
        RudpPacketKind::Ack | RudpPacketKind::Response | RudpPacketKind::Pong => {}
    }
}

async fn cache_request_state(
    sessions: &Arc<Mutex<HashMap<SocketAddr, PeerSession>>>,
    runtime: &Arc<TransportRuntime>,
    peer: SocketAddr,
    packet_id: u64,
    response: Option<Vec<u8>>,
    complete: bool,
) -> RequestAction {
    let mut sessions = sessions.lock().await;
    prune_sessions(&mut sessions, runtime);

    let is_new_peer = !sessions.contains_key(&peer);
    if is_new_peer && sessions.len() >= runtime.limits.max_connections {
        return RequestAction::Drop;
    }

    let session = sessions.entry(peer).or_default();
    if is_new_peer {
        runtime
            .metrics
            .active_sessions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    session.last_seen = Some(Instant::now());
    prune_peer(session, runtime);

    if complete {
        upsert_peer_entry(
            session,
            packet_id,
            CachedRequest::Complete {
                seen_at: Instant::now(),
                response,
            },
        );
        return RequestAction::Drop;
    }

    match session.entries.get(&packet_id).cloned() {
        Some(CachedRequest::InFlight { .. }) => RequestAction::Ack,
        Some(CachedRequest::Complete { response, .. }) => match response {
            Some(response) => RequestAction::Response(response),
            None => RequestAction::Ack,
        },
        None => {
            upsert_peer_entry(
                session,
                packet_id,
                CachedRequest::InFlight {
                    seen_at: Instant::now(),
                },
            );
            RequestAction::Process
        }
    }
}

fn upsert_peer_entry(session: &mut PeerSession, packet_id: u64, entry: CachedRequest) {
    if !session.entries.contains_key(&packet_id) {
        session.order.push_back(packet_id);
    }
    session.entries.insert(packet_id, entry);
}

fn prune_sessions(
    sessions: &mut HashMap<SocketAddr, PeerSession>,
    runtime: &Arc<TransportRuntime>,
) {
    let now = Instant::now();
    let ttl = runtime.udp_reliability.dedup_cache_ttl;
    let mut removed = 0usize;
    sessions.retain(|_, session| {
        prune_peer(session, runtime);
        let keep = session
            .last_seen
            .is_some_and(|last_seen| now.duration_since(last_seen) <= ttl)
            && !session.entries.is_empty();
        if !keep {
            removed += 1;
        }
        keep
    });
    if removed > 0 {
        runtime
            .metrics
            .active_sessions
            .fetch_sub(removed, std::sync::atomic::Ordering::Relaxed);
    }
}

fn prune_peer(session: &mut PeerSession, runtime: &Arc<TransportRuntime>) {
    let now = Instant::now();
    let ttl = runtime.udp_reliability.dedup_cache_ttl;
    while let Some(packet_id) = session.order.front().copied() {
        let expired = match session.entries.get(&packet_id) {
            Some(CachedRequest::InFlight { seen_at })
            | Some(CachedRequest::Complete { seen_at, .. }) => now.duration_since(*seen_at) > ttl,
            None => true,
        };
        if !expired && session.entries.len() <= runtime.udp_reliability.dedup_cache_capacity {
            break;
        }
        session.order.pop_front();
        session.entries.remove(&packet_id);
    }
}

async fn send_response(
    socket: &Arc<UdpSocket>,
    peer: SocketAddr,
    packet_id: u64,
    response: WebSocketMessage,
    runtime: &Arc<TransportRuntime>,
    endpoint: &Arc<EndpointState>,
    transport_name: &'static str,
) -> Option<Vec<u8>> {
    let response_bytes = encode_websocket_message(&response).ok()?;
    if response_bytes.len() > MAX_MESSAGE_BYTES {
        log_transport_error(
            transport_name,
            peer,
            "response",
            &format!(
                "WebSocket payload exceeds transport limit ({} > {})",
                response_bytes.len(),
                MAX_MESSAGE_BYTES
            ),
        );
        return None;
    }
    let packet = endpoint.response(packet_id, response_bytes);
    match encode_rudp_packet(&packet) {
        Ok(encoded) => {
            if encoded.len() > runtime.udp_reliability.max_packet_size {
                log_transport_error(
                    transport_name,
                    peer,
                    "response",
                    &format!(
                        "encoded RUDP response exceeds configured packet limit ({} > {})",
                        encoded.len(),
                        runtime.udp_reliability.max_packet_size
                    ),
                );
                None
            } else {
                if let Err(error) = socket.send_to(&encoded, peer).await {
                    log_transport_error(transport_name, peer, "response", &error);
                }
                Some(encoded)
            }
        }
        Err(error) => {
            log_transport_error(transport_name, peer, "response", &error);
            None
        }
    }
}

async fn send_packet(
    socket: &Arc<UdpSocket>,
    peer: SocketAddr,
    packet: RudpPacket,
    runtime: &Arc<TransportRuntime>,
) -> io::Result<()> {
    let encoded = encode_rudp_packet(&packet).map_err(io::Error::other)?;
    if encoded.len() > runtime.udp_reliability.max_packet_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "encoded RUDP packet exceeds configured packet limit ({} > {})",
                encoded.len(),
                runtime.udp_reliability.max_packet_size
            ),
        ));
    }
    socket.send_to(&encoded, peer).await.map(|_| ())
}
