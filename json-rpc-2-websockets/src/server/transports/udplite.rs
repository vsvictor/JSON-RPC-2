//! WebSocket-style framing over the reliable UDP-Lite transport.

use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::Arc,
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
    ReliableUdpPacket, ReliableUdpPacketKind, decode_udp_packet, decode_websocket_message,
    encode_udp_packet, encode_websocket_message, websocket_message_bytes,
};

/// Dedicated WebSocket-style transport over reliable UDP-Lite datagrams.
pub struct UdpLiteWebSocketTransport {
    socket: UdpSocket,
    runtime: Arc<TransportRuntime>,
}

impl UdpLiteWebSocketTransport {
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

impl WebSocketTransport for UdpLiteWebSocketTransport {
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
    InFlight { seen_at: Instant },
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
        let packet = match decode_udp_packet(&buffer[..size]) {
            Ok(packet) => packet,
            Err(error) => {
                log_transport_error("WebSocket/UDP-Lite", peer, "packet", &error);
                continue;
            }
        };
        handle_packet(
            Arc::clone(&socket),
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
    protocol: Arc<dyn WebSocketProtocol>,
    sessions: Arc<Mutex<HashMap<SocketAddr, PeerSession>>>,
    runtime: Arc<TransportRuntime>,
    tasks: &mut JoinSet<()>,
    peer: SocketAddr,
    packet: ReliableUdpPacket,
) {
    match packet.kind {
        ReliableUdpPacketKind::Request => {
            let action =
                cache_request_state(&sessions, &runtime, peer, packet.packet_id, None, false).await;
            match action {
                RequestAction::Process => {
                    if let Err(error) = send_packet(
                        &socket,
                        peer,
                        ReliableUdpPacket::ack(packet.packet_id),
                        &runtime,
                    )
                    .await
                    {
                        log_transport_error("WebSocket/UDP-Lite", peer, "ack", &error);
                        return;
                    }
                    let inbound = match decode_websocket_message(&packet.payload) {
                        Ok(inbound) => inbound,
                        Err(error) => {
                            log_transport_error("WebSocket/UDP-Lite", peer, "frame", &error);
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
                                        "WebSocket/UDP-Lite",
                                    )
                                    .await
                                }
                                None => None,
                            },
                            None => {
                                log_control("WebSocket/UDP-Lite", peer, "close", "ack");
                                None
                            }
                        };
                        log_json_rpc(
                            "WebSocket/UDP-Lite",
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
                        ReliableUdpPacket::ack(packet.packet_id),
                        &runtime,
                    )
                    .await
                    {
                        log_transport_error("WebSocket/UDP-Lite", peer, "ack", &error);
                    }
                }
                RequestAction::Response(response) => {
                    if let Err(error) = socket.send_to(&response, peer).await {
                        log_transport_error("WebSocket/UDP-Lite", peer, "response", &error);
                    }
                }
                RequestAction::Drop => {}
            }
        }
        ReliableUdpPacketKind::Ping => {
            log_control("WebSocket/UDP-Lite", peer, "ping", "pong");
            if let Err(error) = send_packet(
                &socket,
                peer,
                ReliableUdpPacket::pong(packet.packet_id),
                &runtime,
            )
            .await
            {
                log_transport_error("WebSocket/UDP-Lite", peer, "pong", &error);
            }
        }
        ReliableUdpPacketKind::Ack
        | ReliableUdpPacketKind::Response
        | ReliableUdpPacketKind::Pong => {}
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

fn encode_response(
    packet_id: u64,
    response: WebSocketMessage,
    runtime: &Arc<TransportRuntime>,
) -> io::Result<Vec<u8>> {
    let frame_bytes = encode_websocket_message(&response)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if frame_bytes.len() > MAX_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WebSocket/UDP-Lite response exceeds message limit",
        ));
    }
    let packet = ReliableUdpPacket::response(packet_id, frame_bytes);
    let bytes = encode_udp_packet(&packet)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if bytes.len() > runtime.udp_reliability.max_packet_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "WebSocket/UDP-Lite response exceeds configured packet limit ({} > {})",
                bytes.len(),
                runtime.udp_reliability.max_packet_size
            ),
        ));
    }
    Ok(bytes)
}

async fn send_response(
    socket: &UdpSocket,
    peer: SocketAddr,
    packet_id: u64,
    response: WebSocketMessage,
    runtime: &Arc<TransportRuntime>,
    transport_name: &'static str,
) -> Option<Vec<u8>> {
    match encode_response(packet_id, response, runtime) {
        Ok(bytes) => {
            if let Err(error) = socket.send_to(&bytes, peer).await {
                log_transport_error(transport_name, peer, "response", &error);
            }
            Some(bytes)
        }
        Err(error) => {
            log_transport_error(transport_name, peer, "response", &error);
            None
        }
    }
}

async fn send_packet(
    socket: &UdpSocket,
    peer: SocketAddr,
    packet: ReliableUdpPacket,
    runtime: &Arc<TransportRuntime>,
) -> io::Result<()> {
    let bytes = encode_udp_packet(&packet)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if bytes.len() > runtime.udp_reliability.max_packet_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "WebSocket/UDP-Lite packet exceeds configured packet limit ({} > {})",
                bytes.len(),
                runtime.udp_reliability.max_packet_size
            ),
        ));
    }
    socket.send_to(&bytes, peer).await.map(|_| ())
}
