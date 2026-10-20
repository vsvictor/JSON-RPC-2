#![cfg(feature = "udp")]

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use json_rpc_2::client::RpcClient;
use json_rpc_2::{
    JsonRpcProtocol, MultiTransportServer, ReliableUdpPacketKind, RpcServer, UdpReliabilityConfig,
    decode_udp_packet,
};
use serde_json::json;
use tokio::{net::UdpSocket, sync::oneshot};

#[tokio::test]
async fn udp_retransmission_replays_cached_response_without_reexecuting_handler() {
    let server = Arc::new(RpcServer::new());
    let executions = Arc::new(AtomicUsize::new(0));
    let execution_count = Arc::clone(&executions);
    server
        .register("charge_once", move |_| {
            let execution_count = Arc::clone(&execution_count);
            async move {
                let execution = execution_count.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(json!({"execution": execution}))
            }
        })
        .expect("register non-idempotent test method");

    let running = MultiTransportServer::builder(server.clone() as Arc<dyn JsonRpcProtocol>)
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_udp_port(0)
        .start()
        .await
        .expect("start UDP server");
    let (_, server_address) = running.listeners().next().expect("UDP listener");

    let proxy_socket = Arc::new(
        UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .expect("bind local UDP fault proxy"),
    );
    let proxy_address = proxy_socket.local_addr().expect("proxy local address");
    let dropped_response = Arc::new(AtomicBool::new(false));
    let request_packets = Arc::new(AtomicUsize::new(0));
    let proxy_dropped_response = Arc::clone(&dropped_response);
    let proxy_request_packets = Arc::clone(&request_packets);
    let (proxy_ready_tx, proxy_ready_rx) = oneshot::channel();
    let proxy_task = tokio::spawn(async move {
        let mut client_address = None;
        let mut buffer = vec![0u8; 65_535];
        let _ = proxy_ready_tx.send(());
        loop {
            let (length, peer) = proxy_socket
                .recv_from(&mut buffer)
                .await
                .expect("proxy receive");
            if peer == server_address {
                if let Ok(packet) = decode_udp_packet(&buffer[..length])
                    && packet.kind == ReliableUdpPacketKind::Response
                    && !proxy_dropped_response.swap(true, Ordering::SeqCst)
                {
                    continue;
                }
                if let Some(client_address) = client_address {
                    proxy_socket
                        .send_to(&buffer[..length], client_address)
                        .await
                        .expect("forward server response");
                }
            } else {
                client_address = Some(peer);
                if let Ok(packet) = decode_udp_packet(&buffer[..length])
                    && packet.kind == ReliableUdpPacketKind::Request
                {
                    proxy_request_packets.fetch_add(1, Ordering::SeqCst);
                }
                proxy_socket
                    .send_to(&buffer[..length], server_address)
                    .await
                    .expect("forward client request");
            }
        }
    });
    proxy_ready_rx.await.expect("proxy started");

    let config = UdpReliabilityConfig {
        retransmit_interval: Duration::from_millis(25),
        max_retransmits: 4,
        ..UdpReliabilityConfig::default()
    };
    let transport = json_rpc_2::client::transports::udp::UdpClientTransport::connect_with_config(
        proxy_address,
        config,
    )
    .await
    .expect("connect UDP client through proxy");
    let client = RpcClient::new(transport);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.request_value("charge_once", None),
    )
    .await
    .expect("retried request exceeded bound")
    .expect("request should complete after response retransmission");

    assert_eq!(result, json!({"execution": 1}));
    assert!(
        dropped_response.load(Ordering::SeqCst),
        "proxy did not drop a response"
    );
    assert!(
        request_packets.load(Ordering::SeqCst) >= 2,
        "client did not retransmit the dropped response's request"
    );
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "the live server deduplication cache must suppress the duplicate handler call"
    );

    client.close().await.expect("close client");
    proxy_task.abort();
    let _ = proxy_task.await;
    running.close().await.expect("close server");
}
