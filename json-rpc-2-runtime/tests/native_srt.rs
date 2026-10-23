#![cfg(feature = "srt")]

use json_rpc_2_client::RpcClient;
use json_rpc_2_core::SrtConfig;
use json_rpc_2_datagram::client::transports::srt::SrtClientTransport;
use json_rpc_2_runtime::{MultiTransportServer, TransportKind};
use json_rpc_2_server::RpcServer;
use serde_json::json;

#[test]
fn runtime_rejects_invalid_native_srt_config_before_starting() {
    for passphrase in ["".to_owned(), "short".to_owned(), "a".repeat(80)] {
        let rpc = Arc::new(RpcServer::new());
        let server = MultiTransportServer::builder(rpc)
            .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .set_srt_config(SrtConfig { passphrase })
            .set_srt_port(0)
            .build();
        assert_eq!(
            server.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn runtime_binds_a_native_srt_listener_and_shuts_it_down() {
    let rpc = Arc::new(RpcServer::new());
    rpc.register("echo", |params| async move { Ok(params) })
        .unwrap();
    let running = MultiTransportServer::builder(rpc)
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_srt_config(SrtConfig {
            passphrase: "test-passphrase".into(),
        })
        .set_srt_port(0)
        .start()
        .await
        .unwrap();
    let address = running
        .address(TransportKind::Srt, IpAddr::V4(Ipv4Addr::LOCALHOST))
        .unwrap();
    assert_ne!(address.port(), 0);
    let transport = SrtClientTransport::connect(address, "test-passphrase")
        .await
        .unwrap();
    let client = RpcClient::new(transport);
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request_value("echo", Some(json!({"native": "srt"}))),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response, json!({"native": "srt"}));
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), running.close())
        .await
        .expect("SRT runtime shutdown timed out")
        .unwrap();
}

#[cfg(feature = "websocket")]
#[tokio::test]
async fn runtime_binds_native_websocket_srt() {
    let rpc = Arc::new(RpcServer::new());
    rpc.register("echo", |params| async move { Ok(params) })
        .unwrap();
    let running = MultiTransportServer::builder(rpc)
        .set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .set_srt_config(SrtConfig {
            passphrase: "test-passphrase".into(),
        })
        .set_ws_srt_port(0)
        .start()
        .await
        .unwrap();
    let address = running
        .address(TransportKind::WebSocketSrt, IpAddr::V4(Ipv4Addr::LOCALHOST))
        .unwrap();
    let transport =
        json_rpc_2_websockets::client::transports::srt::SrtWebSocketClientTransport::connect(
            address,
            "test-passphrase",
        )
        .await
        .unwrap();
    let client = RpcClient::new(transport);
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request_value("echo", Some(json!({"native": "websocket-srt"}))),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response, json!({"native": "websocket-srt"}));
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), running.close())
        .await
        .expect("WebSocket SRT runtime shutdown timed out")
        .unwrap();
}
