#![cfg(any(
    feature = "tcp",
    feature = "http1",
    feature = "udp",
    feature = "rtp",
    feature = "rudp",
    feature = "srt",
    feature = "mqtt",
    feature = "coap",
    feature = "amqp"
))]

use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use json_rpc_2::client::{BatchRequest, RpcClient, RpcClientError};
use json_rpc_2::{
    JsonRpcProtocol, MultiTransportServer, RpcClientTransport, RpcServer, RunningServer,
    TransportKind,
};
use serde_json::{Value, json};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const OUTER_TIMEOUT: Duration = Duration::from_millis(1_600);
const TEST_PASSPHRASE: &str = "contract-test-only";

struct ServerContext {
    running: RunningServer,
    notification_count: Arc<AtomicUsize>,
    slow_call_count: Arc<AtomicUsize>,
    address: SocketAddr,
}

#[cfg(feature = "mqtt")]
fn mqtt_config(kind: TransportKind) -> json_rpc_2::MqttConfig {
    json_rpc_2::MqttConfig {
        broker_url: std::env::var("JSON_RPC_TEST_MQTT_URL")
            .expect("set JSON_RPC_TEST_MQTT_URL to an external MQTT broker"),
        request_topic: format!(
            "json-rpc-2/contract/{}/{}/requests",
            std::process::id(),
            kind.id()
        ),
        ..Default::default()
    }
}

#[cfg(feature = "amqp")]
fn amqp_config(kind: TransportKind) -> json_rpc_2::AmqpConfig {
    json_rpc_2::AmqpConfig {
        broker_url: std::env::var("JSON_RPC_TEST_AMQP_URL")
            .expect("set JSON_RPC_TEST_AMQP_URL to an external AMQP broker"),
        request_queue: format!("json-rpc-2.contract.{}.{}", std::process::id(), kind.id()),
        ..Default::default()
    }
}

async fn start_server(kind: TransportKind) -> ServerContext {
    let server = Arc::new(RpcServer::new());
    let notification_count = Arc::new(AtomicUsize::new(0));
    let slow_call_count = Arc::new(AtomicUsize::new(0));

    server
        .register("echo", |params| async move { Ok(params) })
        .expect("register echo");

    let notification_counter = Arc::clone(&notification_count);
    server
        .register("record_notification", move |_| {
            let notification_counter = Arc::clone(&notification_counter);
            async move {
                notification_counter.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            }
        })
        .expect("register notification");

    let slow_counter = Arc::clone(&slow_call_count);
    server
        .register("slow", move |params| {
            let slow_counter = Arc::clone(&slow_counter);
            async move {
                slow_counter.fetch_add(1, Ordering::SeqCst);
                let delay = params
                    .get("delay_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(350);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                Ok(params.get("value").cloned().unwrap_or(Value::Null))
            }
        })
        .expect("register slow");

    let builder = MultiTransportServer::builder(server.clone() as Arc<dyn JsonRpcProtocol>)
        .set_bind([LOCALHOST])
        .set_port(kind, 0)
        .set_srt_config(json_rpc_2::SrtConfig {
            passphrase: TEST_PASSPHRASE.to_owned(),
        });
    let builder = match kind {
        #[cfg(feature = "mqtt")]
        TransportKind::Mqtt => builder.set_mqtt_config(mqtt_config(kind)),
        #[cfg(feature = "amqp")]
        TransportKind::Amqp => builder.set_amqp_config(amqp_config(kind)),
        #[cfg(all(feature = "mqtt", feature = "websocket"))]
        TransportKind::WebSocketMqtt => builder.set_ws_mqtt_config(mqtt_config(kind)),
        #[cfg(all(feature = "amqp", feature = "websocket"))]
        TransportKind::WebSocketAmqp => builder.set_ws_amqp_config(amqp_config(kind)),
        _ => builder,
    };
    let running = builder.start().await.expect("start transport server");
    let address = if let Some((_, address)) = running.listeners().next() {
        address
    } else {
        assert_eq!(
            running.broker_transports().count(),
            1,
            "started broker consumer"
        );
        SocketAddr::new(LOCALHOST, 0)
    };

    ServerContext {
        running,
        notification_count,
        slow_call_count,
        address,
    }
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize, bound: Duration) {
    tokio::time::timeout(bound, async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server did not observe the expected request");
}

async fn exercise_transport<T, Start, StartFuture, Connect, ConnectFuture>(
    kind: TransportKind,
    startup: Start,
    connect: Connect,
    verify_datagram_cancellation: bool,
) where
    T: RpcClientTransport,
    Start: FnOnce(TransportKind) -> StartFuture,
    StartFuture: Future<Output = ServerContext>,
    Connect: FnOnce(SocketAddr) -> ConnectFuture,
    ConnectFuture: Future<Output = T>,
{
    let context = startup(kind).await;
    let transport = Arc::new(connect(context.address).await);
    let client = RpcClient::new(Arc::clone(&transport));
    client
        .set_request_timeout(Duration::from_millis(300))
        .expect("set request timeout");

    let echoed = tokio::time::timeout(
        OUTER_TIMEOUT,
        client.request_value("echo", Some(json!({"contract": kind.id()}))),
    )
    .await
    .expect("echo exceeded outer bound")
    .expect("echo request failed");
    assert_eq!(echoed, json!({"contract": kind.id()}));

    tokio::time::timeout(OUTER_TIMEOUT, client.notify("record_notification", None))
        .await
        .expect("notification exceeded outer bound")
        .expect("notification send failed");
    wait_for_count(&context.notification_count, 1, Duration::from_millis(300)).await;

    let batch = tokio::time::timeout(
        OUTER_TIMEOUT,
        client.batch(vec![
            BatchRequest::Call {
                method: "echo".to_owned(),
                params: Some(json!({"batch": "first"})),
            },
            BatchRequest::Call {
                method: "method_that_does_not_exist".to_owned(),
                params: None,
            },
            BatchRequest::Notification {
                method: "record_notification".to_owned(),
                params: None,
            },
        ]),
    )
    .await
    .expect("batch exceeded outer bound")
    .expect("batch request failed");
    assert_eq!(
        batch.len(),
        2,
        "batch notifications must not have responses"
    );
    assert_ne!(batch[0].id, batch[1].id, "batch call IDs must be distinct");
    assert_eq!(
        batch[0].result.as_ref().unwrap(),
        &json!({"batch": "first"})
    );
    assert_eq!(
        batch[1].result.as_ref().unwrap_err().code,
        -32601,
        "method errors must remain associated with their call ID"
    );
    wait_for_count(&context.notification_count, 2, Duration::from_millis(300)).await;

    let before_timeout = context.slow_call_count.load(Ordering::SeqCst);
    let timed_call = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .request_value("slow", Some(json!({"delay_ms": 800, "value": "timeout"})))
                .await
        })
    };
    wait_for_count(
        &context.slow_call_count,
        before_timeout + 1,
        Duration::from_millis(700),
    )
    .await;
    let timed = tokio::time::timeout(OUTER_TIMEOUT, timed_call)
        .await
        .expect("client timeout was not bounded by the outer timeout")
        .expect("timeout request task panicked");
    assert!(
        matches!(timed, Err(RpcClientError::Timeout { ref method, .. }) if method == "slow"),
        "expected a client-side request timeout, got {timed:?}"
    );
    assert_eq!(client.metrics().request_timeouts_total, 1);

    if kind.id() != "tcp" {
        tokio::time::timeout(OUTER_TIMEOUT, transport.reconnect())
            .await
            .expect("transport reconnect exceeded outer bound")
            .expect("transport reconnect failed");
    }
    let after_reconnect = tokio::time::timeout(
        OUTER_TIMEOUT,
        client.request_value("echo", Some(json!({"phase": "after-timeout"}))),
    )
    .await
    .expect("post-timeout request exceeded outer bound")
    .expect("post-timeout request failed");
    assert_eq!(after_reconnect, json!({"phase": "after-timeout"}));

    if verify_datagram_cancellation {
        client
            .set_request_timeout(Duration::from_secs(2))
            .expect("increase timeout before caller cancellation");
        let before_cancel = context.slow_call_count.load(Ordering::SeqCst);
        let pending = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value(
                        "slow",
                        Some(json!({"delay_ms": 1_000, "value": "cancelled"})),
                    )
                    .await
            })
        };
        wait_for_count(
            &context.slow_call_count,
            before_cancel + 1,
            Duration::from_millis(700),
        )
        .await;
        pending.abort();
        assert!(
            pending
                .await
                .expect_err("cancelled task should stop")
                .is_cancelled()
        );

        tokio::time::timeout(OUTER_TIMEOUT, async {
            loop {
                if client.metrics().cancelled_requests_total > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("client did not record caller cancellation");
    }

    tokio::time::timeout(OUTER_TIMEOUT, client.close())
        .await
        .expect("client close exceeded outer bound")
        .expect("client close failed");
    assert!(matches!(
        client.request_value("echo", None).await,
        Err(RpcClientError::SessionClosed)
    ));
    tokio::time::timeout(OUTER_TIMEOUT, context.running.close())
        .await
        .expect("server close exceeded outer bound")
        .expect("server close failed");
}

#[cfg(feature = "tcp")]
#[tokio::test]
async fn tcp_transport_contract() {
    use json_rpc_2::client::transports::tcp::TcpClientTransport;

    exercise_transport(
        TransportKind::Tcp,
        start_server,
        |address| async move { TcpClientTransport::connect(address).await.unwrap() },
        false,
    )
    .await;
}

#[cfg(feature = "http1")]
#[tokio::test]
async fn http1_transport_contract() {
    use json_rpc_2::client::transports::http1::Http1ClientTransport;

    exercise_transport(
        TransportKind::Http1,
        start_server,
        |address| async move { Http1ClientTransport::connect(address).await.unwrap() },
        false,
    )
    .await;
}

#[cfg(feature = "udp")]
#[tokio::test]
async fn udp_transport_contract() {
    use json_rpc_2::client::transports::udp::UdpClientTransport;

    exercise_transport(
        TransportKind::Udp,
        start_server,
        |address| async move { UdpClientTransport::connect(address).await.unwrap() },
        true,
    )
    .await;
}

#[cfg(feature = "rtp")]
#[tokio::test]
async fn rtp_transport_contract() {
    use json_rpc_2::client::transports::rtp::RtpClientTransport;

    exercise_transport(
        TransportKind::Rtp,
        start_server,
        |address| async move { RtpClientTransport::connect(address).await.unwrap() },
        true,
    )
    .await;
}

#[cfg(feature = "rudp")]
#[tokio::test]
async fn rudp_transport_contract() {
    use json_rpc_2::client::transports::rudp::RudpClientTransport;

    exercise_transport(
        TransportKind::Rudp,
        start_server,
        |address| async move { RudpClientTransport::connect(address).await.unwrap() },
        true,
    )
    .await;
}

#[cfg(feature = "coap")]
#[tokio::test]
async fn coap_transport_contract() {
    use json_rpc_2::client::transports::coap::CoapClientTransport;

    exercise_transport(
        TransportKind::Coap,
        start_server,
        |address| async move { CoapClientTransport::connect(address).await.unwrap() },
        true,
    )
    .await;
}

#[cfg(any(
    feature = "mqtt",
    feature = "amqp",
    feature = "srt",
    feature = "websocket"
))]
macro_rules! contract_case {
    (#[ignore = $reason:literal] $name:ident, $kind:ident, $connect:expr, $cancel:expr) => {
        #[tokio::test]
        #[ignore = $reason]
        async fn $name() {
            exercise_transport(TransportKind::$kind, start_server, $connect, $cancel).await;
        }
    };
    ($name:ident, $kind:ident, $connect:expr, $cancel:expr) => {
        #[tokio::test]
        async fn $name() {
            exercise_transport(TransportKind::$kind, start_server, $connect, $cancel).await;
        }
    };
}

#[cfg(feature = "mqtt")]
contract_case!(
    #[ignore = "requires an external MQTT broker; run broker integration tests"]
    mqtt_transport_contract,
    Mqtt,
    |_address| async move {
        json_rpc_2::client::transports::mqtt::MqttClientTransport::connect_with_config(mqtt_config(
            TransportKind::Mqtt,
        ))
        .await
        .unwrap()
    },
    false
);

#[cfg(feature = "amqp")]
contract_case!(
    #[ignore = "requires an external AMQP broker; run broker integration tests"]
    amqp_transport_contract,
    Amqp,
    |_address| async move {
        json_rpc_2::client::transports::amqp::AmqpClientTransport::connect_with_config(amqp_config(
            TransportKind::Amqp,
        ))
        .await
        .unwrap()
    },
    false
);

#[cfg(feature = "srt")]
contract_case!(
    srt_transport_contract,
    Srt,
    |address| async move {
        json_rpc_2::client::transports::srt::SrtClientTransport::connect(address, TEST_PASSPHRASE)
            .await
            .unwrap()
    },
    true
);

#[cfg(all(feature = "websocket", feature = "tcp"))]
contract_case!(
    ws_tcp_transport_contract,
    WebSocketTcp,
    |address| async move {
        json_rpc_2::client::transports::websocket::tcp::TcpWebSocketClientTransport::connect(
            &format!("ws://{address}/"),
        )
        .await
        .unwrap()
    },
    false
);

#[cfg(all(feature = "websocket", feature = "http1"))]
contract_case!(
    ws_http1_transport_contract,
    WebSocketHttp1,
    |address| async move {
        json_rpc_2::client::transports::websocket::http1::Http1WebSocketClientTransport::connect(
            &format!("ws://{address}/ws"),
        )
        .await
        .unwrap()
    },
    false
);

#[cfg(all(feature = "websocket", feature = "udp"))]
contract_case!(
    ws_udp_transport_contract,
    WebSocketUdp,
    |address| async move {
        json_rpc_2::client::transports::websocket::udp::UdpWebSocketClientTransport::connect(
            address,
        )
        .await
        .unwrap()
    },
    true
);

#[cfg(all(feature = "websocket", feature = "rtp"))]
contract_case!(
    ws_rtp_transport_contract,
    WebSocketRtp,
    |address| async move {
        json_rpc_2::client::transports::websocket::rtp::RtpWebSocketClientTransport::connect(
            address,
        )
        .await
        .unwrap()
    },
    true
);

#[cfg(all(feature = "websocket", feature = "rudp"))]
contract_case!(
    ws_rudp_transport_contract,
    WebSocketRudp,
    |address| async move {
        json_rpc_2::client::transports::websocket::rudp::RudpWebSocketClientTransport::connect(
            address,
        )
        .await
        .unwrap()
    },
    true
);

#[cfg(all(feature = "websocket", feature = "coap"))]
contract_case!(
    ws_coap_transport_contract,
    WebSocketCoap,
    |address| async move {
        json_rpc_2::client::transports::websocket::coap::CoapWebSocketClientTransport::connect(
            address,
        )
        .await
        .unwrap()
    },
    true
);

#[cfg(all(feature = "websocket", feature = "mqtt"))]
contract_case!(
    #[ignore = "requires an external MQTT broker; run broker integration tests"]
    ws_mqtt_transport_contract,
    WebSocketMqtt,
    |_address| async move {
        json_rpc_2::client::transports::websocket::mqtt::MqttWebSocketClientTransport::connect_with_config(
            mqtt_config(TransportKind::WebSocketMqtt),
        )
        .await
        .unwrap()
    },
    false
);

#[cfg(all(feature = "websocket", feature = "amqp"))]
contract_case!(
    #[ignore = "requires an external AMQP broker; run broker integration tests"]
    ws_amqp_transport_contract,
    WebSocketAmqp,
    |_address| async move {
        json_rpc_2::client::transports::websocket::amqp::AmqpWebSocketClientTransport::connect_with_config(
            amqp_config(TransportKind::WebSocketAmqp),
        )
        .await
        .unwrap()
    },
    false
);

#[cfg(all(feature = "websocket", feature = "srt"))]
contract_case!(
    ws_srt_transport_contract,
    WebSocketSrt,
    |address| async move {
        json_rpc_2::client::transports::websocket::srt::SrtWebSocketClientTransport::connect(
            address,
            TEST_PASSPHRASE,
        )
        .await
        .unwrap()
    },
    true
);
