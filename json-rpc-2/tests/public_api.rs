//! Compile-time checks for supported facade paths and compatibility aliases.

use json_rpc_2::{ClientLimits, ServerLimits};

#[test]
fn limits_keep_distinct_types_and_legacy_aliases() {
    let client: json_rpc_2::client::TransportLimits = ClientLimits::default();
    let server: json_rpc_2::server::TransportLimits = ServerLimits::default();
    let _: ClientLimits = client;
    let _: ServerLimits = server;
}

#[test]
fn server_runtime_remains_available_through_facade() {
    fn accepts_protocol(_: &dyn json_rpc_2::JsonRpcProtocol) {}
    accepts_protocol(&json_rpc_2::RpcServer::new());
    let _ = std::any::type_name::<json_rpc_2::MultiTransportServer>();
    let _ = std::any::type_name::<json_rpc_2::RunningServer>();
}

#[test]
fn broker_configuration_is_available_through_facade() {
    #[cfg(feature = "mqtt")]
    let _ = std::any::type_name::<json_rpc_2::MqttConfig>();
    #[cfg(feature = "amqp")]
    let _ = std::any::type_name::<json_rpc_2::AmqpConfig>();
}

#[cfg(any(
    feature = "tcp",
    feature = "udp",
    feature = "rtp",
    feature = "rudp",
    feature = "srt",
    feature = "http1",
    feature = "quic",
    feature = "http3",
    feature = "mqtt",
    feature = "coap",
    feature = "amqp",
    feature = "udplite",
    feature = "dccp",
    feature = "sctp"
))]
mod contract_assertions {
    pub fn client_contract<T: json_rpc_2::client::RpcClientTransport>() {}
    pub fn server_contract<T: json_rpc_2::Transport>() {}
}

#[test]
fn family_types_keep_transport_contracts() {
    #[cfg(any(
        feature = "tcp",
        feature = "udp",
        feature = "rtp",
        feature = "rudp",
        feature = "srt",
        feature = "http1",
        feature = "quic",
        feature = "http3",
        feature = "mqtt",
        feature = "coap",
        feature = "amqp",
        feature = "udplite",
        feature = "dccp",
        feature = "sctp"
    ))]
    use contract_assertions::{client_contract, server_contract};
    #[cfg(feature = "tcp")]
    {
        client_contract::<json_rpc_2::client::transports::tcp::TcpClientTransport>();
        server_contract::<json_rpc_2::server::transports::tcp::TcpTransport>();
    }
    #[cfg(feature = "udp")]
    {
        client_contract::<json_rpc_2::client::transports::udp::UdpClientTransport>();
        server_contract::<json_rpc_2::server::transports::udp::UdpTransport>();
    }
    #[cfg(feature = "rtp")]
    {
        client_contract::<json_rpc_2::client::transports::rtp::RtpClientTransport>();
        server_contract::<json_rpc_2::server::transports::rtp::RtpTransport>();
    }
    #[cfg(feature = "rudp")]
    {
        client_contract::<json_rpc_2::client::transports::rudp::RudpClientTransport>();
        server_contract::<json_rpc_2::server::transports::rudp::RudpTransport>();
    }
    #[cfg(feature = "srt")]
    {
        client_contract::<json_rpc_2::client::transports::srt::SrtClientTransport>();
        server_contract::<json_rpc_2::server::transports::srt::SrtTransport>();
    }
    #[cfg(feature = "http1")]
    {
        client_contract::<json_rpc_2::client::transports::http1::Http1ClientTransport>();
        server_contract::<json_rpc_2::server::transports::http1::Http1Transport>();
    }
    #[cfg(feature = "quic")]
    {
        client_contract::<json_rpc_2::client::transports::quic::QuicClientTransport>();
        server_contract::<json_rpc_2::server::transports::quic::QuicTransport>();
    }
    #[cfg(feature = "http3")]
    {
        client_contract::<json_rpc_2::client::transports::http3::Http3ClientTransport>();
        server_contract::<json_rpc_2::server::transports::http3::Http3Transport>();
    }
    #[cfg(feature = "mqtt")]
    {
        client_contract::<json_rpc_2::client::transports::mqtt::MqttClientTransport>();
    }
    #[cfg(feature = "coap")]
    {
        client_contract::<json_rpc_2::client::transports::coap::CoapClientTransport>();
    }
    #[cfg(feature = "amqp")]
    {
        client_contract::<json_rpc_2::client::transports::amqp::AmqpClientTransport>();
    }
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    {
        client_contract::<json_rpc_2::client::transports::udplite::UdpLiteClientTransport>();
        server_contract::<json_rpc_2::server::transports::udplite::UdpLiteTransport>();
    }
    #[cfg(feature = "dccp")]
    {
        client_contract::<json_rpc_2::client::transports::dccp::DccpClientTransport>();
        server_contract::<json_rpc_2::server::transports::dccp::DccpTransport>();
    }
    #[cfg(feature = "sctp")]
    {
        client_contract::<json_rpc_2::client::transports::sctp::SctpClientTransport>();
        server_contract::<json_rpc_2::server::transports::sctp::SctpTransport>();
    }
}
