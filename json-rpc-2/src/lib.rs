//! Compatibility facade re-exporting the split core/client/server crates.

pub use json_rpc_2_client::ClientLimits;
pub use json_rpc_2_core::*;
#[cfg(feature = "amqp")]
pub use json_rpc_2_iot::AmqpConfig;
#[cfg(feature = "mqtt")]
pub use json_rpc_2_iot::MqttConfig;
pub use json_rpc_2_runtime::{
    HasBind, HasTransport, HealthHandle, HealthSnapshot, MissingBind, MultiTransportServer,
    MultiTransportServerBuilder, NoTransport, RunningServer, TransportKind,
};
pub use json_rpc_2_server::{
    JsonRpcProtocol, RegistrationError, RpcServer, ServerDispatcher, ServerDispatcherBuilder,
    ServerLimits, ServerMetricsSnapshot, TransportLimits,
};
pub use json_rpc_2_transport::*;

pub mod client {
    pub use json_rpc_2_client::*;
    #[cfg(any(feature = "quic", feature = "http3"))]
    pub use json_rpc_2_quic::client::ClientTlsConfig;

    pub mod transports {
        #[cfg(feature = "dccp")]
        pub use json_rpc_2_datagram::client::transports::dccp;
        #[cfg(feature = "rtp")]
        pub use json_rpc_2_datagram::client::transports::rtp;
        #[cfg(feature = "rudp")]
        pub use json_rpc_2_datagram::client::transports::rudp;
        #[cfg(feature = "sctp")]
        pub use json_rpc_2_datagram::client::transports::sctp;
        #[cfg(feature = "srt")]
        pub use json_rpc_2_datagram::client::transports::srt;
        #[cfg(feature = "udp")]
        pub use json_rpc_2_datagram::client::transports::udp;
        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        pub use json_rpc_2_datagram::client::transports::udplite;
        #[cfg(feature = "amqp")]
        pub use json_rpc_2_iot::client::transports::amqp;
        #[cfg(feature = "coap")]
        pub use json_rpc_2_iot::client::transports::coap;
        #[cfg(feature = "mqtt")]
        pub use json_rpc_2_iot::client::transports::mqtt;
        #[cfg(feature = "http3")]
        pub use json_rpc_2_quic::client::transports::http3;
        #[cfg(feature = "quic")]
        pub use json_rpc_2_quic::client::transports::quic;
        #[cfg(feature = "http1")]
        pub use json_rpc_2_tcp::client::transports::http1;
        #[cfg(feature = "tcp")]
        pub use json_rpc_2_tcp::client::transports::tcp;
        #[cfg(feature = "tls")]
        pub use json_rpc_2_tcp::client::transports::tls;

        #[cfg(feature = "websocket")]
        pub mod websocket {
            pub use json_rpc_2_websockets::client::transports::*;
        }
    }
}

pub mod transports {
    pub use json_rpc_2_transport::*;
    pub mod multiplex {
        pub use json_rpc_2_transport::multiplex::{StreamMultiplexor, StreamMultiplexorHandle};
    }

    #[cfg(feature = "dccp")]
    pub use json_rpc_2_datagram::server::transports::dccp;
    #[cfg(feature = "rtp")]
    pub use json_rpc_2_datagram::server::transports::rtp;
    #[cfg(feature = "rudp")]
    pub use json_rpc_2_datagram::server::transports::rudp;
    #[cfg(feature = "sctp")]
    pub use json_rpc_2_datagram::server::transports::sctp;
    #[cfg(feature = "srt")]
    pub use json_rpc_2_datagram::server::transports::srt;
    #[cfg(feature = "udp")]
    pub use json_rpc_2_datagram::server::transports::udp;
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    pub use json_rpc_2_datagram::server::transports::udplite;
    #[cfg(feature = "amqp")]
    pub use json_rpc_2_iot::server::transports::amqp;
    #[cfg(feature = "coap")]
    pub use json_rpc_2_iot::server::transports::coap;
    #[cfg(feature = "mqtt")]
    pub use json_rpc_2_iot::server::transports::mqtt;
    #[cfg(feature = "http3")]
    pub use json_rpc_2_quic::server::transports::http3;
    #[cfg(feature = "quic")]
    pub use json_rpc_2_quic::server::transports::quic;
    #[cfg(feature = "http1")]
    pub use json_rpc_2_tcp::server::transports::http1;
    #[cfg(feature = "tcp")]
    pub use json_rpc_2_tcp::server::transports::tcp;
    #[cfg(feature = "tls")]
    pub use json_rpc_2_tcp::server::transports::tls;

    #[cfg(feature = "websocket")]
    pub mod websocket {
        pub use json_rpc_2_websockets::transports::*;
    }
}

pub mod server {
    pub use json_rpc_2_runtime::{
        HasBind, HasTransport, HealthHandle, HealthSnapshot, MissingBind, MultiTransportServer,
        MultiTransportServerBuilder, NoTransport, RunningServer, TransportKind,
    };
    pub use json_rpc_2_server::{
        JsonRpcProtocol, RegistrationError, RpcServer, ServerDispatcher, ServerDispatcherBuilder,
        ServerLimits, ServerMetricsSnapshot, TransportLimits,
    };

    pub mod multi {
        pub use json_rpc_2_runtime::multi::*;
    }

    pub mod transports {
        pub use crate::transports::*;
    }
}
