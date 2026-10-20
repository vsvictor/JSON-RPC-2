//! Compatibility facade re-exporting the split core/client/server crates.

pub use json_rpc_2_core::*;
pub use json_rpc_2_server::{
    JsonRpcProtocol, MultiTransportServer, RegistrationError, RpcServer, RunningServer,
    ServerDispatcher, ServerDispatcherBuilder, ServerMetricsSnapshot, ServerShutdownPolicy,
    TransportKind, TransportLimits,
};

pub mod client {
    pub use json_rpc_2_client::*;

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
        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        pub use json_rpc_2_datagram::client::transports::udplite;
        #[cfg(feature = "udp")]
        pub use json_rpc_2_datagram::client::transports::udp;
        #[cfg(feature = "mqtt")]
        pub use json_rpc_2_iot::client::transports::mqtt;
        #[cfg(feature = "coap")]
        pub use json_rpc_2_iot::client::transports::coap;
        #[cfg(feature = "amqp")]
        pub use json_rpc_2_iot::client::transports::amqp;
        #[cfg(feature = "http3")]
        pub use json_rpc_2_quic::client::transports::http3;
        #[cfg(feature = "quic")]
        pub use json_rpc_2_quic::client::transports::quic;
        #[cfg(feature = "http1")]
        pub use json_rpc_2_tcp::client::transports::http1;
        #[cfg(feature = "tcp")]
        pub use json_rpc_2_tcp::client::transports::tcp;

        #[cfg(feature = "websocket")]
        pub mod websocket {
            pub use json_rpc_2_websockets::client::transports::*;
        }
    }
}

pub mod transports {
    #[cfg(any(
        feature = "tcp",
        feature = "http1",
        all(feature = "websocket", feature = "tcp"),
        all(feature = "websocket", feature = "http1")
    ))]
    pub use json_rpc_2_server::apply_tcp_listener_tuning;
    #[cfg(any(feature = "tcp", all(feature = "websocket", feature = "tcp")))]
    pub use json_rpc_2_server::apply_tcp_stream_tuning;
    pub use json_rpc_2_server::{
        CONTROL_PATH, CONTROL_PING, CONTROL_PONG, CancellationToken, H3_NO_ERROR, QUIC_NO_ERROR,
        RunningTransport, RuntimeMetrics, RuntimeMetricsSnapshot, StreamMultiplexor,
        StreamMultiplexorHandle, Transport, TransportFuture, TransportRuntime, log_control,
        log_json_rpc, log_transport_error,
    };
    pub mod multiplex {
        pub use json_rpc_2_server::{StreamMultiplexor, StreamMultiplexorHandle};
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
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    pub use json_rpc_2_datagram::server::transports::udplite;
    #[cfg(feature = "udp")]
    pub use json_rpc_2_datagram::server::transports::udp;
    #[cfg(feature = "mqtt")]
    pub use json_rpc_2_iot::server::transports::mqtt;
    #[cfg(feature = "coap")]
    pub use json_rpc_2_iot::server::transports::coap;
    #[cfg(feature = "amqp")]
    pub use json_rpc_2_iot::server::transports::amqp;
    #[cfg(feature = "http3")]
    pub use json_rpc_2_quic::server::transports::http3;
    #[cfg(feature = "quic")]
    pub use json_rpc_2_quic::server::transports::quic;
    #[cfg(feature = "http1")]
    pub use json_rpc_2_tcp::server::transports::http1;
    #[cfg(feature = "tcp")]
    pub use json_rpc_2_tcp::server::transports::tcp;

    #[cfg(feature = "websocket")]
    pub mod websocket {
        pub use json_rpc_2_websockets::transports::*;
    }
}

pub mod server {
    pub use json_rpc_2_server::{
        HasBind, HasTransport, HealthHandle, HealthSnapshot, JsonRpcProtocol, MissingBind,
        MultiTransportServer, MultiTransportServerBuilder, NoTransport, RegistrationError,
        RpcServer, RunningServer, ServerDispatcher, ServerDispatcherBuilder, ServerMetricsSnapshot,
        ServerShutdownPolicy, TransportKind, TransportLimits,
    };

    pub mod multi {
        pub use json_rpc_2_server::multi::*;
    }

    pub mod transports {
        pub use crate::transports::*;
    }
}
