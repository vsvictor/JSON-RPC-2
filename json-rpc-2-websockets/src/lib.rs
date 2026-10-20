pub use json_rpc_2_client::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, expects_response,
};
pub use json_rpc_2_core::*;
pub use json_rpc_2_server::{
    JsonRpcProtocol, ProtocolFuture, RpcServer, ServerMetricsSnapshot, ServerShutdownPolicy,
    TransportLimits,
};

pub mod transports {
    #[cfg(feature = "websocket")]
    pub use json_rpc_2_client::transports::websocket::{
        DatagramWebSocketClientTransport, WebSocketCarrier, WebSocketClientTransport,
    };
    #[cfg(any(feature = "dccp", feature = "sctp"))]
    pub use json_rpc_2_server::protocol_adapter;
    #[cfg(feature = "websocket")]
    pub use json_rpc_2_server::{
        JsonRpcWebSocket, WebSocketFuture, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
        WebSocketTransportFuture,
    };

    #[cfg(all(feature = "websocket", feature = "dccp"))]
    pub mod dccp {
        pub use json_rpc_2_client::transports::websocket::dccp::*;
        pub use json_rpc_2_server::transports::websocket::dccp::*;
    }

    #[cfg(all(feature = "websocket", feature = "sctp"))]
    pub mod sctp {
        pub use json_rpc_2_client::transports::websocket::sctp::*;
        pub use json_rpc_2_server::transports::websocket::sctp::*;
    }

    #[cfg(all(feature = "websocket", feature = "udp"))]
    pub mod udp {
        pub use json_rpc_2_client::transports::websocket::udp::*;
        pub use json_rpc_2_server::transports::websocket::udp::*;
    }

    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    pub mod mqtt {
        pub use json_rpc_2_client::transports::websocket::mqtt::*;
        pub use json_rpc_2_server::transports::websocket::mqtt::*;
    }
    #[cfg(all(feature = "websocket", feature = "coap"))]
    pub mod coap {
        pub use json_rpc_2_client::transports::websocket::coap::*;
        pub use json_rpc_2_server::transports::websocket::coap::*;
    }
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    pub mod amqp {
        pub use json_rpc_2_client::transports::websocket::amqp::*;
        pub use json_rpc_2_server::transports::websocket::amqp::*;
    }

    #[cfg(all(feature = "websocket", feature = "rtp"))]
    pub mod rtp {
        pub use crate::rtp_client::*;
        pub use json_rpc_2_server::transports::websocket::rtp::*;
    }
    #[cfg(all(feature = "websocket", feature = "rudp"))]
    pub mod rudp {
        pub use crate::rudp_client::*;
        pub use json_rpc_2_server::transports::websocket::rudp::*;
    }
    #[cfg(all(feature = "websocket", feature = "srt"))]
    pub mod srt {
        pub use crate::srt_client::*;
        pub use json_rpc_2_server::transports::websocket::srt::*;
    }
    #[cfg(all(
        feature = "websocket",
        feature = "udplite",
        any(target_os = "linux", target_os = "android")
    ))]
    pub mod udplite {
        pub use crate::udplite_client::*;
        pub use json_rpc_2_server::transports::websocket::udplite::*;
    }

    #[cfg(all(feature = "websocket", feature = "http3"))]
    pub mod http3 {
        pub use json_rpc_2_client::transports::websocket::http3::*;
        pub use json_rpc_2_server::transports::websocket::http3::*;
    }

    #[cfg(all(feature = "websocket", feature = "quic"))]
    pub mod quic {
        pub use json_rpc_2_client::transports::websocket::quic::*;
        pub use json_rpc_2_server::transports::websocket::quic::*;
    }

    #[cfg(all(feature = "websocket", feature = "http1"))]
    pub mod http1 {
        pub use json_rpc_2_client::transports::websocket::http1::*;
        pub use json_rpc_2_server::transports::websocket::http1::*;
    }

    #[cfg(all(feature = "websocket", feature = "tcp"))]
    pub mod tcp {
        pub use json_rpc_2_client::transports::websocket::tcp::*;
        pub use json_rpc_2_server::transports::websocket::tcp::*;
    }
}

pub mod client {
    pub use json_rpc_2_client::{
        ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
        RpcClientTransport, TransportError, expects_response,
    };
    pub use json_rpc_2_core::*;

    pub mod transports {
        #[cfg(feature = "websocket")]
        pub use json_rpc_2_client::transports::websocket::{
            DatagramWebSocketClientTransport, WebSocketCarrier, WebSocketClientTransport,
        };

        #[cfg(all(feature = "websocket", feature = "dccp"))]
        pub mod dccp {
            pub use json_rpc_2_client::transports::websocket::dccp::*;
        }
        #[cfg(all(feature = "websocket", feature = "sctp"))]
        pub mod sctp {
            pub use json_rpc_2_client::transports::websocket::sctp::*;
        }
        #[cfg(all(feature = "websocket", feature = "udp"))]
        pub mod udp {
            pub use json_rpc_2_client::transports::websocket::udp::*;
        }
        #[cfg(all(feature = "websocket", feature = "mqtt"))]
        pub mod mqtt {
            pub use json_rpc_2_client::transports::websocket::mqtt::*;
        }
        #[cfg(all(feature = "websocket", feature = "coap"))]
        pub mod coap {
            pub use json_rpc_2_client::transports::websocket::coap::*;
        }
        #[cfg(all(feature = "websocket", feature = "amqp"))]
        pub mod amqp {
            pub use json_rpc_2_client::transports::websocket::amqp::*;
        }
        #[cfg(all(feature = "websocket", feature = "rtp"))]
        pub mod rtp {
            pub use crate::rtp_client::*;
        }
        #[cfg(all(feature = "websocket", feature = "rudp"))]
        pub mod rudp {
            pub use crate::rudp_client::*;
        }
        #[cfg(all(feature = "websocket", feature = "srt"))]
        pub mod srt {
            pub use crate::srt_client::*;
        }
        #[cfg(all(
            feature = "websocket",
            feature = "udplite",
            any(target_os = "linux", target_os = "android")
        ))]
        pub mod udplite {
            pub use crate::udplite_client::*;
        }
        #[cfg(all(feature = "websocket", feature = "http3"))]
        pub mod http3 {
            pub use json_rpc_2_client::transports::websocket::http3::*;
        }
        #[cfg(all(feature = "websocket", feature = "quic"))]
        pub mod quic {
            pub use json_rpc_2_client::transports::websocket::quic::*;
        }
        #[cfg(all(feature = "websocket", feature = "http1"))]
        pub mod http1 {
            pub use json_rpc_2_client::transports::websocket::http1::*;
        }
        #[cfg(all(feature = "websocket", feature = "tcp"))]
        pub mod tcp {
            pub use json_rpc_2_client::transports::websocket::tcp::*;
        }
    }
}

pub mod server {
    pub use json_rpc_2_core::*;
    pub use json_rpc_2_server::{
        JsonRpcProtocol, RpcServer, ServerMetricsSnapshot, TransportLimits,
    };

    pub mod transports {
        #[cfg(feature = "websocket")]
        pub use json_rpc_2_server::CancellationToken;
        #[cfg(any(feature = "dccp", feature = "sctp"))]
        pub use json_rpc_2_server::protocol_adapter;
        #[cfg(feature = "websocket")]
        pub use json_rpc_2_server::{
            JsonRpcWebSocket, WebSocketFuture, WebSocketMessage, WebSocketProtocol,
            WebSocketTransport, WebSocketTransportFuture,
        };

        #[cfg(all(feature = "websocket", feature = "dccp"))]
        pub mod dccp {
            pub use json_rpc_2_server::transports::websocket::dccp::*;
        }
        #[cfg(all(feature = "websocket", feature = "sctp"))]
        pub mod sctp {
            pub use json_rpc_2_server::transports::websocket::sctp::*;
        }
        #[cfg(all(feature = "websocket", feature = "udp"))]
        pub mod udp {
            pub use json_rpc_2_server::transports::websocket::udp::*;
        }
        #[cfg(all(feature = "websocket", feature = "mqtt"))]
        pub mod mqtt {
            pub use json_rpc_2_server::transports::websocket::mqtt::*;
        }
        #[cfg(all(feature = "websocket", feature = "coap"))]
        pub mod coap {
            pub use json_rpc_2_server::transports::websocket::coap::*;
        }
        #[cfg(all(feature = "websocket", feature = "amqp"))]
        pub mod amqp {
            pub use json_rpc_2_server::transports::websocket::amqp::*;
        }
        #[cfg(all(feature = "websocket", feature = "rtp"))]
        pub mod rtp {
            pub use json_rpc_2_server::transports::websocket::rtp::*;
        }
        #[cfg(all(feature = "websocket", feature = "rudp"))]
        pub mod rudp {
            pub use json_rpc_2_server::transports::websocket::rudp::*;
        }
        #[cfg(all(feature = "websocket", feature = "srt"))]
        pub mod srt {
            pub use json_rpc_2_server::transports::websocket::srt::*;
        }
        #[cfg(all(
            feature = "websocket",
            feature = "udplite",
            any(target_os = "linux", target_os = "android")
        ))]
        pub mod udplite {
            pub use json_rpc_2_server::transports::websocket::udplite::*;
        }
        #[cfg(all(feature = "websocket", feature = "http3"))]
        pub mod http3 {
            pub use json_rpc_2_server::transports::websocket::http3::*;
        }
        #[cfg(all(feature = "websocket", feature = "quic"))]
        pub mod quic {
            pub use json_rpc_2_server::transports::websocket::quic::*;
        }
        #[cfg(all(feature = "websocket", feature = "http1"))]
        pub mod http1 {
            pub use json_rpc_2_server::transports::websocket::http1::*;
        }
        #[cfg(all(feature = "websocket", feature = "tcp"))]
        pub mod tcp {
            pub use json_rpc_2_server::transports::websocket::tcp::*;
        }
    }
}
#[cfg(all(feature = "websocket", feature = "rtp"))]
mod rtp_client;
#[cfg(all(feature = "websocket", feature = "rudp"))]
mod rudp_client;
#[cfg(all(feature = "websocket", feature = "srt"))]
mod srt_client;
#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
mod udplite_client;
