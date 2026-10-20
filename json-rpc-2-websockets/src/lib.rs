pub use json_rpc_2_core::*;
pub use json_rpc_2_transport::*;

pub mod client;
pub mod server;

pub mod transports {
    #[cfg(feature = "websocket")]
    pub use crate::client::transports::{
        DatagramWebSocketClientTransport, WebSocketCarrier, WebSocketClientTransport,
    };
    #[cfg(any(feature = "dccp", feature = "sctp"))]
    pub use crate::server::transports::protocol_adapter;
    #[cfg(feature = "websocket")]
    pub use crate::server::transports::{
        JsonRpcWebSocket, WebSocketFuture, WebSocketMessage, WebSocketProtocol, WebSocketTransport,
        WebSocketTransportFuture,
    };

    #[cfg(all(feature = "websocket", feature = "dccp"))]
    pub mod dccp {
        pub use crate::client::transports::dccp::*;
        pub use crate::server::transports::dccp::*;
    }

    #[cfg(all(feature = "websocket", feature = "sctp"))]
    pub mod sctp {
        pub use crate::client::transports::sctp::*;
        pub use crate::server::transports::sctp::*;
    }

    #[cfg(all(feature = "websocket", feature = "udp"))]
    pub mod udp {
        pub use crate::client::transports::udp::*;
        pub use crate::server::transports::udp::*;
    }

    #[cfg(all(feature = "websocket", feature = "mqtt"))]
    pub mod mqtt {
        pub use crate::client::transports::mqtt::*;
        pub use crate::server::transports::mqtt::*;
    }
    #[cfg(all(feature = "websocket", feature = "coap"))]
    pub mod coap {
        pub use crate::client::transports::coap::*;
        pub use crate::server::transports::coap::*;
    }
    #[cfg(all(feature = "websocket", feature = "amqp"))]
    pub mod amqp {
        pub use crate::client::transports::amqp::*;
        pub use crate::server::transports::amqp::*;
    }

    #[cfg(all(feature = "websocket", feature = "rtp"))]
    pub mod rtp {
        pub use crate::client::transports::rtp::*;
        pub use crate::server::transports::rtp::*;
    }
    #[cfg(all(feature = "websocket", feature = "rudp"))]
    pub mod rudp {
        pub use crate::client::transports::rudp::*;
        pub use crate::server::transports::rudp::*;
    }
    #[cfg(all(feature = "websocket", feature = "srt"))]
    pub mod srt {
        pub use crate::client::transports::srt::*;
        pub use crate::server::transports::srt::*;
    }
    #[cfg(all(
        feature = "websocket",
        feature = "udplite",
        any(target_os = "linux", target_os = "android")
    ))]
    pub mod udplite {
        pub use crate::client::transports::udplite::*;
        pub use crate::server::transports::udplite::*;
    }

    #[cfg(all(feature = "websocket", feature = "http3"))]
    pub mod http3 {
        pub use crate::client::transports::http3::*;
        pub use crate::server::transports::http3::*;
    }

    #[cfg(all(feature = "websocket", feature = "quic"))]
    pub mod quic {
        pub use crate::client::transports::quic::*;
        pub use crate::server::transports::quic::*;
    }

    #[cfg(all(feature = "websocket", feature = "http1"))]
    pub mod http1 {
        pub use crate::client::transports::http1::*;
        pub use crate::server::transports::http1::*;
    }

    #[cfg(all(feature = "websocket", feature = "tcp"))]
    pub mod tcp {
        pub use crate::client::transports::tcp::*;
        pub use crate::server::transports::tcp::*;
    }
}
