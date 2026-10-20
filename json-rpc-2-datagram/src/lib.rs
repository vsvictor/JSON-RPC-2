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
    pub use crate::client::transports::{closed, transport_error};
    pub use json_rpc_2_server::transports::{
        CancellationToken, RunningTransport, RuntimeMetrics, RuntimeMetricsSnapshot, Transport,
        TransportFuture, TransportRuntime,
    };
    pub const CONTROL_PATH: &str = json_rpc_2_server::CONTROL_PATH;
    pub const CONTROL_PING: &[u8] = json_rpc_2_server::CONTROL_PING;
    pub const CONTROL_PONG: &[u8] = json_rpc_2_server::CONTROL_PONG;
    pub const H3_NO_ERROR: u32 = json_rpc_2_server::H3_NO_ERROR;
    pub const QUIC_NO_ERROR: u32 = json_rpc_2_server::QUIC_NO_ERROR;

    pub fn log_control(
        transport: &str,
        peer: impl std::fmt::Display,
        action: &str,
        response: &str,
    ) {
        json_rpc_2_server::log_control(transport, peer, action, response)
    }

    pub fn log_json_rpc(
        transport: &str,
        peer: impl std::fmt::Display,
        request: &[u8],
        response: Option<&[u8]>,
        latency: std::time::Duration,
    ) {
        json_rpc_2_server::log_json_rpc(transport, peer, request, response, latency)
    }

    pub fn log_transport_error(
        transport: &str,
        peer: impl std::fmt::Display,
        stage: &'static str,
        error: &dyn std::fmt::Display,
    ) {
        json_rpc_2_server::log_transport_error(transport, peer, stage, error)
    }

    pub mod multiplex {
        pub use json_rpc_2_server::{StreamMultiplexor, StreamMultiplexorHandle};
    }

    #[cfg(feature = "dccp")]
    pub mod dccp {
        pub use crate::client::transports::dccp::*;
        pub use crate::server::transports::dccp::*;
    }

    #[cfg(feature = "sctp")]
    pub mod sctp {
        pub use crate::client::transports::sctp::*;
        pub use crate::server::transports::sctp::*;
    }

    #[cfg(feature = "udp")]
    pub mod udp {
        pub use crate::client::transports::udp::*;
        pub use crate::server::transports::udp::*;
    }
    #[cfg(feature = "rtp")]
    pub mod rtp {
        pub use crate::client::transports::rtp::*;
        pub use crate::server::transports::rtp::*;
    }
    #[cfg(feature = "rudp")]
    pub mod rudp {
        pub use crate::client::transports::rudp::*;
        pub use crate::server::transports::rudp::*;
    }
    #[cfg(feature = "srt")]
    pub mod srt {
        pub use crate::client::transports::srt::*;
        pub use crate::server::transports::srt::*;
    }
    #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
    pub mod udplite {
        pub use crate::client::transports::udplite::*;
        pub use crate::server::transports::udplite::*;
    }
}

pub mod client {
    pub use json_rpc_2_client::{
        ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
        RpcClientTransport, TransportError, expects_response,
    };
    pub use json_rpc_2_core::*;

    pub mod transports {
        use crate::client::TransportError;

        #[cfg(feature = "dccp")]
        pub mod dccp {
            pub use json_rpc_2_client::transports::dccp::*;
        }
        #[cfg(feature = "sctp")]
        pub mod sctp {
            pub use json_rpc_2_client::transports::sctp::*;
        }
        #[cfg(feature = "udp")]
        pub mod udp {
            pub use json_rpc_2_client::transports::udp::*;
        }
        #[cfg(feature = "rtp")]
        pub mod rtp {
            pub use json_rpc_2_client::transports::rtp::*;
        }
        #[cfg(feature = "rudp")]
        pub mod rudp {
            pub use json_rpc_2_client::transports::rudp::*;
        }
        pub fn transport_error(error: impl std::fmt::Display) -> TransportError {
            TransportError::new(error.to_string())
        }

        #[cfg(feature = "srt")]
        pub mod srt {
            pub use json_rpc_2_client::transports::srt::*;
        }
        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        pub mod udplite {
            pub use json_rpc_2_client::transports::udplite::*;
        }

        pub fn closed() -> TransportError {
            TransportError::new("transport is closed")
        }
    }
}

pub mod server {
    pub use json_rpc_2_core::*;
    pub use json_rpc_2_server::{
        JsonRpcProtocol, RpcServer, ServerMetricsSnapshot, TransportLimits,
    };

    pub mod transports {
        pub use json_rpc_2_server::transports::{
            CancellationToken, RunningTransport, RuntimeMetrics, RuntimeMetricsSnapshot, Transport,
            TransportFuture, TransportRuntime,
        };
        pub const CONTROL_PATH: &str = json_rpc_2_server::CONTROL_PATH;
        pub const CONTROL_PING: &[u8] = json_rpc_2_server::CONTROL_PING;
        pub const CONTROL_PONG: &[u8] = json_rpc_2_server::CONTROL_PONG;
        pub const H3_NO_ERROR: u32 = json_rpc_2_server::H3_NO_ERROR;
        pub const QUIC_NO_ERROR: u32 = json_rpc_2_server::QUIC_NO_ERROR;

        pub fn log_control(
            transport: &str,
            peer: impl std::fmt::Display,
            action: &str,
            response: &str,
        ) {
            json_rpc_2_server::log_control(transport, peer, action, response)
        }

        pub fn log_json_rpc(
            transport: &str,
            peer: impl std::fmt::Display,
            request: &[u8],
            response: Option<&[u8]>,
            latency: std::time::Duration,
        ) {
            json_rpc_2_server::log_json_rpc(transport, peer, request, response, latency)
        }

        pub fn log_transport_error(
            transport: &str,
            peer: impl std::fmt::Display,
            stage: &'static str,
            error: &dyn std::fmt::Display,
        ) {
            json_rpc_2_server::log_transport_error(transport, peer, stage, error)
        }

        pub mod multiplex {
            pub use json_rpc_2_server::{StreamMultiplexor, StreamMultiplexorHandle};
        }

        #[cfg(feature = "dccp")]
        pub mod dccp {
            pub use json_rpc_2_server::transports::dccp::*;
        }
        #[cfg(feature = "sctp")]
        pub mod sctp {
            pub use json_rpc_2_server::transports::sctp::*;
        }
        #[cfg(feature = "udp")]
        pub mod udp {
            pub use json_rpc_2_server::transports::udp::*;
        }
        #[cfg(feature = "rtp")]
        pub mod rtp {
            pub use json_rpc_2_server::transports::rtp::*;
        }
        #[cfg(feature = "rudp")]
        pub mod rudp {
            pub use json_rpc_2_server::transports::rudp::*;
        }
        #[cfg(feature = "srt")]
        pub mod srt {
            pub use json_rpc_2_server::transports::srt::*;
        }
        #[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
        pub mod udplite {
            pub use json_rpc_2_server::transports::udplite::*;
        }
    }
}
