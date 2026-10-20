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
    #[cfg(feature = "http3")]
    pub use crate::client::transports::uri_host;
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

    #[cfg(feature = "http3")]
    pub mod http3 {
        pub use crate::client::transports::http3::*;
        pub use crate::server::transports::http3::*;
    }

    #[cfg(feature = "quic")]
    pub mod quic {
        pub use crate::client::transports::quic::*;
        pub use crate::server::transports::quic::*;
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

        #[cfg(feature = "http3")]
        pub mod http3 {
            pub use json_rpc_2_client::transports::http3::*;
        }
        #[cfg(feature = "quic")]
        pub mod quic {
            pub use json_rpc_2_client::transports::quic::*;
        }
        pub fn transport_error(error: impl std::fmt::Display) -> TransportError {
            TransportError::new(error.to_string())
        }

        pub fn closed() -> TransportError {
            TransportError::new("transport is closed")
        }

        #[cfg(feature = "http3")]
        pub fn uri_host(server_name: &str) -> String {
            match server_name.parse::<std::net::Ipv6Addr>() {
                Ok(ip) => format!("[{ip}]"),
                Err(_) => server_name.to_owned(),
            }
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

        #[cfg(feature = "http3")]
        pub mod http3 {
            pub use json_rpc_2_server::transports::http3::*;
        }
        #[cfg(feature = "quic")]
        pub mod quic {
            pub use json_rpc_2_server::transports::quic::*;
        }
    }
}
