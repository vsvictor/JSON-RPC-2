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

    #[cfg(feature = "mqtt")]
    pub mod mqtt {
        pub use crate::client::transports::mqtt::*;
        pub use crate::server::transports::mqtt::*;
    }
    #[cfg(feature = "coap")]
    pub mod coap {
        pub use crate::client::transports::coap::*;
        pub use crate::server::transports::coap::*;
    }
    #[cfg(feature = "amqp")]
    pub mod amqp {
        pub use crate::client::transports::amqp::*;
        pub use crate::server::transports::amqp::*;
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

        #[cfg(feature = "mqtt")]
        pub mod mqtt {
            pub use json_rpc_2_client::transports::mqtt::*;
        }
        #[cfg(feature = "coap")]
        pub mod coap {
            pub use json_rpc_2_client::transports::coap::*;
        }
        #[cfg(feature = "amqp")]
        pub mod amqp {
            pub use json_rpc_2_client::transports::amqp::*;
        }

        pub fn transport_error(error: impl std::fmt::Display) -> TransportError {
            TransportError::new(error.to_string())
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

        #[cfg(feature = "mqtt")]
        pub mod mqtt {
            pub use json_rpc_2_server::transports::mqtt::*;
        }
        #[cfg(feature = "coap")]
        pub mod coap {
            pub use json_rpc_2_server::transports::coap::*;
        }
        #[cfg(feature = "amqp")]
        pub mod amqp {
            pub use json_rpc_2_server::transports::amqp::*;
        }
    }
}
