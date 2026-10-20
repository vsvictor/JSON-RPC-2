pub use json_rpc_2_core::*;

pub mod client;
pub mod server;

pub mod transports {
    pub use json_rpc_2_core::{
        CancellationToken, RunningTransport, RuntimeMetrics, RuntimeMetricsSnapshot, Transport,
        TransportFuture, TransportRuntime, log_control, log_json_rpc, log_transport_error,
        multiplex,
    };
    pub use json_rpc_2_core::{CONTROL_PATH, CONTROL_PING, CONTROL_PONG, H3_NO_ERROR, QUIC_NO_ERROR};
    pub use json_rpc_2_core::{closed, transport_error};

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
