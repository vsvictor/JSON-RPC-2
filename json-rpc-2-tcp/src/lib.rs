pub use json_rpc_2_core::*;
pub use json_rpc_2_transport::*;

pub mod client;
pub mod server;

pub mod transports {
    #[cfg(any(feature = "tcp", feature = "http1"))]
    pub use json_rpc_2_transport::connect_tcp_stream;
    pub use json_rpc_2_transport::{
        CONTROL_PATH, CONTROL_PING, CONTROL_PONG, H3_NO_ERROR, QUIC_NO_ERROR,
    };
    pub use json_rpc_2_transport::{
        CancellationToken, RunningTransport, RuntimeMetrics, RuntimeMetricsSnapshot, Transport,
        TransportFuture, TransportRuntime, apply_tcp_listener_tuning, apply_tcp_stream_tuning,
        log_control, log_json_rpc, log_transport_error, multiplex,
    };
    pub use json_rpc_2_transport::{closed, transport_error};

    #[cfg(feature = "http1")]
    pub mod http1 {
        pub use crate::client::transports::http1::*;
        pub use crate::server::transports::http1::*;
    }
    #[cfg(feature = "tcp")]
    pub mod tcp {
        pub use crate::client::transports::tcp::*;
        pub use crate::server::transports::tcp::*;
    }
}
