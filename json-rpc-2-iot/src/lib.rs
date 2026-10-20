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

    #[cfg(feature = "amqp")]
    pub mod amqp {
        pub use crate::client::transports::amqp::*;
        pub use crate::server::transports::amqp::*;
    }
    #[cfg(feature = "coap")]
    pub mod coap {
        pub use crate::client::transports::coap::*;
        pub use crate::server::transports::coap::*;
    }
    #[cfg(feature = "mqtt")]
    pub mod mqtt {
        pub use crate::client::transports::mqtt::*;
        pub use crate::server::transports::mqtt::*;
    }
}
