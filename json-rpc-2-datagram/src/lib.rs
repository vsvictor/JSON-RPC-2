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
