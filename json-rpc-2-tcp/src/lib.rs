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
    #[cfg(any(feature = "tcp", feature = "http1"))]
    pub use crate::client::transports::connect_tcp_stream;
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

    #[cfg(any(feature = "tcp", feature = "http1"))]
    pub fn apply_tcp_listener_tuning(
        socket: &socket2::Socket,
        tuning: &json_rpc_2_core::TcpTuningConfig,
    ) -> std::io::Result<()> {
        json_rpc_2_server::apply_tcp_listener_tuning(socket, tuning)
    }

    #[cfg(any(feature = "tcp", feature = "http1"))]
    pub fn apply_tcp_stream_tuning(
        stream: &tokio::net::TcpStream,
        tuning: &json_rpc_2_core::TcpTuningConfig,
    ) -> std::io::Result<()> {
        json_rpc_2_server::apply_tcp_stream_tuning(stream, tuning)
    }

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

pub mod client {
    pub use json_rpc_2_client::{
        ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
        RpcClientTransport, TransportError, expects_response,
    };
    pub use json_rpc_2_core::*;

    pub mod transports {
        #[cfg(any(feature = "tcp", feature = "http1"))]
        use socket2::{Domain, Protocol, SockRef, Socket, Type};
        #[cfg(any(feature = "tcp", feature = "http1"))]
        use std::{io, net::SocketAddr};
        #[cfg(any(feature = "tcp", feature = "http1"))]
        use tokio::net::TcpStream;

        use crate::client::TransportError;

        #[cfg(feature = "http1")]
        pub mod http1 {
            pub use json_rpc_2_client::transports::http1::*;
        }
        #[cfg(feature = "tcp")]
        pub mod tcp {
            pub use json_rpc_2_client::transports::tcp::*;
        }
        pub fn transport_error(error: impl std::fmt::Display) -> TransportError {
            TransportError::new(error.to_string())
        }

        pub fn closed() -> TransportError {
            TransportError::new("transport is closed")
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        pub async fn connect_tcp_stream(
            address: SocketAddr,
            tuning: &json_rpc_2_core::TcpTuningConfig,
        ) -> Result<TcpStream, TransportError> {
            let socket = Socket::new(
                Domain::for_address(address),
                Type::STREAM,
                Some(Protocol::TCP),
            )
            .map_err(transport_error)?;
            socket.set_nonblocking(true).map_err(transport_error)?;
            apply_pre_connect_tuning(&socket, tuning).map_err(transport_error)?;
            match socket.connect(&address.into()) {
                Ok(()) => {}
                Err(error) if connect_in_progress(&error) => {}
                Err(error) => return Err(transport_error(error)),
            }
            let stream = TcpStream::from_std(socket.into()).map_err(transport_error)?;
            stream.writable().await.map_err(transport_error)?;
            if let Some(error) = stream.take_error().map_err(transport_error)? {
                return Err(transport_error(error));
            }
            apply_post_connect_tuning(&stream, tuning).map_err(transport_error)?;
            Ok(stream)
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        fn apply_pre_connect_tuning(
            socket: &Socket,
            tuning: &json_rpc_2_core::TcpTuningConfig,
        ) -> io::Result<()> {
            apply_tcp_congestion_socket(socket, tuning.congestion_control)?;
            apply_tcp_fast_open_connect(socket, tuning)
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        pub(crate) fn apply_post_connect_tuning(
            stream: &TcpStream,
            tuning: &json_rpc_2_core::TcpTuningConfig,
        ) -> io::Result<()> {
            let socket = SockRef::from(stream);
            socket.set_tcp_nodelay(tuning.no_delay)?;
            apply_tcp_congestion_sockref(&socket, tuning.congestion_control)
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        fn connect_in_progress(error: &io::Error) -> bool {
            if error.kind() == io::ErrorKind::WouldBlock {
                return true;
            }
            #[cfg(unix)]
            {
                matches!(
                    error.raw_os_error(),
                    Some(libc::EINPROGRESS) | Some(libc::EALREADY)
                )
            }
            #[cfg(not(unix))]
            {
                false
            }
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        fn apply_tcp_congestion_socket(
            _socket: &Socket,
            congestion: json_rpc_2_core::TcpCongestionControl,
        ) -> io::Result<()> {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            {
                apply_tcp_congestion_raw(_socket, congestion)
            }
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            {
                if matches!(
                    congestion,
                    json_rpc_2_core::TcpCongestionControl::SystemDefault
                ) {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "TCP congestion control overrides are only supported on linux/android in this crate",
                    ))
                }
            }
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        fn apply_tcp_congestion_sockref(
            _socket: &SockRef<'_>,
            congestion: json_rpc_2_core::TcpCongestionControl,
        ) -> io::Result<()> {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            {
                apply_tcp_congestion_raw(_socket, congestion)
            }
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            {
                if matches!(
                    congestion,
                    json_rpc_2_core::TcpCongestionControl::SystemDefault
                ) {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "TCP congestion control overrides are only supported on linux/android in this crate",
                    ))
                }
            }
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        #[cfg(any(feature = "tcp", feature = "http1"))]
        fn apply_tcp_congestion_raw<Fd>(
            socket: &Fd,
            congestion: json_rpc_2_core::TcpCongestionControl,
        ) -> io::Result<()>
        where
            Fd: std::os::fd::AsFd,
        {
            let Some(name) = congestion.as_socket_name() else {
                return Ok(());
            };
            use std::os::fd::{AsFd, AsRawFd};

            let result = unsafe {
                libc::setsockopt(
                    socket.as_fd().as_raw_fd(),
                    libc::IPPROTO_TCP,
                    libc::TCP_CONGESTION,
                    name.as_ptr().cast(),
                    name.len() as libc::socklen_t,
                )
            };
            if result == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        fn apply_tcp_fast_open_connect(
            socket: &Socket,
            tuning: &json_rpc_2_core::TcpTuningConfig,
        ) -> io::Result<()> {
            if !tuning.fast_open {
                return Ok(());
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            {
                use std::os::fd::AsRawFd;

                let value: libc::c_int = 1;
                let result = unsafe {
                    libc::setsockopt(
                        socket.as_raw_fd(),
                        libc::IPPROTO_TCP,
                        libc::TCP_FASTOPEN_CONNECT,
                        (&value as *const libc::c_int).cast(),
                        std::mem::size_of_val(&value) as libc::socklen_t,
                    )
                };
                if result == -1 {
                    return Err(io::Error::last_os_error());
                }
                return Ok(());
            }
            let _ = socket;
            #[allow(unreachable_code)]
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "client-side TCP Fast Open is only supported on linux/android in this crate",
            ))
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

        #[cfg(any(feature = "tcp", feature = "http1"))]
        pub fn apply_tcp_listener_tuning(
            socket: &socket2::Socket,
            tuning: &json_rpc_2_core::TcpTuningConfig,
        ) -> std::io::Result<()> {
            json_rpc_2_server::apply_tcp_listener_tuning(socket, tuning)
        }

        #[cfg(any(feature = "tcp", feature = "http1"))]
        pub fn apply_tcp_stream_tuning(
            stream: &tokio::net::TcpStream,
            tuning: &json_rpc_2_core::TcpTuningConfig,
        ) -> std::io::Result<()> {
            json_rpc_2_server::apply_tcp_stream_tuning(stream, tuning)
        }

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

        #[cfg(feature = "http1")]
        pub mod http1 {
            pub use json_rpc_2_server::transports::http1::*;
        }
        #[cfg(feature = "tcp")]
        pub mod tcp {
            pub use json_rpc_2_server::transports::tcp::*;
        }
    }
}
