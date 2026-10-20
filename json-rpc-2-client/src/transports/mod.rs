//! Shared client-side transport support.

use crate::TransportError;
#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
use socket2::{Domain, Protocol, SockRef, Socket, Type};
#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
use std::{io, net::SocketAddr};
#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
use tokio::net::TcpStream;

#[cfg(feature = "amqp")]
pub mod amqp;
#[cfg(feature = "coap")]
pub mod coap;
#[cfg(feature = "dccp")]
pub mod dccp;
#[cfg(feature = "http1")]
pub mod http1;
#[cfg(feature = "http3")]
pub mod http3;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "rtp")]
pub mod rtp;
#[cfg(feature = "rudp")]
pub mod rudp;
#[cfg(feature = "sctp")]
pub mod sctp;
#[cfg(feature = "srt")]
pub mod srt;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(all(feature = "udplite", any(target_os = "linux", target_os = "android")))]
pub mod udplite;
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "websocket")]
pub mod websocket;

pub(crate) fn transport_error(error: impl std::fmt::Display) -> TransportError {
    TransportError::new(error.to_string())
}

pub(crate) fn closed() -> TransportError {
    TransportError::new("transport is closed")
}

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
pub(crate) async fn connect_tcp_stream(
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

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
fn apply_pre_connect_tuning(
    socket: &Socket,
    tuning: &json_rpc_2_core::TcpTuningConfig,
) -> io::Result<()> {
    apply_tcp_congestion_socket(socket, tuning.congestion_control)?;
    apply_tcp_fast_open_connect(socket, tuning)
}

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
fn apply_post_connect_tuning(
    stream: &TcpStream,
    tuning: &json_rpc_2_core::TcpTuningConfig,
) -> io::Result<()> {
    let socket = SockRef::from(stream);
    socket.set_tcp_nodelay(tuning.no_delay)?;
    apply_tcp_congestion_sockref(&socket, tuning.congestion_control)
}

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
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

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
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

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
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

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
fn apply_tcp_fast_open_connect(
    socket: &Socket,
    tuning: &json_rpc_2_core::TcpTuningConfig,
) -> io::Result<()> {
    if !tuning.fast_open {
        return Ok(());
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
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

#[cfg(any(feature = "tcp", feature = "http1", feature = "mqtt", feature = "amqp"))]
#[cfg(any(target_os = "linux", target_os = "android"))]
fn apply_tcp_congestion_raw(
    socket: &(impl std::os::fd::AsRawFd + ?Sized),
    congestion: json_rpc_2_core::TcpCongestionControl,
) -> io::Result<()> {
    let algorithm = match congestion {
        json_rpc_2_core::TcpCongestionControl::SystemDefault => return Ok(()),
        json_rpc_2_core::TcpCongestionControl::Cubic => b"cubic\0".as_slice(),
        json_rpc_2_core::TcpCongestionControl::Bbr => b"bbr\0".as_slice(),
    };
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CONGESTION,
            algorithm.as_ptr().cast(),
            algorithm.len() as libc::socklen_t,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(feature = "http3")]
pub(crate) fn uri_host(server_name: &str) -> String {
    match server_name.parse::<std::net::Ipv6Addr>() {
        Ok(ip) => format!("[{ip}]"),
        Err(_) => server_name.to_owned(),
    }
}
