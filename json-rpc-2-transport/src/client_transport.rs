//! Transport-independent client contract.
//!
//! A client transport only handles message framing and delivery; JSON-RPC
//! semantics are implemented once in `json-rpc-2-client` on top of this trait.

use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc};

use bytes::Bytes;
use serde_json::Value;
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::TcpCongestionControl;

/// Future returned by a client transport.
pub type ClientTransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Bytes>, TransportError>> + Send + 'a>>;

/// Future returned by a graceful transport close.
pub type ClientCloseFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;
/// Future returned by a transport-level liveness check.
pub type ClientPingFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;
/// Future returned while recovering a broken transport connection.
pub type ClientRecoverFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;

/// Transport boundary used by the client.
///
/// `None` represents a transport-level empty response, such as HTTP 204.
pub trait RpcClientTransport: Send + Sync + 'static {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a>;

    /// Send a serialized JSON-RPC message when the caller already knows
    /// whether a response is expected. Stream transports override this to
    /// avoid re-parsing their own outbound JSON just to detect notifications.
    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        let _ = expects_response;
        self.send(message)
    }

    /// Send a message while observing transport-level cancellation.
    ///
    /// Multiplexed transports should override this so a cancelled JSON-RPC
    /// request also tears down its dedicated carrier stream instead of only
    /// dropping the waiting future in the client session.
    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            tokio::select! {
                _ = cancel.cancelled() => Err(TransportError::new("request cancelled")),
                result = self.send_with_expectation(message, expects_response) => result,
            }
        })
    }

    /// Short transport identifier for logs and metrics correlation.
    fn kind(&self) -> &'static str {
        "unknown"
    }

    /// Remote peer identifier for logs and metrics correlation.
    fn peer(&self) -> Option<String> {
        None
    }

    /// Perform a transport-level ping/pong without entering the JSON-RPC
    /// protocol handler, so control traffic stays separate from application
    /// requests and responses.
    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a>;

    /// Best-effort recovery of the current connection without fully
    /// rebuilding it. The default implementation retries one transport-level
    /// ping on the existing connection.
    fn restore<'a>(&'a self) -> ClientRecoverFuture<'a> {
        self.transport_ping()
    }

    /// Rebuild the transport connection from its original connect settings.
    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a>;

    /// Gracefully close the transport: finish outstanding streams, perform the
    /// transport's closing handshake and release network resources.
    ///
    /// After a successful close, `send` must fail. Closing an already closed
    /// transport must succeed.
    fn close<'a>(&'a self) -> ClientCloseFuture<'a>;
}

impl<T> RpcClientTransport for Arc<T>
where
    T: RpcClientTransport + ?Sized,
{
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        (**self).send(message)
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        (**self).send_with_expectation(message, expects_response)
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        (**self).send_cancelable(message, expects_response, cancel)
    }

    fn kind(&self) -> &'static str {
        (**self).kind()
    }

    fn peer(&self) -> Option<String> {
        (**self).peer()
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        (**self).transport_ping()
    }

    fn restore<'a>(&'a self) -> ClientRecoverFuture<'a> {
        (**self).restore()
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        (**self).reconnect()
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        (**self).close()
    }
}

/// Whether a serialized JSON-RPC message requires the server to reply.
///
/// Stream transports (TCP, WebSocket) use this to avoid waiting for a
/// response to a notification or to a batch made only of notifications.
/// Malformed messages yield `true` because the server answers them with an
/// error response.
pub fn expects_response(message: &[u8]) -> bool {
    match serde_json::from_slice::<Value>(message) {
        Ok(Value::Object(object)) => object.contains_key("id"),
        Ok(Value::Array(items)) => {
            items.is_empty()
                || items.iter().any(|item| {
                    item.as_object()
                        .is_none_or(|object| object.contains_key("id"))
                })
        }
        _ => true,
    }
}

/// Error raised by a concrete network transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError {
    message: String,
}

impl TransportError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TransportError {}

/// Shared helper for mapping any displayable error into a [`TransportError`].
pub fn transport_error(error: impl std::fmt::Display) -> TransportError {
    TransportError::new(error.to_string())
}

/// Shared "transport is closed" error used by every client transport.
pub fn closed() -> TransportError {
    TransportError::new("transport is closed")
}

/// Connect a TCP stream honoring the shared tuning knobs (congestion control,
/// Fast Open, no-delay) used by every TCP-carried client transport (raw TCP,
/// HTTP/1.1, MQTT, AMQP, and their WebSocket variants).
pub async fn connect_tcp_stream(
    address: SocketAddr,
    tuning: &crate::TcpTuningConfig,
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

fn apply_pre_connect_tuning(socket: &Socket, tuning: &crate::TcpTuningConfig) -> io::Result<()> {
    apply_tcp_congestion_socket(socket, tuning.congestion_control)?;
    apply_tcp_fast_open_connect(socket, tuning)
}

pub(crate) fn apply_post_connect_tuning(
    stream: &TcpStream,
    tuning: &crate::TcpTuningConfig,
) -> io::Result<()> {
    let socket = SockRef::from(stream);
    socket.set_tcp_nodelay(tuning.no_delay)?;
    apply_tcp_congestion_sockref(&socket, tuning.congestion_control)
}

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

fn apply_tcp_congestion_socket(
    _socket: &Socket,
    congestion: TcpCongestionControl,
) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        apply_tcp_congestion_raw(_socket, congestion)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        if matches!(congestion, TcpCongestionControl::SystemDefault) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TCP congestion control overrides are only supported on linux/android in this crate",
            ))
        }
    }
}

fn apply_tcp_congestion_sockref(
    _socket: &SockRef<'_>,
    congestion: TcpCongestionControl,
) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        apply_tcp_congestion_raw(_socket, congestion)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        if matches!(congestion, TcpCongestionControl::SystemDefault) {
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
fn apply_tcp_congestion_raw(socket: &Socket, congestion: TcpCongestionControl) -> io::Result<()>
{
    let Some(name) = congestion.as_socket_name() else {
        return Ok(());
    };
    use std::os::fd::AsRawFd;

    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
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

fn apply_tcp_fast_open_connect(socket: &Socket, tuning: &crate::TcpTuningConfig) -> io::Result<()> {
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
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = socket;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "client-side TCP Fast Open is only supported on linux/android in this crate",
        ))
    }
}
