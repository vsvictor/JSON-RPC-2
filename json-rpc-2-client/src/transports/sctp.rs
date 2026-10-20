//! Stream Control Transmission Protocol (SCTP) transport.

use std::net::SocketAddr;

use bytes::Bytes;
use tokio::sync::Mutex;

use super::{closed, transport_error};
use crate::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, RpcClientTransport, TransportError, expects_response,
};

/// One JSON-RPC message per SCTP packet over a persistent SCTP association.
///
/// The current adapter uses one connected association and preserves packet
/// boundaries through `SOCK_SEQPACKET`. Stream-selection policy can be layered
/// on top later without changing the JSON-RPC contract.
pub struct SctpClientTransport {
    address: SocketAddr,
    connection: Mutex<Option<platform::SctpSocket>>,
}

impl SctpClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Ok(Self {
            address,
            connection: Mutex::new(Some(
                platform::SctpSocket::connect(address)
                    .await
                    .map_err(transport_error)?,
            )),
        })
    }
}

impl RpcClientTransport for SctpClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects_response = expects_response(&message);
            self.send_packet(message, expects_response).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move { self.send_packet(message, expects_response).await })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            let socket = connection.as_mut().ok_or_else(closed)?;
            socket.send(CONTROL_PING).await.map_err(transport_error)?;
            let response = socket.recv().await.map_err(transport_error)?;
            if response == CONTROL_PONG {
                Ok(())
            } else {
                Err(TransportError::new(format!(
                    "unexpected SCTP ping response: {}",
                    String::from_utf8_lossy(&response)
                )))
            }
        })
    }

    fn kind(&self) -> &'static str {
        "sctp"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = platform::SctpSocket::connect(self.address)
                .await
                .map_err(transport_error)?;
            self.connection.lock().await.replace(replacement);
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            self.connection.lock().await.take();
            Ok(())
        })
    }
}

impl SctpClientTransport {
    async fn send_packet(
        &self,
        message: Bytes,
        expects_response: bool,
    ) -> Result<Option<Bytes>, TransportError> {
        let mut connection = self.connection.lock().await;
        let socket = connection.as_mut().ok_or_else(closed)?;
        socket.send(&message).await.map_err(transport_error)?;
        if !expects_response {
            return Ok(None);
        }
        Ok(Some(Bytes::from(
            socket.recv().await.map_err(transport_error)?,
        )))
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod platform {
    use std::{
        io,
        net::SocketAddr,
        os::{
            fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
            raw::c_void,
        },
    };

    use socket2::{Domain, Protocol, SockRef, Socket, Type};
    use tokio::io::unix::AsyncFd;

    use crate::MAX_MESSAGE_BYTES;

    pub(super) struct SctpSocket {
        fd: AsyncFd<OwnedFd>,
    }

    impl SctpSocket {
        pub(super) async fn connect(address: SocketAddr) -> io::Result<Self> {
            let socket = Socket::new(
                Domain::for_address(address),
                Type::SEQPACKET,
                Some(Protocol::from(libc::IPPROTO_SCTP)),
            )?;
            socket.set_nonblocking(true)?;
            match socket.connect(&address.into()) {
                Ok(()) => {}
                Err(error) if connect_in_progress(&error) => {}
                Err(error) => return Err(error),
            }
            let fd = unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) };
            let fd = AsyncFd::new(fd)?;
            wait_connected(&fd).await?;
            Ok(Self { fd })
        }

        pub(super) async fn send(&self, payload: &[u8]) -> io::Result<()> {
            loop {
                let mut guard = self.fd.writable().await?;
                match guard.try_io(|fd| send_raw(fd.get_ref().as_raw_fd(), payload)) {
                    Ok(result) => return result,
                    Err(_would_block) => continue,
                }
            }
        }

        pub(super) async fn recv(&self) -> io::Result<Vec<u8>> {
            let mut buffer = vec![0; MAX_MESSAGE_BYTES];
            loop {
                let mut guard = self.fd.readable().await?;
                match guard.try_io(|fd| recv_raw(fd.get_ref().as_raw_fd(), &mut buffer)) {
                    Ok(result) => {
                        let read = result?;
                        if read == 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "SCTP association closed by server",
                            ));
                        }
                        buffer.truncate(read);
                        return Ok(buffer);
                    }
                    Err(_would_block) => continue,
                }
            }
        }
    }

    async fn wait_connected(fd: &AsyncFd<OwnedFd>) -> io::Result<()> {
        loop {
            let mut guard = fd.writable().await?;
            match guard.try_io(|inner| {
                let socket = SockRef::from(inner.get_ref());
                match socket.take_error()? {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    fn connect_in_progress(error: &io::Error) -> bool {
        matches!(error.raw_os_error(), Some(code) if code == libc::EINPROGRESS || code == libc::EALREADY || code == libc::EWOULDBLOCK)
    }

    fn send_raw(fd: i32, payload: &[u8]) -> io::Result<()> {
        let written =
            unsafe { libc::send(fd, payload.as_ptr().cast::<c_void>(), payload.len(), 0) };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        if written as usize != payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "partial SCTP packet write",
            ));
        }
        Ok(())
    }

    fn recv_raw(fd: i32, buffer: &mut [u8]) -> io::Result<usize> {
        let read = unsafe { libc::recv(fd, buffer.as_mut_ptr().cast::<c_void>(), buffer.len(), 0) };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(read as usize)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
mod platform {
    use std::{io, net::SocketAddr};

    pub(super) struct SctpSocket;

    impl SctpSocket {
        pub(super) async fn connect(_address: SocketAddr) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "SCTP transport is supported only on Linux and Android",
            ))
        }

        pub(super) async fn send(&self, _payload: &[u8]) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "SCTP transport is supported only on Linux and Android",
            ))
        }

        pub(super) async fn recv(&self) -> io::Result<Vec<u8>> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "SCTP transport is supported only on Linux and Android",
            ))
        }
    }
}
