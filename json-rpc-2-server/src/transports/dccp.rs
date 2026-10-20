//! Datagram Congestion Control Protocol (DCCP) transport.

use std::{io, net::SocketAddr, sync::Arc, time::Instant};

use super::{
    CONTROL_PING, CONTROL_PONG, CancellationToken, Transport, TransportFuture, TransportRuntime,
    log_control, log_json_rpc, log_transport_error, multiplex::StreamMultiplexor,
};
use crate::JsonRpcProtocol;

/// One JSON-RPC message per DCCP datagram over a persistent DCCP session.
pub struct DccpTransport {
    listener: platform::DccpListener,
    runtime: Arc<TransportRuntime>,
}

impl DccpTransport {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Ok(Self::new(platform::DccpListener::bind(address)?))
    }

    pub(crate) fn new(listener: platform::DccpListener) -> Self {
        Self {
            listener,
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
    }
}

impl Transport for DccpTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.listener, protocol, shutdown, self.runtime))
    }
}

pub(crate) async fn serve(
    listener: platform::DccpListener,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let connections = multiplexor.handle();
    let result = loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = shutdown.cancelled() => break Ok(()),
        };
        let stream = match accepted {
            Ok(stream) => stream,
            Err(error) => break Err(error),
        };
        let protocol = protocol.clone();
        let shutdown = shutdown.clone();
        connections
            .spawn(async move {
                if let Err(error) = handle_connection(stream, protocol, shutdown).await {
                    log_transport_error("DCCP", "listener", "connection", &error);
                }
            })
            .await?;
    };
    drop(listener);
    shutdown.cancel();
    multiplexor.close().await;
    result
}

async fn handle_connection(
    stream: platform::DccpStream,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let peer = stream.peer_addr();
    loop {
        let message = tokio::select! {
            message = stream.recv() => message,
            _ = shutdown.cancelled() => break,
        }?;
        if message.is_empty() {
            break;
        }
        if message.as_slice() == CONTROL_PING {
            log_control("DCCP", peer, "ping", "pong");
            stream.send(CONTROL_PONG).await?;
            continue;
        }
        let started = Instant::now();
        let response = protocol.handle_message(&message).await;
        log_json_rpc(
            "DCCP",
            peer,
            &message,
            response.as_deref(),
            started.elapsed(),
        );
        if let Some(response) = response {
            stream.send(&response).await?;
        }
    }
    Ok(())
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

    use socket2::{Domain, Protocol, SockAddr, SockRef, Socket, Type};
    use tokio::io::unix::AsyncFd;

    use crate::MAX_MESSAGE_BYTES;

    pub(crate) struct DccpListener {
        fd: AsyncFd<OwnedFd>,
        local_addr: SocketAddr,
    }

    impl DccpListener {
        pub(super) fn bind(address: SocketAddr) -> io::Result<Self> {
            let socket = Socket::new(
                Domain::for_address(address),
                Type::from(libc::SOCK_DCCP),
                Some(Protocol::from(libc::IPPROTO_DCCP)),
            )?;
            if address.is_ipv6() {
                socket.set_only_v6(true)?;
            }
            socket.set_reuse_address(true)?;
            socket.bind(&SockAddr::from(address))?;
            socket.listen(1024)?;
            socket.set_nonblocking(true)?;
            let local_addr = socket.local_addr()?.as_socket().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "DCCP listener without address",
                )
            })?;
            let fd = unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) };
            Ok(Self {
                fd: AsyncFd::new(fd)?,
                local_addr,
            })
        }

        pub(super) fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        pub(super) async fn accept(&self) -> io::Result<DccpStream> {
            loop {
                let mut guard = self.fd.readable().await?;
                match guard.try_io(|fd| accept_raw(fd.get_ref().as_raw_fd())) {
                    Ok(result) => return result.and_then(DccpStream::new),
                    Err(_would_block) => continue,
                }
            }
        }
    }

    pub(crate) struct DccpStream {
        fd: AsyncFd<OwnedFd>,
        peer: SocketAddr,
    }

    impl DccpStream {
        fn new((fd, peer): (OwnedFd, SocketAddr)) -> io::Result<Self> {
            Ok(Self {
                fd: AsyncFd::new(fd)?,
                peer,
            })
        }

        pub(super) fn peer_addr(&self) -> SocketAddr {
            self.peer
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
                        buffer.truncate(read);
                        return Ok(buffer);
                    }
                    Err(_would_block) => continue,
                }
            }
        }
    }

    fn accept_raw(fd: i32) -> io::Result<(OwnedFd, SocketAddr)> {
        let accepted = unsafe {
            libc::accept4(
                fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            )
        };
        if accepted < 0 {
            return Err(io::Error::last_os_error());
        }
        let owned = unsafe { OwnedFd::from_raw_fd(accepted) };
        let peer = SockRef::from(&owned)
            .peer_addr()?
            .as_socket()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "DCCP peer without socket address",
                )
            })?;
        Ok((owned, peer))
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
                "partial DCCP datagram write",
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

    #[derive(Clone, Debug)]
    pub(crate) struct DccpListener;

    impl DccpListener {
        pub(super) fn bind(_address: SocketAddr) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DCCP transport is supported only on Linux and Android",
            ))
        }

        pub(super) fn local_addr(&self) -> SocketAddr {
            SocketAddr::from(([0, 0, 0, 0], 0))
        }

        pub(super) async fn accept(&self) -> io::Result<DccpStream> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DCCP transport is supported only on Linux and Android",
            ))
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct DccpStream;

    impl DccpStream {
        pub(super) fn peer_addr(&self) -> SocketAddr {
            SocketAddr::from(([0, 0, 0, 0], 0))
        }

        pub(super) async fn send(&self, _payload: &[u8]) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DCCP transport is supported only on Linux and Android",
            ))
        }

        pub(super) async fn recv(&self) -> io::Result<Vec<u8>> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DCCP transport is supported only on Linux and Android",
            ))
        }
    }
}
