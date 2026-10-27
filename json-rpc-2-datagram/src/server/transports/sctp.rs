//! Stream Control Transmission Protocol (SCTP) transport.

use std::{io, net::SocketAddr, sync::Arc, time::Instant};

use json_rpc_2_transport::JsonRpcProtocol;
use json_rpc_2_transport::multiplex::StreamMultiplexor;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, CancellationToken, Transport, TransportFuture, TransportRuntime,
    log_control, log_json_rpc, log_transport_error,
};

/// One JSON-RPC message per SCTP packet over a persistent SCTP association.
pub struct SctpTransport {
    listener: platform::SctpListener,
    runtime: Arc<TransportRuntime>,
}

impl SctpTransport {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Ok(Self::new(platform::SctpListener::bind(address)?))
    }

    pub(crate) fn new(listener: platform::SctpListener) -> Self {
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

impl Transport for SctpTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.listener, protocol, shutdown, self.runtime))
    }
}

pub(crate) async fn serve(
    listener: platform::SctpListener,
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
                    log_transport_error("SCTP", "listener", "connection", &error);
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
    stream: platform::SctpStream,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let peer = stream.peer_addr();
    let mut session = json_rpc_2_transport::TransportSession::new("sctp", true);
    let _guard =
        json_rpc_2_transport::ProtocolSessionGuard::new(protocol.clone(), session.context.clone());
    let serving = async {
        loop {
            let receive = stream.recv();
            tokio::pin!(receive);
            let message = loop {
                tokio::select! {
                    message = &mut receive => break message?,
                    Some(push) = session.push_receiver.recv() => stream.send(&push).await?,
                    _ = shutdown.cancelled() => return Ok(()),
                }
            };
            if message.is_empty() {
                break;
            }
            if message.as_slice() == CONTROL_PING {
                log_control("SCTP", peer, "ping", "pong");
                stream.send(CONTROL_PONG).await?;
                continue;
            }
            let started = Instant::now();
            let message = bytes::Bytes::from(message);
            let response = protocol
                .handle_owned_message(message.clone(), &session.context)
                .await
                .map(json_rpc_2_transport::TransportMessage::into_bytes);
            if protocol.session_close_requested(&session.context) {
                break;
            }
            log_json_rpc(
                "SCTP",
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
    };
    tokio::select! {
        biased;
        _ = protocol.session_cancelled(&session.context) => Ok(()),
        result = serving => result,
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

    use socket2::{Domain, Protocol, SockAddr, SockRef, Socket, Type};
    use tokio::io::unix::AsyncFd;

    pub(crate) struct SctpListener {
        fd: AsyncFd<OwnedFd>,
        local_addr: SocketAddr,
    }

    impl SctpListener {
        pub(super) fn bind(address: SocketAddr) -> io::Result<Self> {
            let socket = Socket::new(
                Domain::for_address(address),
                Type::STREAM,
                Some(Protocol::from(libc::IPPROTO_SCTP)),
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
                    "SCTP listener without address",
                )
            })?;
            let fd = unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) };
            // SAFETY: OwnedFd keeps the same descriptor open until AsyncFd is dropped.
            let fd = unsafe { AsyncFd::register(fd) }?;
            Ok(Self { fd, local_addr })
        }

        pub(super) fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        pub(super) async fn accept(&self) -> io::Result<SctpStream> {
            loop {
                let mut guard = self.fd.readable().await?;
                match guard.try_io(|fd| accept_raw(fd.get_ref().as_raw_fd())) {
                    Ok(result) => return result.and_then(SctpStream::new),
                    Err(_would_block) => continue,
                }
            }
        }
    }

    pub(crate) struct SctpStream {
        fd: AsyncFd<OwnedFd>,
        peer: SocketAddr,
    }

    impl SctpStream {
        fn new((fd, peer): (OwnedFd, SocketAddr)) -> io::Result<Self> {
            // SAFETY: OwnedFd keeps the same descriptor open until AsyncFd is dropped.
            let fd = unsafe { AsyncFd::register(fd) }?;
            Ok(Self { fd, peer })
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
            crate::sctp_io::recv_record(&self.fd).await
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
                    "SCTP peer without socket address",
                )
            })?;
        Ok((owned, peer))
    }

    fn send_raw(fd: i32, payload: &[u8]) -> io::Result<()> {
        let written = unsafe {
            libc::send(
                fd,
                payload.as_ptr().cast::<c_void>(),
                payload.len(),
                libc::MSG_NOSIGNAL,
            )
        };
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
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
mod platform {
    use std::{io, net::SocketAddr};

    #[derive(Clone, Debug)]
    pub(crate) struct SctpListener;

    impl SctpListener {
        pub(super) fn bind(_address: SocketAddr) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "SCTP transport is supported only on Linux and Android",
            ))
        }

        pub(super) fn local_addr(&self) -> SocketAddr {
            SocketAddr::from(([0, 0, 0, 0], 0))
        }

        pub(super) async fn accept(&self) -> io::Result<SctpStream> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "SCTP transport is supported only on Linux and Android",
            ))
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct SctpStream;

    impl SctpStream {
        pub(super) fn peer_addr(&self) -> SocketAddr {
            SocketAddr::from(([0, 0, 0, 0], 0))
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
