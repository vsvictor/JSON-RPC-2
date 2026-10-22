//! Safe ownership boundary around the maintained libsrt C bindings.
//!
//! SRT live messages carry ordered fragments of one application frame. This is
//! application framing, not a replacement transport handshake or crypto profile.

use std::{
    ffi::CStr,
    io,
    net::SocketAddr,
    sync::{Mutex, OnceLock},
    time::Duration,
};

use bytes::Bytes;
use json_rpc_2_core::{MAX_MESSAGE_BYTES, SrtConfig};
use libsrt_sys as sys;
use socket2::SockAddr;
use tokio::sync::Mutex as AsyncMutex;

const POLL: Duration = Duration::from_millis(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const PAYLOAD_SIZE: usize = 1316;
const HEADER_SIZE: usize = 18;
const MAGIC: &[u8; 4] = b"JRPC";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    Request = 1,
    Response = 2,
    Ack = 3,
    Ping = 4,
    Pong = 5,
    Drain = 6,
    Drained = 7,
    Push = 8,
}

pub(crate) struct Frame {
    pub kind: Kind,
    pub id: u64,
    pub payload: Bytes,
}

pub(crate) struct NativeSocket {
    // Every native call holds this lock. Closing cannot race a syscall using a
    // stale/recycled integer handle, including when an async operation is dropped.
    handle: Mutex<Option<sys::SRTSOCKET>>,
    writer: AsyncMutex<()>,
    listener_poll: Option<i32>,
}

fn last_error(operation: &str) -> io::Error {
    // SAFETY: libsrt returns a thread-local, NUL-terminated error description.
    let message = unsafe { CStr::from_ptr(sys::srt_getlasterror_str()) }.to_string_lossy();
    io::Error::other(format!("libsrt {operation}: {message}"))
}

fn would_block() -> bool {
    // SAFETY: a null optional errno output is allowed; read immediately on the calling thread.
    let code = unsafe { sys::srt_getlasterror(std::ptr::null_mut()) };
    code == sys::SRT_ERRNO_SRT_EASYNCRCV || code == sys::SRT_ERRNO_SRT_EASYNCSND
}

pub(crate) fn transport_error(error: io::Error) -> json_rpc_2_transport::TransportError {
    json_rpc_2_transport::TransportError::new(error.to_string())
}

impl NativeSocket {
    fn new(config: &SrtConfig) -> io::Result<Self> {
        config
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        static STARTUP: OnceLock<Result<(), String>> = OnceLock::new();
        STARTUP
            .get_or_init(|| {
                // SAFETY: called once process-wide; no cleanup while other sockets may exist.
                if unsafe { sys::srt_startup() } == 0 {
                    Ok(())
                } else {
                    Err(last_error("startup").to_string())
                }
            })
            .as_ref()
            .map_err(|error| io::Error::other(error.clone()))?;
        // SAFETY: libsrt initialized above; ownership transfers to the RAII object.
        let handle = unsafe { sys::srt_create_socket() };
        if handle == sys::SRT_INVALID_SOCK {
            return Err(last_error("create"));
        }
        let socket = Self {
            handle: Mutex::new(Some(handle)),
            writer: AsyncMutex::new(()),
            listener_poll: None,
        };
        socket.option(sys::SRT_SOCKOPT_SRTO_SNDSYN, &false)?;
        socket.option(sys::SRT_SOCKOPT_SRTO_RCVSYN, &false)?;
        socket.option(sys::SRT_SOCKOPT_SRTO_TLPKTDROP, &false)?;
        socket.option(sys::SRT_SOCKOPT_SRTO_CONNTIMEO, &3000i32)?;
        socket.option(sys::SRT_SOCKOPT_SRTO_PEERIDLETIMEO, &5000i32)?;
        socket.option(sys::SRT_SOCKOPT_SRTO_PBKEYLEN, &32i32)?;
        socket.option(sys::SRT_SOCKOPT_SRTO_ENFORCEDENCRYPTION, &true)?;
        socket.with_handle(|handle| {
            // SAFETY: passphrase bytes remain valid for the call; libsrt copies them.
            if unsafe {
                sys::srt_setsockflag(
                    handle,
                    sys::SRT_SOCKOPT_SRTO_PASSPHRASE,
                    config.passphrase.as_ptr().cast(),
                    config.passphrase.len() as i32,
                )
            } < 0
            {
                Err(last_error("set passphrase"))
            } else {
                Ok(())
            }
        })?;
        Ok(socket)
    }

    fn with_handle<T>(&self, call: impl FnOnce(sys::SRTSOCKET) -> io::Result<T>) -> io::Result<T> {
        let guard = self.handle.lock().unwrap();
        call(
            guard
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "SRT socket closed"))?,
        )
    }

    fn option<T>(&self, option: sys::SRT_SOCKOPT, value: &T) -> io::Result<()> {
        self.with_handle(|handle| {
            // SAFETY: all callers use the native option's documented scalar representation.
            if unsafe {
                sys::srt_setsockflag(
                    handle,
                    option,
                    (value as *const T).cast(),
                    std::mem::size_of::<T>() as i32,
                )
            } < 0
            {
                Err(last_error("setsockflag"))
            } else {
                Ok(())
            }
        })
    }

    pub fn bind(address: SocketAddr, config: &SrtConfig) -> io::Result<Self> {
        let mut socket = Self::new(config)?;
        if address.is_ipv6() {
            // Runtime binds distinct IPv4/IPv6 listeners on the same port.
            socket.option(sys::SRT_SOCKOPT_SRTO_IPV6ONLY, &1i32)?;
        }
        let address = SockAddr::from(address);
        // Register before listen so a completed handshake cannot precede the
        // epoll subscription (libsrt subscriptions do not snapshot old readiness).
        // SAFETY: libsrt was initialized by new().
        let poll = unsafe { sys::srt_epoll_create() };
        if poll < 0 {
            return Err(last_error("epoll_create"));
        }
        socket.listener_poll = Some(poll);
        socket.with_handle(|handle| {
            // SAFETY: SockAddr provides a valid sockaddr and its exact initialized length.
            if unsafe { sys::srt_bind(handle, address.as_ptr().cast(), address.len() as i32) } < 0 {
                return Err(last_error("bind"));
            }
            let events =
                (sys::SRT_EPOLL_OPT_SRT_EPOLL_IN | sys::SRT_EPOLL_OPT_SRT_EPOLL_ERR) as i32;
            // SAFETY: poll and socket are owned; event storage lives through the call.
            if unsafe { sys::srt_epoll_add_usock(poll, handle, &events) } < 0 {
                return Err(last_error("epoll_add_usock"));
            }
            // SAFETY: the owned socket is bound and not closed while this lock is held.
            if unsafe { sys::srt_listen(handle, 128) } < 0 {
                return Err(last_error("listen"));
            }
            Ok(())
        })?;
        Ok(socket)
    }

    pub async fn connect(
        address: SocketAddr,
        config: &SrtConfig,
        stream_id: &str,
    ) -> io::Result<Self> {
        if stream_id.len() > 512 || stream_id.as_bytes().contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SRT stream ID must be at most 512 bytes and contain no NUL",
            ));
        }
        let socket = Self::new(config)?;
        socket.with_handle(|handle| {
            // SAFETY: libsrt copies the initialized string bytes before connect.
            if unsafe {
                sys::srt_setsockflag(
                    handle,
                    sys::SRT_SOCKOPT_SRTO_STREAMID,
                    stream_id.as_ptr().cast(),
                    stream_id.len() as i32,
                )
            } < 0
            {
                Err(last_error("set stream ID"))
            } else {
                Ok(())
            }
        })?;
        let address = SockAddr::from(address);
        socket.with_handle(|handle| {
            // SAFETY: valid sockaddr, socket exclusively guarded; nonblocking connect.
            if unsafe { sys::srt_connect(handle, address.as_ptr().cast(), address.len() as i32) }
                < 0
                && !would_block()
            {
                return Err(last_error("connect"));
            }
            Ok(())
        })?;
        tokio::time::timeout(CONNECT_TIMEOUT, async {
            loop {
                let state = socket.with_handle(|handle| {
                    // SAFETY: handle is owned and guarded against concurrent close.
                    Ok(unsafe { sys::srt_getsockstate(handle) })
                })?;
                match state {
                    sys::SRT_SOCKSTATUS_SRTS_CONNECTED => return Ok(()),
                    sys::SRT_SOCKSTATUS_SRTS_CONNECTING | sys::SRT_SOCKSTATUS_SRTS_OPENED => {}
                    _ => return Err(io::Error::new(io::ErrorKind::ConnectionRefused,
                        format!("libsrt handshake failed (socket state {state}); check peer and passphrase"))),
                }
                tokio::time::sleep(POLL).await;
            }
        }).await.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "libsrt handshake timed out"))??;
        Ok(socket)
    }

    pub fn stream_id(&self) -> io::Result<String> {
        self.with_handle(|handle| {
            let mut buffer = [0u8; 513];
            let mut length = buffer.len() as i32;
            // SAFETY: buffer/length describe initialized writable storage;
            // the socket handle is guarded against concurrent close.
            if unsafe {
                sys::srt_getsockflag(
                    handle,
                    sys::SRT_SOCKOPT_SRTO_STREAMID,
                    buffer.as_mut_ptr().cast(),
                    &mut length,
                )
            } < 0
            {
                return Err(last_error("get stream ID"));
            }
            if !(0..=512).contains(&length) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid SRT stream ID length",
                ));
            }
            String::from_utf8(buffer[..length as usize].to_vec())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        })
    }

    pub async fn accept(&self) -> io::Result<Self> {
        let poll = self
            .listener_poll
            .ok_or_else(|| io::Error::other("SRT socket is not a listener"))?;
        loop {
            let accepted = self.with_handle(|handle| {
                let mut event = sys::SRT_EPOLL_EVENT::default();
                // SAFETY: one writable event slot; zero timeout never blocks Tokio.
                let ready = unsafe { sys::srt_epoll_uwait(poll, &mut event, 1, 0) };
                if ready == 0 {
                    return Ok(None);
                }
                if ready < 0 {
                    return Err(last_error("epoll_uwait"));
                }
                // SAFETY: address outputs are optional; accept is nonblocking.
                let accepted =
                    unsafe { sys::srt_accept(handle, std::ptr::null_mut(), std::ptr::null_mut()) };
                if accepted != sys::SRT_INVALID_SOCK {
                    Ok(Some(accepted))
                } else if would_block() {
                    Ok(None)
                } else {
                    Err(last_error("accept"))
                }
            })?;
            if let Some(handle) = accepted {
                let socket = Self {
                    handle: Mutex::new(Some(handle)),
                    writer: AsyncMutex::new(()),
                    listener_poll: None,
                };
                socket.option(sys::SRT_SOCKOPT_SRTO_RCVSYN, &false)?;
                socket.option(sys::SRT_SOCKOPT_SRTO_SNDSYN, &false)?;
                return Ok(socket);
            }
            tokio::time::sleep(POLL).await;
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.with_handle(|handle| {
            // SAFETY: try_init supplies initialized address capacity and length;
            // libsrt fills a sockaddr and updates its length on success.
            let (_, address) = unsafe {
                SockAddr::try_init(|storage, length| {
                    if sys::srt_getsockname(handle, storage.cast(), length.cast()) < 0 {
                        Err(last_error("getsockname"))
                    } else {
                        Ok(())
                    }
                })
            }?;
            address
                .as_socket()
                .ok_or_else(|| io::Error::other("libsrt returned a non-IP address"))
        })
    }

    pub fn close(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            // SAFETY: handle is consumed once, with no outstanding native syscall.
            unsafe {
                sys::srt_close(handle);
            }
        }
    }

    async fn send_message(&self, data: &[u8]) -> io::Result<()> {
        loop {
            let sent = self.with_handle(|handle| {
                // SAFETY: data is initialized and <= 1316 bytes; libsrt copies it.
                let result = unsafe {
                    sys::srt_sendmsg(handle, data.as_ptr().cast(), data.len() as i32, -1, 1)
                };
                if result >= 0 {
                    if result as usize != data.len() {
                        return Err(io::Error::other("libsrt short message send"));
                    }
                    Ok(true)
                } else if would_block() {
                    Ok(false)
                } else {
                    Err(last_error("sendmsg"))
                }
            })?;
            if sent {
                return Ok(());
            }
            tokio::time::sleep(POLL).await;
        }
    }

    pub async fn send(&self, frame: Frame) -> io::Result<()> {
        if frame.payload.len() > MAX_MESSAGE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SRT message exceeds MAX_MESSAGE_BYTES",
            ));
        }
        // A dropped partial write would corrupt the next frame. The guard closes
        // this generation on cancellation/timeout until every fragment is queued.
        struct WriteGuard<'a>(&'a NativeSocket, bool);
        impl Drop for WriteGuard<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.close();
                }
            }
        }
        let _writer = self.writer.lock().await;
        let mut guard = WriteGuard(self, false);
        tokio::time::timeout(IO_TIMEOUT, async {
            let mut header = Vec::with_capacity(HEADER_SIZE);
            header.extend_from_slice(MAGIC);
            header.push(1);
            header.push(frame.kind as u8);
            header.extend_from_slice(&frame.id.to_be_bytes());
            header.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
            self.send_message(&header).await?;
            for chunk in frame.payload.chunks(PAYLOAD_SIZE) {
                self.send_message(chunk).await?;
            }
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "libsrt send timed out"))??;
        guard.1 = true;
        Ok(())
    }

    async fn recv_message(&self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            let received = self.with_handle(|handle| {
                // SAFETY: buffer is writable for its full length and retained during the call.
                let result = unsafe {
                    sys::srt_recvmsg(handle, buffer.as_mut_ptr().cast(), buffer.len() as i32)
                };
                if result > 0 {
                    Ok(Some(result as usize))
                } else if result == 0 {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "SRT peer closed",
                    ))
                } else if would_block() {
                    Ok(None)
                } else {
                    Err(last_error("recvmsg"))
                }
            })?;
            if let Some(size) = received {
                return Ok(size);
            }
            tokio::time::sleep(POLL).await;
        }
    }

    // Exactly one receiver is permitted per socket, as with an owned stream read half.
    pub async fn recv(&self) -> io::Result<Frame> {
        let mut buffer = [0u8; PAYLOAD_SIZE];
        let size = self.recv_message(&mut buffer).await?;
        if size != HEADER_SIZE || &buffer[..4] != MAGIC || buffer[4] != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid SRT RPC frame header",
            ));
        }
        let kind = match buffer[5] {
            1 => Kind::Request,
            2 => Kind::Response,
            3 => Kind::Ack,
            4 => Kind::Ping,
            5 => Kind::Pong,
            6 => Kind::Drain,
            7 => Kind::Drained,
            8 => Kind::Push,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid SRT RPC frame kind",
                ));
            }
        };
        let id = u64::from_be_bytes(buffer[6..14].try_into().unwrap());
        let length = u32::from_be_bytes(buffer[14..18].try_into().unwrap()) as usize;
        if length > MAX_MESSAGE_BYTES
            || (matches!(
                kind,
                Kind::Ack | Kind::Ping | Kind::Pong | Kind::Drain | Kind::Drained
            ) && length != 0)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid SRT RPC payload length",
            ));
        }
        let payload = tokio::time::timeout(IO_TIMEOUT, async {
            let mut payload = Vec::with_capacity(length);
            while payload.len() < length {
                let size = self.recv_message(&mut buffer).await?;
                if size > length - payload.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "SRT RPC fragment exceeds declared length",
                    ));
                }
                payload.extend_from_slice(&buffer[..size]);
            }
            Ok::<_, io::Error>(Bytes::from(payload))
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "SRT RPC frame timed out"))??;
        Ok(Frame { kind, id, payload })
    }
}

impl Drop for NativeSocket {
    fn drop(&mut self) {
        self.close();
        if let Some(poll) = self.listener_poll.take() {
            // SAFETY: this owned epoll ID is no longer used by any socket call.
            unsafe {
                sys::srt_epoll_release(poll);
            }
        }
    }
}
