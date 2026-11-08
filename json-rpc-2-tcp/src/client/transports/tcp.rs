//! JSON lines and self-delimited application-defined binary frames over persistent TCP.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, MutexGuard, Notify},
};

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::TcpTuningConfig;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientIncomingFuture, ClientPingFuture,
    ClientRecoverFuture, ClientTransportFuture, IncomingQueue, RpcClientTransport, StreamFraming,
    TransportError, TransportMessage, expects_response, is_incoming_message,
};
use json_rpc_2_transport::{closed, transport_error};

type Connection = (TcpStream, BytesMut);

pub(crate) enum ConnectionState<C> {
    Ready(C),
    Invalidated,
    Closed,
}

/// A stream plus the read buffer that owns any partially received frame.
pub(crate) trait FramedConnection: Send {
    type Stream: AsyncRead + Unpin + Send;

    fn parts(&mut self) -> (&mut Self::Stream, &mut BytesMut);
}

impl<S: AsyncRead + Unpin + Send> FramedConnection for (S, BytesMut) {
    type Stream = S;

    fn parts(&mut self) -> (&mut S, &mut BytesMut) {
        (&mut self.0, &mut self.1)
    }
}

impl<C: FramedConnection> FramedConnection for Box<C> {
    type Stream = C::Stream;

    fn parts(&mut self) -> (&mut Self::Stream, &mut BytesMut) {
        (**self).parts()
    }
}

/// Server-initiated messages of a lock-step JSON-lines stream transport.
///
/// The connection mutex keeps exchanges strictly request/response. While no
/// exchange runs, [`StreamIncoming::recv`] owns the connection and reads push
/// lines; every other user signals contention before locking so the reader
/// yields immediately. Push lines that arrive while an exchange waits for its
/// response are diverted into the queue instead of being taken as the reply.
#[derive(Default)]
pub(crate) struct StreamIncoming {
    pub(crate) queue: IncomingQueue,
    contention: Notify,
}

impl StreamIncoming {
    /// Lock the connection, asking an idle incoming reader to yield first.
    pub(crate) async fn lock<'a, C>(
        &self,
        connection: &'a Mutex<ConnectionState<C>>,
    ) -> MutexGuard<'a, ConnectionState<C>> {
        self.contention.notify_one();
        connection.lock().await
    }

    /// Read the reply to the exchange in flight, queueing push lines.
    pub(crate) async fn read_response<C: FramedConnection>(
        &self,
        framed: &mut C,
        framing: Option<&dyn StreamFraming>,
    ) -> std::io::Result<Option<Bytes>> {
        let (stream, buffer) = framed.parts();
        loop {
            match read_frame(stream, buffer, framing).await? {
                Some(frame) if framing.is_none() && is_incoming_message(&frame) => {
                    self.queue.push(frame);
                }
                other => return Ok(other),
            }
        }
    }

    /// Wait for the next push line. Cancel-safe: the partial frame stays in
    /// the connection buffer. A broken connection is invalidated and the wait
    /// continues until an exchange or reconnect opens a new one.
    pub(crate) async fn recv<C: FramedConnection>(
        &self,
        connection: &Mutex<ConnectionState<C>>,
        framing: Option<&dyn StreamFraming>,
    ) -> Result<Bytes, TransportError> {
        if framing.is_some() {
            return Err(TransportError::new(
                "server-initiated messages require JSON-lines framing",
            ));
        }
        loop {
            let changed = self.queue.changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(message) = self.queue.try_pop() {
                return Ok(message);
            }
            let mut guard = tokio::select! {
                guard = connection.lock() => guard,
                _ = &mut changed => continue,
            };
            let frame = match &mut *guard {
                ConnectionState::Closed => return Err(closed()),
                ConnectionState::Invalidated => {
                    drop(guard);
                    changed.await;
                    continue;
                }
                ConnectionState::Ready(framed) => {
                    let (stream, buffer) = framed.parts();
                    tokio::select! {
                        biased;
                        _ = self.contention.notified() => continue,
                        frame = read_frame(stream, buffer, None) => frame,
                    }
                }
            };
            match frame {
                Ok(Some(frame)) if is_incoming_message(&frame) => return Ok(frame),
                // No exchange is in flight, so this cannot answer a request.
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => *guard = ConnectionState::Invalidated,
            }
        }
    }
}

/// TCP client transport supporting JSON lines and application-defined binary frames.
///
/// An interrupted exchange discards its socket, including any partially written
/// frame or outstanding reply. The next exchange opens a new connection; the
/// interrupted request is never replayed (it may already have executed).
pub struct TcpClientTransport {
    address: SocketAddr,
    tuning: TcpTuningConfig,
    framing: Option<std::sync::Arc<dyn json_rpc_2_transport::StreamFraming>>,
    connection: Mutex<ConnectionState<Connection>>,
    incoming: StreamIncoming,
}

impl TcpClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_tuning(address, TcpTuningConfig::default()).await
    }

    pub async fn connect_with_tuning(
        address: SocketAddr,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            address,
            connection: Mutex::new(ConnectionState::Ready(Self::open(address, &tuning).await?)),
            tuning,
            framing: None,
            incoming: StreamIncoming::default(),
        })
    }

    pub fn with_stream_framing(
        mut self,
        framing: std::sync::Arc<dyn json_rpc_2_transport::StreamFraming>,
    ) -> Self {
        self.framing = Some(framing);
        self
    }

    async fn open(
        address: SocketAddr,
        tuning: &TcpTuningConfig,
    ) -> Result<Connection, TransportError> {
        let stream = json_rpc_2_transport::connect_tcp_stream(address, tuning).await?;
        Ok((stream, BytesMut::new()))
    }
}

impl RpcClientTransport for TcpClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects = expects_response(&message);
            self.send_line(message, expects).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move { self.send_line(message, expects_response).await })
    }

    fn send_message<'a>(
        &'a self,
        message: TransportMessage,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move { self.exchange(message, expects_response).await })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            match self
                .send_line(Bytes::from_static(CONTROL_PING), true)
                .await?
            {
                Some(response) if response.as_ref() == CONTROL_PONG => Ok(()),
                Some(response) => Err(TransportError::new(format!(
                    "unexpected TCP ping response: {}",
                    String::from_utf8_lossy(&response),
                ))),
                None => Err(TransportError::new("TCP connection closed by server")),
            }
        })
    }

    fn kind(&self) -> &'static str {
        "tcp"
    }

    fn supports_incoming(&self) -> bool {
        self.framing.is_none()
    }

    fn recv_incoming<'a>(&'a self) -> ClientIncomingFuture<'a> {
        Box::pin(
            self.incoming
                .recv(&self.connection, self.framing.as_deref()),
        )
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let mut connection = self.incoming.lock(&self.connection).await;
            let replacement = Self::open(self.address, &self.tuning).await?;
            // There are no in-flight exchanges while this lock is held. Drop
            // the old socket synchronously so cancellation cannot strand it.
            *connection = ConnectionState::Ready(replacement);
            self.incoming.queue.wake();
            Ok(())
        })
    }

    /// Send FIN and wait until the server closes its side too.
    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let mut connection = self.incoming.lock(&self.connection).await;
            let previous = std::mem::replace(&mut *connection, ConnectionState::Closed);
            self.incoming.queue.close();
            let ConnectionState::Ready(framed) = previous else {
                return Ok(());
            };
            let (mut stream, _) = framed;
            stream.shutdown().await.map_err(transport_error)?;
            stream
                .read_to_end(&mut Vec::new())
                .await
                .map_err(transport_error)?;
            Ok(())
        })
    }
}

impl TcpClientTransport {
    async fn send_line(
        &self,
        message: Bytes,
        expects_response: bool,
    ) -> Result<Option<Bytes>, TransportError> {
        self.exchange(message.into(), expects_response).await
    }

    async fn exchange(
        &self,
        message: TransportMessage,
        expects_response: bool,
    ) -> Result<Option<Bytes>, TransportError> {
        validate_message(&message, self.framing.as_deref()).map_err(transport_error)?;
        let mut connection = self.incoming.lock(&self.connection).await;
        if matches!(*connection, ConnectionState::Closed) {
            return Err(closed());
        }
        // Exclusive ownership is the cancellation guard: dropping this future
        // drops the socket before releasing the mutex, leaving Invalidated for
        // queued callers. A cancellation while waiting for the mutex changes
        // nothing. There is no cleanup docs and no old generation to restore.
        let mut framed = match std::mem::replace(&mut *connection, ConnectionState::Invalidated) {
            ConnectionState::Ready(framed) => framed,
            ConnectionState::Invalidated => Self::open(self.address, &self.tuning).await?,
            ConnectionState::Closed => unreachable!(),
        };
        write_message(&mut framed.0, &message, self.framing.as_deref())
            .await
            .map_err(transport_error)?;
        let response = if expects_response {
            match self
                .incoming
                .read_response(&mut framed, self.framing.as_deref())
                .await
                .map_err(transport_error)?
            {
                Some(response) => Some(response),
                None => return Err(TransportError::new("TCP connection closed by server")),
            }
        } else {
            None
        };
        *connection = ConnectionState::Ready(framed);
        self.incoming.queue.wake();
        Ok(response)
    }
}

pub fn validate_message(
    message: &TransportMessage,
    framing: Option<&dyn json_rpc_2_transport::StreamFraming>,
) -> std::io::Result<bool> {
    use json_rpc_2_transport::FrameBoundary;
    if message.len() > MAX_MESSAGE_BYTES {
        return Err(invalid_frame("message exceeds maximum size"));
    }
    // Normally the small header is sufficient. Coalescing is needed only when
    // a custom codec's header spans both segments.
    let combined;
    let first = if message.header.is_empty() {
        &message.payload
    } else {
        &message.header
    };
    let boundary = match framing {
        Some(codec) => {
            let boundary = codec.boundary(first)?;
            if matches!(boundary, FrameBoundary::Need(_)) && !message.header.is_empty() {
                combined = message.clone().into_bytes();
                codec.boundary(&combined)?
            } else {
                boundary
            }
        }
        None => FrameBoundary::Line,
    };
    match boundary {
        FrameBoundary::Frame(length) if length > 0 && length == message.len() => Ok(true),
        FrameBoundary::Frame(_) | FrameBoundary::Need(_) => {
            Err(invalid_frame("invalid or incomplete custom frame"))
        }
        FrameBoundary::Line => {
            if message.binary {
                return Err(invalid_frame(
                    "binary stream message requires a matching frame codec",
                ));
            }
            std::str::from_utf8(&message.header).map_err(invalid_frame)?;
            std::str::from_utf8(&message.payload).map_err(invalid_frame)?;
            if message.header.contains(&b'\n') || message.payload.contains(&b'\n') {
                return Err(invalid_frame("line payload contains a newline"));
            }
            Ok(false)
        }
    }
}

fn invalid_frame(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}

/// The caller owns the buffer so cancellation never discards a partial frame.
pub async fn read_frame<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    framing: Option<&dyn json_rpc_2_transport::StreamFraming>,
) -> std::io::Result<Option<Bytes>>
where
    R: AsyncRead + Unpin,
{
    use json_rpc_2_transport::FrameBoundary;
    loop {
        let boundary = match framing {
            Some(codec) => codec.boundary(buffer)?,
            None => FrameBoundary::Line,
        };
        let read_limit = match boundary {
            FrameBoundary::Need(length) | FrameBoundary::Frame(length)
                if length == 0 || length > MAX_MESSAGE_BYTES =>
            {
                return Err(invalid_frame(
                    "custom frame exceeds maximum size or is empty",
                ));
            }
            FrameBoundary::Need(length) => {
                if length <= buffer.len() {
                    return Err(invalid_frame("codec requested no additional bytes"));
                }
                length - buffer.len()
            }
            FrameBoundary::Frame(length) => {
                if buffer.len() >= length {
                    return Ok(Some(buffer.split_to(length).freeze()));
                }
                length - buffer.len()
            }
            FrameBoundary::Line => {
                if let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
                    let length = end - usize::from(end > 0 && buffer[end - 1] == b'\r');
                    if length > MAX_MESSAGE_BYTES {
                        return Err(invalid_frame("line exceeds maximum message size"));
                    }
                    let mut line = buffer.split_to(end + 1).freeze();
                    line.truncate(length);
                    std::str::from_utf8(&line).map_err(invalid_frame)?;
                    return Ok(Some(line));
                }
                if buffer.len() > MAX_MESSAGE_BYTES + 1 {
                    return Err(invalid_frame("line exceeds maximum message size"));
                }
                MAX_MESSAGE_BYTES + 2 - buffer.len()
            }
        };
        let count = (&mut *reader)
            .take(read_limit.min(8192) as u64)
            .read_buf(buffer)
            .await?;
        if count == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Err(invalid_frame("connection closed in an incomplete frame"));
        }
    }
}

/// Write both regions without coalescing, advancing correctly after short writes.
pub async fn write_message<W>(
    writer: &mut W,
    message: &TransportMessage,
    framing: Option<&dyn json_rpc_2_transport::StreamFraming>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let binary = validate_message(message, framing)?;
    let mut header = message.header.as_ref();
    let mut payload = message.payload.as_ref();
    while !header.is_empty() || !payload.is_empty() {
        let slices = [
            std::io::IoSlice::new(header),
            std::io::IoSlice::new(payload),
        ];
        let count = writer.write_vectored(&slices).await?;
        if count == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        if count < header.len() {
            header = &header[count..];
        } else {
            let payload_count = count - header.len();
            header = &[];
            payload = &payload[payload_count..];
        }
    }
    if !binary {
        writer.write_all(b"\n").await?;
    }
    writer.flush().await
}

// Explicit path: this file is also included from tls.rs in TLS-only builds.
#[cfg(all(test, feature = "tcp"))]
#[path = "tcp/tests.rs"]
mod tests;
