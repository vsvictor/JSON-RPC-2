//! JSON lines and self-delimited binary gateway frames over persistent TCP.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::TcpTuningConfig;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, RpcClientTransport, TransportError, TransportMessage, expects_response,
};
use json_rpc_2_transport::{closed, transport_error};

type Connection = (TcpStream, BytesMut);

enum ConnectionState {
    Ready(Connection),
    Invalidated,
    Closed,
}

/// TCP client transport supporting JSON lines and binary gateway frames.
///
/// An interrupted exchange discards its socket, including any partially written
/// frame or outstanding reply. The next exchange opens a new connection; the
/// interrupted request is never replayed (it may already have executed).
pub struct TcpClientTransport {
    address: SocketAddr,
    tuning: TcpTuningConfig,
    connection: Mutex<ConnectionState>,
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
        })
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

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            let replacement = Self::open(self.address, &self.tuning).await?;
            // There are no in-flight exchanges while this lock is held. Drop
            // the old socket synchronously so cancellation cannot strand it.
            *connection = ConnectionState::Ready(replacement);
            Ok(())
        })
    }

    /// Send FIN and wait until the server closes its side too.
    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            let ConnectionState::Ready(framed) =
                std::mem::replace(&mut *connection, ConnectionState::Closed)
            else {
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
        validate_message(&message).map_err(transport_error)?;
        let mut connection = self.connection.lock().await;
        if matches!(*connection, ConnectionState::Closed) {
            return Err(closed());
        }
        // Exclusive ownership is the cancellation guard: dropping this future
        // drops the socket before releasing the mutex, leaving Invalidated for
        // queued callers. A cancellation while waiting for the mutex changes
        // nothing. There is no cleanup task and no old generation to restore.
        let mut framed = match std::mem::replace(&mut *connection, ConnectionState::Invalidated) {
            ConnectionState::Ready(framed) => framed,
            ConnectionState::Invalidated => Self::open(self.address, &self.tuning).await?,
            ConnectionState::Closed => unreachable!(),
        };
        write_message(&mut framed.0, &message)
            .await
            .map_err(transport_error)?;
        let response = if expects_response {
            match read_frame(&mut framed.0, &mut framed.1)
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
        Ok(response)
    }
}

pub(crate) fn validate_message(message: &TransportMessage) -> std::io::Result<bool> {
    use json_rpc_2_transport::gateway;
    let first = if message.header.is_empty() {
        &message.payload
    } else {
        &message.header
    };
    let binary = gateway::is_gateway_frame(first);
    if message.len() > MAX_MESSAGE_BYTES {
        return Err(invalid_frame("message exceeds maximum size"));
    }
    if binary {
        let length = gateway::frame_length(first).map_err(invalid_frame)?;
        if length != message.len() {
            return Err(invalid_frame("gateway frame length does not match message"));
        }
    } else {
        std::str::from_utf8(&message.header).map_err(invalid_frame)?;
        std::str::from_utf8(&message.payload).map_err(invalid_frame)?;
    }
    Ok(binary)
}

fn invalid_frame(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}

/// The caller owns the buffer so cancellation never discards a partial frame.
pub(crate) async fn read_frame<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
) -> std::io::Result<Option<Bytes>>
where
    R: AsyncRead + Unpin,
{
    use json_rpc_2_transport::gateway;
    loop {
        let prefix_length = buffer.len().min(gateway::MAGIC.len());
        let possible_binary = buffer[..prefix_length] == gateway::MAGIC[..prefix_length];
        let read_limit = if possible_binary {
            if buffer.len() < gateway::HEADER_LEN {
                gateway::HEADER_LEN - buffer.len()
            } else {
                let length =
                    gateway::frame_length(&buffer[..gateway::HEADER_LEN]).map_err(invalid_frame)?;
                if length > MAX_MESSAGE_BYTES {
                    return Err(invalid_frame("gateway frame exceeds maximum size"));
                }
                if buffer.len() >= length {
                    return Ok(Some(buffer.split_to(length).freeze()));
                }
                length - buffer.len()
            }
        } else {
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
pub(crate) async fn write_message<W>(
    writer: &mut W,
    message: &TransportMessage,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let binary = validate_message(message)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use json_rpc_2_transport::gateway::{GATEWAY_PING, GATEWAY_PONG};
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    #[tokio::test]
    async fn coalesced_binary_and_line_reads_preserve_buffer_pointer() {
        let mut buffer = BytesMut::with_capacity(256);
        buffer.extend_from_slice(GATEWAY_PING);
        buffer.extend_from_slice(GATEWAY_PONG);
        buffer.extend_from_slice(b"pong\r\n");
        let pointer = buffer.as_ptr();
        let mut reader = tokio::io::empty();
        let ping = read_frame(&mut reader, &mut buffer).await.unwrap().unwrap();
        assert_eq!(ping.as_ptr(), pointer);
        assert_eq!(ping, GATEWAY_PING);
        assert_eq!(
            read_frame(&mut reader, &mut buffer).await.unwrap().unwrap(),
            GATEWAY_PONG
        );
        assert_eq!(
            read_frame(&mut reader, &mut buffer).await.unwrap().unwrap(),
            b"pong".as_slice()
        );
        assert!(
            read_frame(&mut reader, &mut buffer)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn partial_binary_read_survives_cancellation() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut buffer = BytesMut::new();
        for byte in &GATEWAY_PING[..GATEWAY_PING.len() - 1] {
            writer.write_all(&[*byte]).await.unwrap();
            tokio::select! {
                result = read_frame(&mut reader, &mut buffer) => {
                    panic!("incomplete frame was accepted: {result:?}");
                }
                _ = tokio::task::yield_now() => {}
            }
        }
        writer
            .write_all(&GATEWAY_PING[GATEWAY_PING.len() - 1..])
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut reader, &mut buffer).await.unwrap().unwrap(),
            GATEWAY_PING
        );
    }

    #[tokio::test]
    async fn opaque_payload_is_not_treated_as_lines_or_utf8() {
        let payload = Bytes::from_static(b"\xfffirst\nsecond\r\n\0\xfe");
        let message =
            json_rpc_2_transport::gateway::encode_request(&payload, "client-1", "tcp").unwrap();
        assert_eq!(message.payload.as_ptr(), payload.as_ptr());
        let mut writer = ShortWriter(Vec::new());
        write_message(&mut writer, &message).await.unwrap();
        assert_eq!(writer.0.len(), message.len());
        assert_eq!(&writer.0[message.header.len()..], payload.as_ref());
        let mut reader = writer.0.as_slice();
        let mut buffer = BytesMut::new();
        let response = read_frame(&mut reader, &mut buffer).await.unwrap().unwrap();
        assert_eq!(&response[message.header.len()..], payload.as_ref());
        assert!(
            read_frame(&mut reader, &mut buffer)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn oversized_header_is_rejected_before_payload_reads() {
        let payload = Bytes::from_static(b"test");
        let message =
            json_rpc_2_transport::gateway::encode_request(&payload, "client-1", "tcp").unwrap();
        let mut buffer = BytesMut::from(message.header.as_ref());
        buffer[13..17].copy_from_slice(&u32::MAX.to_be_bytes());
        let length = buffer.len();
        let error = read_frame(&mut tokio::io::empty(), &mut buffer)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(buffer.len(), length);
    }

    struct ShortWriter(Vec<u8>);

    impl AsyncWrite for ShortWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            input: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let count = input.len().min(3);
            self.0.extend_from_slice(&input[..count]);
            Poll::Ready(Ok(count))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            input: &[std::io::IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let mut count = 0;
            for slice in input {
                let length = slice.len().min(3 - count);
                self.0.extend_from_slice(&slice[..length]);
                count += length;
                if count == 3 {
                    break;
                }
            }
            Poll::Ready(Ok(count))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn short_vectored_writes_do_not_append_binary_newlines() {
        let mut writer = ShortWriter(Vec::new());
        write_message(&mut writer, &Bytes::from_static(GATEWAY_PING).into())
            .await
            .unwrap();
        assert_eq!(writer.0, GATEWAY_PING);
        write_message(&mut writer, &Bytes::from_static(b"{}").into())
            .await
            .unwrap();
        assert_eq!(&writer.0[GATEWAY_PING.len()..], b"{}\n");
    }
}
