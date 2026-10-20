//! Newline-delimited JSON over one persistent TCP connection.

use std::net::SocketAddr;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tokio_util::codec::{Framed, LinesCodec};

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::TcpTuningConfig;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, RpcClientTransport, TransportError, expects_response,
};
use json_rpc_2_transport::{closed, transport_error};

type Connection = Framed<TcpStream, LinesCodec>;

enum ConnectionState {
    Ready(Connection),
    Invalidated,
    Closed,
}

/// TCP client transport, one compact JSON message per line.
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
    ) -> Result<Framed<TcpStream, LinesCodec>, TransportError> {
        let stream = json_rpc_2_transport::connect_tcp_stream(address, tuning).await?;
        Ok(Framed::new(
            stream,
            LinesCodec::new_with_max_length(MAX_MESSAGE_BYTES),
        ))
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
            let mut stream = framed.into_inner();
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
        let message = std::str::from_utf8(&message)
            .map(str::to_owned)
            .map_err(transport_error)?;
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
        framed.send(message).await.map_err(transport_error)?;
        let response = if expects_response {
            match framed.next().await {
                Some(Ok(response)) => Some(Bytes::from(response)),
                Some(Err(error)) => return Err(transport_error(error)),
                None => return Err(TransportError::new("TCP connection closed by server")),
            }
        } else {
            None
        };
        *connection = ConnectionState::Ready(framed);
        Ok(response)
    }
}
