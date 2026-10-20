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

use json_rpc_2_core::{closed, transport_error};
use json_rpc_2_core::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, MAX_MESSAGE_BYTES, RpcClientTransport, TransportError, expects_response,
};
use json_rpc_2_core::TcpTuningConfig;

/// TCP client transport, one compact JSON message per line.
pub struct TcpClientTransport {
    address: SocketAddr,
    tuning: TcpTuningConfig,
    connection: Mutex<Option<Framed<TcpStream, LinesCodec>>>,
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
            connection: Mutex::new(Some(Self::open(address, &tuning).await?)),
            tuning,
        })
    }

    async fn open(
        address: SocketAddr,
        tuning: &TcpTuningConfig,
    ) -> Result<Framed<TcpStream, LinesCodec>, TransportError> {
        let stream = json_rpc_2_core::connect_tcp_stream(address, tuning).await?;
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
            let mut connection = self.connection.lock().await;
            let framed = connection.as_mut().ok_or_else(closed)?;
            framed
                .send(String::from_utf8(CONTROL_PING.to_vec()).map_err(transport_error)?)
                .await
                .map_err(transport_error)?;
            match framed.next().await {
                Some(Ok(response)) if response.as_bytes() == CONTROL_PONG => Ok(()),
                Some(Ok(response)) => Err(TransportError::new(format!(
                    "unexpected TCP ping response: {response}"
                ))),
                Some(Err(error)) => Err(transport_error(error)),
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
            let replacement = Self::open(self.address, &self.tuning).await?;
            let previous = self.connection.lock().await.replace(replacement);
            if let Some(framed) = previous {
                let mut stream = framed.into_inner();
                let _ = stream.shutdown().await;
            }
            Ok(())
        })
    }

    /// Send FIN and wait until the server closes its side too.
    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let Some(framed) = self.connection.lock().await.take() else {
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
        let mut connection = self.connection.lock().await;
        let framed = connection.as_mut().ok_or_else(closed)?;
        framed
            .send(
                std::str::from_utf8(&message)
                    .map(str::to_owned)
                    .map_err(transport_error)?,
            )
            .await
            .map_err(transport_error)?;
        if !expects_response {
            return Ok(None);
        }
        match framed.next().await {
            Some(Ok(response)) => Ok(Some(Bytes::from(response))),
            Some(Err(error)) => Err(transport_error(error)),
            None => Err(TransportError::new("TCP connection closed by server")),
        }
    }
}
