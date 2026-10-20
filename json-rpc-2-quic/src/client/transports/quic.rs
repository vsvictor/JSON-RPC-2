//! Raw QUIC: one connection, one bidirectional stream per message.

use std::net::SocketAddr;

use bytes::Bytes;
use quinn::{Connection, Endpoint, VarInt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, RpcClientTransport, TransportError,
};
use json_rpc_2_transport::{closed, transport_error};

const QUIC_NO_ERROR: u32 = 0;
const QUIC_REQUEST_CANCELLED: u32 = 0x10c;

/// Raw QUIC client transport. The endpoint must be configured by the caller
/// (TLS, ALPN) and is owned by the transport.
pub struct QuicClientTransport {
    endpoint: Endpoint,
    address: SocketAddr,
    server_name: String,
    connection: Mutex<Option<Connection>>,
}

impl QuicClientTransport {
    pub async fn connect(
        endpoint: Endpoint,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<Self, TransportError> {
        let connection = endpoint
            .connect(address, server_name)
            .map_err(transport_error)?
            .await
            .map_err(transport_error)?;
        Ok(Self {
            endpoint,
            address,
            server_name: server_name.to_owned(),
            connection: Mutex::new(Some(connection)),
        })
    }

    async fn open(&self) -> Result<Connection, TransportError> {
        self.endpoint
            .connect(self.address, &self.server_name)
            .map_err(transport_error)?
            .await
            .map_err(transport_error)
    }

    async fn current_connection(&self) -> Result<Connection, TransportError> {
        self.connection
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(closed)
    }
}

impl RpcClientTransport for QuicClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let connection = self.current_connection().await?;
            send_request(connection, message, None).await
        })
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        _expects_response: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let connection = self.current_connection().await?;
            send_request(connection, message, Some(cancel)).await
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let connection = self.current_connection().await?;
            let (mut send, mut recv) = connection.open_bi().await.map_err(transport_error)?;
            send.write_all(CONTROL_PING)
                .await
                .map_err(transport_error)?;
            send.finish().map_err(transport_error)?;
            let response = recv
                .read_to_end(CONTROL_PONG.len())
                .await
                .map_err(transport_error)?;
            if response == CONTROL_PONG {
                Ok(())
            } else {
                Err(TransportError::new(format!(
                    "unexpected QUIC ping response: {}",
                    String::from_utf8_lossy(&response)
                )))
            }
        })
    }

    fn kind(&self) -> &'static str {
        "quic"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = self.open().await?;
            let previous = self.connection.lock().await.replace(replacement);
            if let Some(connection) = previous {
                connection.close(QUIC_NO_ERROR.into(), b"client reconnect");
            }
            Ok(())
        })
    }

    /// Close the connection with code 0 and wait until the endpoint has
    /// delivered CONNECTION_CLOSE to the server.
    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            if let Some(connection) = self.connection.lock().await.take() {
                connection.close(QUIC_NO_ERROR.into(), b"client closed");
            }
            self.endpoint.wait_idle().await;
            Ok(())
        })
    }
}

async fn send_request(
    connection: Connection,
    message: Bytes,
    cancel: Option<CancellationToken>,
) -> Result<Option<Bytes>, TransportError> {
    let (mut send, mut recv) = connection.open_bi().await.map_err(transport_error)?;
    write_quic_message(&mut send, &mut recv, &message, cancel.as_ref()).await?;
    send.finish().map_err(transport_error)?;
    let response = read_quic_response(&mut send, &mut recv, cancel.as_ref()).await?;
    Ok((!response.is_empty()).then_some(response))
}

async fn write_quic_message(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    message: &[u8],
    cancel: Option<&CancellationToken>,
) -> Result<(), TransportError> {
    match cancel {
        Some(cancel) => {
            tokio::select! {
                result = send.write_all(message) => result.map_err(transport_error),
                _ = cancel.cancelled() => {
                    cancel_quic_stream(send, recv);
                    Err(TransportError::new("request cancelled"))
                }
            }
        }
        None => send.write_all(message).await.map_err(transport_error),
    }
}

async fn read_quic_response(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    cancel: Option<&CancellationToken>,
) -> Result<Bytes, TransportError> {
    match cancel {
        Some(cancel) => {
            tokio::select! {
                result = recv.read_to_end(MAX_MESSAGE_BYTES) => {
                    result.map(Bytes::from).map_err(transport_error)
                }
                _ = cancel.cancelled() => {
                    cancel_quic_stream(send, recv);
                    Err(TransportError::new("request cancelled"))
                }
            }
        }
        None => recv
            .read_to_end(MAX_MESSAGE_BYTES)
            .await
            .map(Bytes::from)
            .map_err(transport_error),
    }
}

fn cancel_quic_stream(send: &mut quinn::SendStream, recv: &mut quinn::RecvStream) {
    let error_code = VarInt::from_u32(QUIC_REQUEST_CANCELLED);
    let _ = send.reset(error_code);
    let _ = recv.stop(error_code);
}
