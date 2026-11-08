//! Raw QUIC: one connection, one bidirectional stream per message.

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use quinn::{Connection, Endpoint, VarInt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientIncomingFuture, ClientPingFuture,
    ClientRecoverFuture, ClientTransportFuture, IncomingQueue, RpcClientTransport, TransportError,
    TransportMessage,
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
    incoming: Arc<IncomingQueue>,
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
        let incoming = Arc::new(IncomingQueue::default());
        spawn_push_reader(connection.clone(), Arc::clone(&incoming));
        Ok(Self {
            endpoint,
            address,
            server_name: server_name.to_owned(),
            connection: Mutex::new(Some(connection)),
            incoming,
        })
    }

    async fn open(&self) -> Result<Connection, TransportError> {
        self.endpoint
            .connect(self.address, &self.server_name)
            .map_err(transport_error)?
            .await
            .map_err(transport_error)
    }

    /// Receive one server-initiated message. Each push arrives on its own
    /// unidirectional stream and never shares a request's response stream.
    /// Equivalent to [`RpcClientTransport::recv_incoming`].
    pub async fn recv_push(&self) -> Result<Bytes, TransportError> {
        self.incoming.recv().await.ok_or_else(closed)
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
        self.send_message(message.into(), true)
    }

    fn send_message<'a>(
        &'a self,
        message: TransportMessage,
        _expects_response: bool,
    ) -> ClientTransportFuture<'a> {
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
            send_request(connection, message.into(), Some(cancel)).await
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

    fn supports_incoming(&self) -> bool {
        true
    }

    fn recv_incoming<'a>(&'a self) -> ClientIncomingFuture<'a> {
        Box::pin(self.recv_push())
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = self.open().await?;
            spawn_push_reader(replacement.clone(), Arc::clone(&self.incoming));
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
            self.incoming.close();
            self.endpoint.wait_idle().await;
            Ok(())
        })
    }
}

/// Accept push streams of one connection in arrival order until it closes.
/// Streams are drained here rather than in the consumer so cancelling a
/// consumer can never lose a partially read push.
fn spawn_push_reader(connection: Connection, incoming: Arc<IncomingQueue>) {
    tokio::spawn(async move {
        while let Ok(mut stream) = connection.accept_uni().await {
            if let Ok(message) = stream.read_to_end(MAX_MESSAGE_BYTES).await {
                incoming.push(Bytes::from(message));
            }
        }
    });
}

async fn send_request(
    connection: Connection,
    message: TransportMessage,
    cancel: Option<CancellationToken>,
) -> Result<Option<Bytes>, TransportError> {
    let (mut send, mut recv) = connection.open_bi().await.map_err(transport_error)?;
    write_quic_message(&mut send, &mut recv, &message.header, cancel.as_ref()).await?;
    write_quic_message(&mut send, &mut recv, &message.payload, cancel.as_ref()).await?;
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
