//! Dedicated WebSocket-over-QUIC client transport.

use std::net::SocketAddr;

use quinn::{Connection, Endpoint};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

use super::{WebSocketCarrier, WebSocketClientTransport};
use json_rpc_2_core::{ClientCloseFuture, TransportError, transport_error};

const DUPLEX_BUFFER_BYTES: usize = 64 * 1024;

/// Raw QUIC carrier dedicated to one WebSocket session.
pub struct QuicWebSocketCarrier {
    endpoint: Endpoint,
    connection: Connection,
    bridges: [JoinHandle<Result<(), TransportError>>; 2],
}

impl WebSocketCarrier for QuicWebSocketCarrier {
    type Io = DuplexStream;

    fn close(self) -> ClientCloseFuture<'static> {
        Box::pin(async move {
            for bridge in self.bridges {
                bridge.await.map_err(transport_error)??;
            }
            self.connection.close(0u32.into(), b"client closed");
            self.endpoint.wait_idle().await;
            Ok(())
        })
    }
}

/// WebSocket-over-QUIC client transport.
pub type QuicWebSocketClientTransport = WebSocketClientTransport<QuicWebSocketCarrier>;

impl WebSocketClientTransport<QuicWebSocketCarrier> {
    pub async fn connect(
        endpoint: Endpoint,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<Self, TransportError> {
        let server_name = server_name.to_owned();
        let endpoint_for_connect = endpoint.clone();
        let (socket, carrier) =
            connect_session(endpoint_for_connect, address, &server_name).await?;
        Ok(Self::new(socket, carrier, move || {
            let endpoint = endpoint.clone();
            let server_name = server_name.clone();
            Box::pin(async move { connect_session(endpoint, address, &server_name).await })
        }))
    }
}

async fn connect_session(
    endpoint: Endpoint,
    address: SocketAddr,
    server_name: &str,
) -> Result<(WebSocketStream<DuplexStream>, QuicWebSocketCarrier), TransportError> {
    let connection = endpoint
        .connect(address, server_name)
        .map_err(transport_error)?
        .await
        .map_err(transport_error)?;
    let (mut send_stream, mut recv_stream) = connection.open_bi().await.map_err(transport_error)?;
    let (websocket_io, bridge_io) = tokio::io::duplex(DUPLEX_BUFFER_BYTES);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge_io);
    let outgoing = tokio::spawn(async move {
        let mut buffer = [0; 16 * 1024];
        loop {
            let count = bridge_reader
                .read(&mut buffer)
                .await
                .map_err(transport_error)?;
            if count == 0 {
                break;
            }
            send_stream
                .write_all(&buffer[..count])
                .await
                .map_err(transport_error)?;
        }
        send_stream.finish().map_err(transport_error)
    });
    let incoming = tokio::spawn(async move {
        while let Some(chunk) = recv_stream
            .read_chunk(16 * 1024, true)
            .await
            .map_err(transport_error)?
        {
            if bridge_writer.write_all(&chunk.bytes).await.is_err() {
                continue;
            }
        }
        let _ = bridge_writer.shutdown().await;
        Ok(())
    });

    let socket = WebSocketStream::from_raw_socket(websocket_io, Role::Client, None).await;
    Ok((
        socket,
        QuicWebSocketCarrier {
            endpoint,
            connection,
            bridges: [outgoing, incoming],
        },
    ))
}
