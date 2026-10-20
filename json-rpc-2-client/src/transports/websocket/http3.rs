//! WebSocket established with HTTP/3 extended CONNECT (RFC 9220).

use std::net::SocketAddr;

use bytes::{Buf, Bytes};
use h3::ext::Protocol;
use http::{Method, Request, StatusCode, header};
use quinn::Endpoint;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    task::JoinHandle,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        handshake::{client::generate_key, derive_accept_key},
        protocol::Role,
    },
};

use super::{WebSocketCarrier, WebSocketClientTransport};
use crate::{
    ClientCloseFuture, TransportError,
    transports::{
        http3::{H3Driver, H3Sender, close_h3, connect_h3},
        transport_error, uri_host,
    },
};

const DUPLEX_BUFFER_BYTES: usize = 64 * 1024;

/// HTTP/3 extended CONNECT carrier: the CONNECT stream is bridged to an
/// in-memory duplex on which the WebSocket runs.
pub struct Http3Connect {
    endpoint: Endpoint,
    bridges: [JoinHandle<Result<(), TransportError>>; 2],
    sender: H3Sender,
    driver: H3Driver,
}

impl WebSocketCarrier for Http3Connect {
    type Io = DuplexStream;

    /// The WebSocket is already dropped, so the outgoing bridge sends FIN;
    /// wait until both directions of the CONNECT stream are finished and only
    /// then close the HTTP/3 connection.
    fn close(self) -> ClientCloseFuture<'static> {
        Box::pin(async move {
            for bridge in self.bridges {
                bridge.await.map_err(transport_error)??;
            }
            close_h3(&self.endpoint, self.sender, self.driver).await
        })
    }
}

/// WebSocket-over-HTTP/3 client transport.
pub type Http3WebSocketClientTransport = WebSocketClientTransport<Http3Connect>;

impl WebSocketClientTransport<Http3Connect> {
    /// Open `wss://{server_name}{path}` with extended CONNECT. The endpoint
    /// must use the `h3` ALPN protocol and is owned by the transport.
    pub async fn connect(
        endpoint: Endpoint,
        address: SocketAddr,
        server_name: &str,
        path: &str,
    ) -> Result<Self, TransportError> {
        let server_name = server_name.to_owned();
        let path = path.to_owned();
        let endpoint_for_connect = endpoint.clone();
        let (socket, carrier) =
            connect_session(endpoint_for_connect, address, &server_name, &path).await?;
        Ok(Self::new(socket, carrier, move || {
            let endpoint = endpoint.clone();
            let server_name = server_name.clone();
            let path = path.clone();
            Box::pin(async move { connect_session(endpoint, address, &server_name, &path).await })
        }))
    }
}

async fn connect_session(
    endpoint: Endpoint,
    address: SocketAddr,
    server_name: &str,
    path: &str,
) -> Result<(WebSocketStream<DuplexStream>, Http3Connect), TransportError> {
    let (mut sender, driver) = connect_h3(&endpoint, address, server_name, true).await?;

    let key = generate_key();
    let mut request = Request::builder()
        .method(Method::CONNECT)
        .uri(format!("https://{}{path}", uri_host(server_name)))
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_KEY, &key)
        .body(())
        .map_err(transport_error)?;
    request.extensions_mut().insert(Protocol::WEBSOCKET);
    let stream = sender
        .send_request(request)
        .await
        .map_err(transport_error)?;
    let (mut send_stream, mut recv_stream) = stream.split();
    let response = recv_stream.recv_response().await.map_err(transport_error)?;
    let expected_accept = derive_accept_key(key.as_bytes());
    if response.status() != StatusCode::OK
        || response
            .headers()
            .get(header::SEC_WEBSOCKET_ACCEPT)
            .map(|value| value.as_bytes())
            != Some(expected_accept.as_bytes())
    {
        return Err(TransportError::new("HTTP/3 WebSocket handshake failed"));
    }

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
                .send_data(Bytes::copy_from_slice(&buffer[..count]))
                .await
                .map_err(transport_error)?;
        }
        send_stream.finish().await.map_err(transport_error)
    });
    let incoming = tokio::spawn(async move {
        while let Some(mut chunk) = recv_stream.recv_data().await.map_err(transport_error)? {
            let count = chunk.remaining();
            if bridge_writer
                .write_all(&chunk.copy_to_bytes(count))
                .await
                .is_err()
            {
                continue;
            }
        }
        let _ = bridge_writer.shutdown().await;
        Ok(())
    });

    let socket = WebSocketStream::from_raw_socket(websocket_io, Role::Client, None).await;
    Ok((
        socket,
        Http3Connect {
            endpoint,
            bridges: [outgoing, incoming],
            sender,
            driver,
        },
    ))
}
