//! WebSocket client transport, independent of the HTTP version used to
//! establish the WebSocket connection.

use std::{future::Future, pin::Pin};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as WsError, Message},
};

use json_rpc_2_core::{
    WebSocketMessage, decode_websocket_message, encode_websocket_close_frame,
    encode_websocket_message,
};
use json_rpc_2_transport::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, expects_response,
};
use json_rpc_2_transport::{closed, transport_error};

const PING_PAYLOAD: &[u8] = b"json-rpc-2";

#[cfg(all(feature = "websocket", feature = "amqp"))]
pub mod amqp;
#[cfg(all(feature = "websocket", feature = "coap"))]
pub mod coap;
#[cfg(all(feature = "websocket", feature = "dccp"))]
pub mod dccp;
#[cfg(all(feature = "websocket", feature = "http1"))]
pub mod http1;
#[cfg(all(feature = "websocket", feature = "http3"))]
pub mod http3;
#[cfg(all(feature = "websocket", feature = "mqtt"))]
pub mod mqtt;
#[cfg(all(feature = "websocket", feature = "quic"))]
pub mod quic;
#[cfg(all(feature = "websocket", feature = "rtp"))]
pub mod rtp;
#[cfg(all(feature = "websocket", feature = "rudp"))]
pub mod rudp;
#[cfg(all(feature = "websocket", feature = "sctp"))]
pub mod sctp;
#[cfg(all(feature = "websocket", feature = "srt"))]
pub mod srt;
#[cfg(all(feature = "websocket", feature = "tcp"))]
pub mod tcp;
#[cfg(all(feature = "websocket", feature = "udp"))]
pub mod udp;
#[cfg(all(
    feature = "websocket",
    feature = "udplite",
    any(target_os = "linux", target_os = "android")
))]
pub mod udplite;

/// Connection that carries a WebSocket: HTTP/1.1 Upgrade, HTTP/3 extended
/// CONNECT, or another mechanism.
pub trait WebSocketCarrier: Send + 'static {
    type Io: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Release the carrier after the WebSocket closing handshake is complete.
    fn close(self) -> ClientCloseFuture<'static>;
}

type WebSocketConnectFuture<C> = Pin<
    Box<
        dyn Future<
                Output = Result<(WebSocketStream<<C as WebSocketCarrier>::Io>, C), TransportError>,
            > + Send,
    >,
>;

/// JSON-RPC over a WebSocket: direct JSON uses text messages, while gateway
/// envelopes use binary messages.
///
/// WebSocket libraries require a contiguous frame, so the default
/// `RpcClientTransport::send_message` coalesces split gateway headers and payloads
/// at this library boundary.
pub struct WebSocketClientTransport<C: WebSocketCarrier> {
    session: Mutex<Option<(WebSocketStream<C::Io>, C)>>,
    connector: std::sync::Arc<dyn Fn() -> WebSocketConnectFuture<C> + Send + Sync>,
}

impl<C: WebSocketCarrier> WebSocketClientTransport<C> {
    /// Wrap an established WebSocket and the carrier it runs over.
    pub fn new(
        socket: WebSocketStream<C::Io>,
        carrier: C,
        connector: impl Fn() -> WebSocketConnectFuture<C> + Send + Sync + 'static,
    ) -> Self {
        Self {
            session: Mutex::new(Some((socket, carrier))),
            connector: std::sync::Arc::new(connector),
        }
    }
}

/// Generic WebSocket-style adapter over message-oriented transports such as
/// reliable UDP, DCCP, or SCTP.
pub struct DatagramWebSocketClientTransport<T> {
    inner: T,
    kind: &'static str,
    closed_message: &'static str,
}

impl<T> DatagramWebSocketClientTransport<T> {
    pub fn new(inner: T, kind: &'static str, closed_message: &'static str) -> Self {
        Self {
            inner,
            kind,
            closed_message,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    fn encode_outbound(message: Bytes) -> Result<Bytes, TransportError> {
        let message = if json_rpc_2_transport::gateway::is_gateway_frame(&message) {
            WebSocketMessage::Binary(message)
        } else {
            let text = std::str::from_utf8(&message).map_err(|error| {
                TransportError::new(format!("invalid WebSocket text payload: {error}"))
            })?;
            WebSocketMessage::Text(text.to_owned())
        };
        // The codec builds a contiguous frame, copying its payload at this boundary.
        encode_websocket_message(&message)
            .map(Bytes::from)
            .map_err(TransportError::new)
    }

    fn decode_inbound(&self, message: Bytes) -> Result<Option<Bytes>, TransportError> {
        match decode_websocket_message(&message).map_err(TransportError::new)? {
            Some(WebSocketMessage::Text(text)) => Ok(Some(Bytes::from(text.into_bytes()))),
            Some(WebSocketMessage::Binary(bytes)) => Ok(Some(bytes)),
            None => Err(TransportError::new(self.closed_message)),
        }
    }
}

impl<C: WebSocketCarrier> RpcClientTransport for WebSocketClientTransport<C> {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let (socket, _) = session.as_mut().ok_or_else(closed)?;
            let expects = expects_response(&message);
            exchange(socket, message, expects).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let (socket, _) = session.as_mut().ok_or_else(closed)?;
            exchange(socket, message, expects_response).await
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let (socket, _) = session.as_mut().ok_or_else(closed)?;
            ping(socket).await
        })
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let (socket, carrier) = (self.connector)().await?;
            let previous = self.session.lock().await.replace((socket, carrier));
            if let Some((socket, carrier)) = previous {
                let _ = close_handshake(socket).await;
                let _ = carrier.close().await;
            }
            Ok(())
        })
    }

    /// WebSocket closing handshake first, then the carrier is closed.
    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let Some((socket, carrier)) = self.session.lock().await.take() else {
                return Ok(());
            };
            close_handshake(socket).await?;
            carrier.close().await
        })
    }
}

impl<T> RpcClientTransport for DatagramWebSocketClientTransport<T>
where
    T: RpcClientTransport,
{
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects_response = json_rpc_2_transport::expects_response(&message);
            self.send_with_expectation(message, expects_response).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let payload = Self::encode_outbound(message)?;
            let response = self
                .inner
                .send_with_expectation(payload, expects_response)
                .await?;
            response
                .map(|message| self.decode_inbound(message))
                .transpose()
                .map(Option::flatten)
        })
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
        cancel: tokio_util::sync::CancellationToken,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let payload = Self::encode_outbound(message)?;
            let response = self
                .inner
                .send_cancelable(payload, expects_response, cancel)
                .await?;
            response
                .map(|message| self.decode_inbound(message))
                .transpose()
                .map(Option::flatten)
        })
    }

    fn kind(&self) -> &'static str {
        self.kind
    }

    fn peer(&self) -> Option<String> {
        self.inner.peer()
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        self.inner.transport_ping()
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        self.inner.reconnect()
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let _ = self
                .inner
                .send_with_expectation(
                    Bytes::from(encode_websocket_close_frame().map_err(TransportError::new)?),
                    false,
                )
                .await;
            self.inner.close().await
        })
    }
}

async fn ping<S>(socket: &mut WebSocketStream<S>) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .send(Message::Ping(PING_PAYLOAD.into()))
        .await
        .map_err(transport_error)?;
    loop {
        match socket.next().await {
            Some(Ok(Message::Pong(payload))) if payload.as_ref() == PING_PAYLOAD => return Ok(()),
            Some(Ok(Message::Ping(payload))) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(transport_error)?;
            }
            Some(Ok(Message::Pong(_) | Message::Frame(_))) => continue,
            Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                return Err(TransportError::new(
                    "unexpected application message during WebSocket ping",
                ));
            }
            Some(Ok(Message::Close(_))) | None => {
                return Err(TransportError::new("WebSocket closed by server"));
            }
            Some(Err(error)) => return Err(transport_error(error)),
        }
    }
}

async fn exchange<S>(
    socket: &mut WebSocketStream<S>,
    message: Bytes,
    expects: bool,
) -> Result<Option<Bytes>, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let message = if json_rpc_2_transport::gateway::is_gateway_frame(&message) {
        Message::Binary(message)
    } else {
        let text = std::str::from_utf8(&message)
            .map(str::to_owned)
            .map_err(transport_error)?;
        Message::Text(text.into())
    };
    socket.send(message).await.map_err(transport_error)?;
    if !expects {
        return Ok(None);
    }
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(response))) => {
                return Ok(Some(response.into()));
            }
            Some(Ok(Message::Binary(response))) => return Ok(Some(response)),
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
            Some(Ok(Message::Close(_))) | None => {
                return Err(TransportError::new("WebSocket closed by server"));
            }
            Some(Err(error)) => return Err(transport_error(error)),
        }
    }
}

/// Send Close and read until the server's Close reply ends the stream.
/// The socket is dropped afterwards, releasing the underlying I/O.
async fn close_handshake<S>(mut socket: WebSocketStream<S>) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match socket.close(None).await {
        Ok(()) | Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => {}
        Err(error) => return Err(transport_error(error)),
    }
    while let Some(message) = socket.next().await {
        match message {
            Ok(_) => {}
            Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => break,
            Err(error) => return Err(transport_error(error)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::Role;

    fn gateway_message(payload: &[u8]) -> Bytes {
        let mut message = b"JRPC-GW\x03".to_vec();
        message.extend_from_slice(&[0; 17]);
        message.extend_from_slice(payload);
        Bytes::from(message)
    }

    struct FramedPeer {
        request: WebSocketMessage,
        response: WebSocketMessage,
    }

    impl RpcClientTransport for FramedPeer {
        fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
            Box::pin(async move {
                assert_eq!(
                    decode_websocket_message(&message).unwrap(),
                    Some(self.request.clone())
                );
                Ok(Some(Bytes::from(
                    encode_websocket_message(&self.response).unwrap(),
                )))
            })
        }

        fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
            Box::pin(async { Ok(()) })
        }

        fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
            Box::pin(async { Ok(()) })
        }

        fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn wrapped_transports_preserve_binary_gateway_and_direct_text() {
        for (request, response, binary) in [
            (
                gateway_message(b"\xffrequest\n\0"),
                gateway_message(b"\xferesponse\n\n"),
                true,
            ),
            (
                Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#),
                Bytes::from_static(br#"{"jsonrpc":"2.0","result":"ok","id":1}"#),
                false,
            ),
        ] {
            let wrap = |bytes: Bytes| {
                if binary {
                    WebSocketMessage::Binary(bytes)
                } else {
                    WebSocketMessage::Text(std::str::from_utf8(&bytes).unwrap().to_owned())
                }
            };
            let transport = DatagramWebSocketClientTransport::new(
                FramedPeer {
                    request: wrap(request.clone()),
                    response: wrap(response.clone()),
                },
                "test",
                "closed",
            );
            assert_eq!(
                transport
                    .send_with_expectation(request.clone(), true)
                    .await
                    .unwrap(),
                Some(response.clone())
            );
            assert_eq!(
                transport
                    .send_cancelable(request, true, tokio_util::sync::CancellationToken::new(),)
                    .await
                    .unwrap(),
                Some(response)
            );
        }
    }

    #[tokio::test]
    async fn native_websockets_preserve_binary_gateway_and_direct_text() {
        for (request, response, binary) in [
            (
                gateway_message(b"\xffrequest\n\0"),
                gateway_message(b"\xferesponse\n\n"),
                true,
            ),
            (
                Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","id":1}"#),
                Bytes::from_static(br#"{"jsonrpc":"2.0","result":"ok","id":1}"#),
                false,
            ),
        ] {
            let (client, server) = tokio::io::duplex(4096);
            let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
            let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
            let expected_request = request.clone();
            let expected_response = response.clone();
            let peer = async move {
                let inbound = server.next().await.unwrap().unwrap();
                assert_eq!(inbound.is_binary(), binary);
                assert_eq!(inbound.into_data(), expected_request);
                let outbound = if binary {
                    Message::Binary(expected_response)
                } else {
                    Message::Text(
                        std::str::from_utf8(&expected_response)
                            .unwrap()
                            .to_owned()
                            .into(),
                    )
                };
                server.send(outbound).await.unwrap();
            };
            let (result, ()) = tokio::join!(exchange(&mut client, request, true), peer);
            assert_eq!(result.unwrap(), Some(response));
        }
    }

    #[test]
    fn invalid_direct_text_still_fails() {
        assert!(
            DatagramWebSocketClientTransport::<FramedPeer>::encode_outbound(Bytes::from_static(
                b"\xffnot-a-gateway-frame"
            ))
            .is_err()
        );
    }
}
