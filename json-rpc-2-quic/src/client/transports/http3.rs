//! HTTP/3 `POST /`, one request stream per message on one QUIC connection.

use std::{future::poll_fn, net::SocketAddr};

use bytes::{Buf, Bytes, BytesMut};
use h3::{
    client::{RequestStream, SendRequest},
    error::{Code, ConnectionError},
};
use http::{Method, Request, Response, StatusCode, header};
use quinn::Endpoint;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::uri_host;
use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::{
    CONTROL_PATH, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, TransportMessage,
};
use json_rpc_2_transport::{closed, transport_error};

pub type H3Sender = SendRequest<h3_quinn::OpenStreams, Bytes>;
pub type H3Driver = JoinHandle<ConnectionError>;
type H3RequestStream = RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

/// Establish an HTTP/3 connection and drive it in a background task.
pub async fn connect_h3(
    endpoint: &Endpoint,
    address: SocketAddr,
    server_name: &str,
    extended_connect: bool,
) -> Result<(H3Sender, H3Driver), TransportError> {
    let connection = endpoint
        .connect(address, server_name)
        .map_err(transport_error)?
        .await
        .map_err(transport_error)?;
    let (mut driver, sender) = h3::client::builder()
        .enable_extended_connect(extended_connect)
        .build(h3_quinn::Connection::new(connection))
        .await
        .map_err(transport_error)?;
    let driver = tokio::spawn(async move { poll_fn(|cx| driver.poll_close(cx)).await });
    Ok((sender, driver))
}

/// Close an HTTP/3 connection once all of its request streams are finished.
pub async fn close_h3(
    endpoint: &Endpoint,
    sender: H3Sender,
    driver: H3Driver,
) -> Result<(), TransportError> {
    drop(sender);
    let error = driver.await.map_err(transport_error)?;
    if !error.is_h3_no_error() {
        return Err(transport_error(error));
    }
    endpoint.wait_idle().await;
    Ok(())
}

/// HTTP/3 client transport. The endpoint must use the `h3` ALPN protocol and
/// is owned by the transport. HTTP 204 is reported as an empty response.
pub struct Http3ClientTransport {
    endpoint: Endpoint,
    address: SocketAddr,
    server_name: String,
    uri: String,
    ping_uri: String,
    connection: Mutex<Option<(H3Sender, H3Driver)>>,
}

impl Http3ClientTransport {
    pub async fn connect(
        endpoint: Endpoint,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<Self, TransportError> {
        let connection = connect_h3(&endpoint, address, server_name, false).await?;
        Ok(Self {
            endpoint,
            address,
            server_name: server_name.to_owned(),
            uri: format!("https://{}/", uri_host(server_name)),
            ping_uri: format!("https://{}{CONTROL_PATH}", uri_host(server_name)),
            connection: Mutex::new(Some(connection)),
        })
    }

    async fn request_stream(
        &self,
        request: Request<()>,
    ) -> Result<H3RequestStream, TransportError> {
        let mut connection = self.connection.lock().await;
        let (sender, _) = connection.as_mut().ok_or_else(closed)?;
        sender.send_request(request).await.map_err(transport_error)
    }
}

impl RpcClientTransport for Http3ClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        self.send_message(message.into(), true)
    }

    fn send_message<'a>(
        &'a self,
        message: TransportMessage,
        _expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let binary =
                json_rpc_2_transport::gateway::is_gateway_frame(if message.header.is_empty() {
                    &message.payload
                } else {
                    &message.header
                });
            let request = rpc_request(&self.uri, binary)?;
            let mut stream = self.request_stream(request).await?;
            receive_http3_response(&mut stream, message, None).await
        })
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        _expects_response: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let request = rpc_request(
                &self.uri,
                json_rpc_2_transport::gateway::is_gateway_frame(&message),
            )?;
            let mut stream = self.request_stream(request).await?;
            receive_http3_response(&mut stream, message.into(), Some(&cancel)).await
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let request = Request::builder()
                .method(Method::GET)
                .uri(&self.ping_uri)
                .body(())
                .map_err(transport_error)?;
            let mut stream = self.request_stream(request).await?;
            stream.finish().await.map_err(transport_error)?;
            let response = stream.recv_response().await.map_err(transport_error)?;
            if response.status() != StatusCode::OK {
                return Err(TransportError::new(format!(
                    "HTTP/3 ping status {}",
                    response.status()
                )));
            }
            let body = recv_http3_body(&mut stream, 16, None).await?;
            if body.as_ref() == b"pong" {
                Ok(())
            } else {
                Err(TransportError::new(format!(
                    "unexpected HTTP/3 ping response: {}",
                    String::from_utf8_lossy(&body)
                )))
            }
        })
    }

    fn kind(&self) -> &'static str {
        "http3"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement =
                connect_h3(&self.endpoint, self.address, &self.server_name, false).await?;
            let previous = self.connection.lock().await.replace(replacement);
            if let Some((sender, driver)) = previous {
                drop(sender);
                driver.abort();
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            match self.connection.lock().await.take() {
                Some((sender, driver)) => close_h3(&self.endpoint, sender, driver).await,
                None => Ok(()),
            }
        })
    }
}

fn rpc_request(uri: &str, binary: bool) -> Result<Request<()>, TransportError> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(
            header::CONTENT_TYPE,
            if binary {
                "application/octet-stream"
            } else {
                "application/json"
            },
        )
        .body(())
        .map_err(transport_error)
}

async fn receive_http3_response(
    stream: &mut H3RequestStream,
    message: TransportMessage,
    cancel: Option<&CancellationToken>,
) -> Result<Option<Bytes>, TransportError> {
    if !message.header.is_empty() {
        send_http3_data(stream, message.header, cancel).await?;
    }
    if !message.payload.is_empty() {
        send_http3_data(stream, message.payload, cancel).await?;
    }
    finish_http3_stream(stream, cancel).await?;
    let response = recv_http3_headers(stream, cancel).await?;
    if response.status() == StatusCode::NO_CONTENT {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(TransportError::new(format!(
            "HTTP/3 status {}",
            response.status()
        )));
    }
    let body = recv_http3_body(stream, MAX_MESSAGE_BYTES, cancel).await?;
    Ok(Some(body))
}

async fn send_http3_data(
    stream: &mut H3RequestStream,
    message: Bytes,
    cancel: Option<&CancellationToken>,
) -> Result<(), TransportError> {
    match cancel {
        Some(cancel) => tokio::select! {
            result = stream.send_data(message) => result.map_err(transport_error),
            _ = cancel.cancelled() => {
                cancel_http3_stream(stream);
                Err(TransportError::new("request cancelled"))
            }
        },
        None => stream.send_data(message).await.map_err(transport_error),
    }
}

async fn finish_http3_stream(
    stream: &mut H3RequestStream,
    cancel: Option<&CancellationToken>,
) -> Result<(), TransportError> {
    match cancel {
        Some(cancel) => tokio::select! {
            result = stream.finish() => result.map_err(transport_error),
            _ = cancel.cancelled() => {
                cancel_http3_stream(stream);
                Err(TransportError::new("request cancelled"))
            }
        },
        None => stream.finish().await.map_err(transport_error),
    }
}

async fn recv_http3_headers(
    stream: &mut H3RequestStream,
    cancel: Option<&CancellationToken>,
) -> Result<Response<()>, TransportError> {
    match cancel {
        Some(cancel) => tokio::select! {
            result = stream.recv_response() => result.map_err(transport_error),
            _ = cancel.cancelled() => {
                cancel_http3_stream(stream);
                Err(TransportError::new("request cancelled"))
            }
        },
        None => stream.recv_response().await.map_err(transport_error),
    }
}

async fn recv_http3_body(
    stream: &mut H3RequestStream,
    limit: usize,
    cancel: Option<&CancellationToken>,
) -> Result<Bytes, TransportError> {
    let mut body = BytesMut::new();
    loop {
        let next = match cancel {
            Some(cancel) => tokio::select! {
                result = stream.recv_data() => result.map_err(transport_error)?,
                _ = cancel.cancelled() => {
                    cancel_http3_stream(stream);
                    return Err(TransportError::new("request cancelled"));
                }
            },
            None => stream.recv_data().await.map_err(transport_error)?,
        };
        let Some(mut chunk) = next else {
            return Ok(body.freeze());
        };
        let len = chunk.remaining();
        if body.len() + len > limit {
            return Err(TransportError::new("HTTP/3 response exceeded size limit"));
        }
        body.extend_from_slice(&chunk.copy_to_bytes(len));
    }
}

fn cancel_http3_stream(stream: &mut H3RequestStream) {
    stream.stop_stream(Code::H3_REQUEST_CANCELLED);
    stream.stop_sending(Code::H3_REQUEST_CANCELLED);
}
