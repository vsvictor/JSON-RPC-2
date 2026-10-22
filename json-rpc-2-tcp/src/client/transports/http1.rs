//! HTTP/1.1 `POST /` over one persistent keep-alive connection.

use std::net::SocketAddr;

use bytes::Bytes;
use http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use hyper::client::conn::http1::{SendRequest, handshake};
use hyper_util::rt::TokioIo;
use tokio::{sync::Mutex, task::JoinHandle};

use json_rpc_2_core::MAX_MESSAGE_BYTES;
use json_rpc_2_transport::TcpTuningConfig;
use json_rpc_2_transport::{
    CONTROL_PATH, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, TransportMessage,
};
use json_rpc_2_transport::{closed, transport_error};

type Http1Connection = (SendRequest<TransportMessage>, JoinHandle<hyper::Result<()>>);

/// HTTP/1.1 client transport. HTTP 204 is reported as an empty response.
pub struct Http1ClientTransport {
    address: SocketAddr,
    uri: String,
    ping_uri: String,
    tuning: TcpTuningConfig,
    connection: Mutex<Option<Http1Connection>>,
}

impl Http1ClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_tuning(address, TcpTuningConfig::default()).await
    }

    pub async fn connect_with_tuning(
        address: SocketAddr,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            address,
            uri: format!("http://{address}/"),
            ping_uri: format!("http://{address}{CONTROL_PATH}"),
            connection: Mutex::new(Some(Self::open(address, &tuning).await?)),
            tuning,
        })
    }

    async fn open(
        address: SocketAddr,
        tuning: &TcpTuningConfig,
    ) -> Result<Http1Connection, TransportError> {
        let stream = json_rpc_2_transport::connect_tcp_stream(address, tuning).await?;
        let (sender, connection) = handshake(TokioIo::new(stream))
            .await
            .map_err(transport_error)?;
        Ok((sender, tokio::spawn(connection)))
    }
}

impl RpcClientTransport for Http1ClientTransport {
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
            let request = Request::builder()
                .method(Method::POST)
                .uri(&self.uri)
                .header(
                    header::CONTENT_TYPE,
                    if binary {
                        "application/octet-stream"
                    } else {
                        "application/json"
                    },
                )
                .body(message)
                .map_err(transport_error)?;
            let response = {
                let mut connection = self.connection.lock().await;
                let (sender, _) = connection.as_mut().ok_or_else(closed)?;
                sender.ready().await.map_err(transport_error)?;
                sender
                    .send_request(request)
                    .await
                    .map_err(transport_error)?
            };
            if response.status() == StatusCode::NO_CONTENT {
                return Ok(None);
            }
            if !response.status().is_success() {
                return Err(TransportError::new(format!(
                    "HTTP/1.1 status {}",
                    response.status()
                )));
            }
            let body = http_body_util::Limited::new(response.into_body(), MAX_MESSAGE_BYTES)
                .collect()
                .await
                .map_err(transport_error)?
                .to_bytes();
            Ok(Some(body))
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let request = Request::builder()
                .method(Method::GET)
                .uri(&self.ping_uri)
                .body(TransportMessage::from(Bytes::new()))
                .map_err(transport_error)?;
            let response = {
                let mut connection = self.connection.lock().await;
                let (sender, _) = connection.as_mut().ok_or_else(closed)?;
                sender.ready().await.map_err(transport_error)?;
                sender
                    .send_request(request)
                    .await
                    .map_err(transport_error)?
            };
            if response.status() != StatusCode::OK {
                return Err(TransportError::new(format!(
                    "HTTP/1.1 ping status {}",
                    response.status()
                )));
            }
            let body = http_body_util::Limited::new(response.into_body(), 16)
                .collect()
                .await
                .map_err(transport_error)?
                .to_bytes();
            if body.as_ref() == b"pong" {
                Ok(())
            } else {
                Err(TransportError::new(format!(
                    "unexpected HTTP/1.1 ping response: {}",
                    String::from_utf8_lossy(&body)
                )))
            }
        })
    }

    fn kind(&self) -> &'static str {
        "http1"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = Self::open(self.address, &self.tuning).await?;
            let previous = self.connection.lock().await.replace(replacement);
            if let Some((sender, task)) = previous {
                drop(sender);
                task.abort();
            }
            Ok(())
        })
    }

    /// Release the request sender; hyper then finishes in-flight exchanges
    /// and shuts the connection down.
    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let Some((sender, connection)) = self.connection.lock().await.take() else {
                return Ok(());
            };
            drop(sender);
            connection
                .await
                .map_err(transport_error)?
                .map_err(transport_error)
        })
    }
}
