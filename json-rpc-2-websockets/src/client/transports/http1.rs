//! WebSocket established with the HTTP/1.1 Upgrade mechanism.

use json_rpc_2_transport::TcpTuningConfig;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, client_async, connect_async_with_config};

use super::{WebSocketCarrier, WebSocketClientTransport};
use json_rpc_2_transport::{
    ClientCloseFuture, TransportError, connect_tcp_stream, transport_error,
};

/// HTTP/1.1 Upgrade carrier. The TCP connection is closed together with the
/// WebSocket, so nothing remains to release afterwards.
pub struct Http1Upgrade;

impl WebSocketCarrier for Http1Upgrade {
    type Io = MaybeTlsStream<TcpStream>;

    fn close(self) -> ClientCloseFuture<'static> {
        Box::pin(async { Ok(()) })
    }
}

/// WebSocket-over-HTTP/1.1 client transport.
pub type Http1WebSocketClientTransport = WebSocketClientTransport<Http1Upgrade>;

impl WebSocketClientTransport<Http1Upgrade> {
    /// Connect to a `ws://` or `wss://` URL.
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        Self::connect_with_tuning(url, TcpTuningConfig::default()).await
    }

    pub async fn connect_with_tuning(
        url: &str,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        let url = url.to_owned();
        let (socket, _) = connect_session(&url, &tuning).await?;
        Ok(Self::new(socket, Http1Upgrade, move || {
            let url = url.clone();
            let tuning = tuning.clone();
            Box::pin(async move {
                let (socket, _) = connect_session(&url, &tuning).await?;
                Ok((socket, Http1Upgrade))
            })
        }))
    }
}

async fn connect_session(
    url: &str,
    tuning: &TcpTuningConfig,
) -> Result<
    (
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        http::Response<Option<Vec<u8>>>,
    ),
    TransportError,
> {
    let uri: http::Uri = url
        .parse()
        .map_err(|error| TransportError::new(format!("invalid WebSocket URL: {error}")))?;
    match uri.scheme_str() {
        Some("ws") => {
            let authority = uri.authority().ok_or_else(|| {
                TransportError::new("WebSocket/HTTP1 URL is missing authority".to_owned())
            })?;
            let address: std::net::SocketAddr = authority.as_str().parse().map_err(|error| {
                TransportError::new(format!("invalid TCP socket address: {error}"))
            })?;
            let stream = connect_tcp_stream(address, tuning).await?;
            client_async(url, MaybeTlsStream::Plain(stream))
                .await
                .map_err(transport_error)
        }
        Some("wss") => connect_async_with_config(url, None, tuning.no_delay)
            .await
            .map_err(transport_error),
        other => Err(TransportError::new(format!(
            "unsupported WebSocket scheme {}",
            other.unwrap_or("<missing>")
        ))),
    }
}
