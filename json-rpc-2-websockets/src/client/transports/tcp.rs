//! Dedicated WebSocket-over-TCP client transport.

use http::Uri;
use json_rpc_2_transport::TcpTuningConfig;
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

use super::{WebSocketCarrier, WebSocketClientTransport};
use json_rpc_2_transport::{ClientCloseFuture, TransportError};

/// Plain TCP carrier dedicated to one WebSocket session.
pub struct TcpWebSocketCarrier;

impl WebSocketCarrier for TcpWebSocketCarrier {
    type Io = TcpStream;

    fn close(self) -> ClientCloseFuture<'static> {
        Box::pin(async { Ok(()) })
    }
}

/// WebSocket-over-TCP client transport.
pub type TcpWebSocketClientTransport = WebSocketClientTransport<TcpWebSocketCarrier>;

impl WebSocketClientTransport<TcpWebSocketCarrier> {
    /// Connect to a dedicated `ws://host:port` endpoint or bare `host:port`.
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        Self::connect_with_tuning(url, TcpTuningConfig::default()).await
    }

    pub async fn connect_with_tuning(
        url: &str,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        let url = url.to_owned();
        let socket = connect_session(&url, &tuning).await?;
        Ok(Self::new(socket, TcpWebSocketCarrier, move || {
            let url = url.clone();
            let tuning = tuning.clone();
            Box::pin(async move {
                let socket = connect_session(&url, &tuning).await?;
                Ok((socket, TcpWebSocketCarrier))
            })
        }))
    }
}

async fn connect_session(
    url: &str,
    tuning: &TcpTuningConfig,
) -> Result<WebSocketStream<TcpStream>, TransportError> {
    let address = tcp_address(url)?;
    let address = address
        .parse()
        .map_err(|error| TransportError::new(format!("invalid TCP socket address: {error}")))?;
    let stream = json_rpc_2_transport::connect_tcp_stream(address, tuning).await?;
    Ok(WebSocketStream::from_raw_socket(stream, Role::Client, None).await)
}

fn tcp_address(url: &str) -> Result<String, TransportError> {
    if !url.contains("://") {
        return Ok(url.to_owned());
    }

    let uri: Uri = url
        .parse()
        .map_err(|error| TransportError::new(format!("invalid WebSocket/TCP URL: {error}")))?;
    if uri.scheme_str() != Some("ws") {
        return Err(TransportError::new(
            "WebSocket/TCP only supports ws:// URLs".to_owned(),
        ));
    }
    let authority = uri
        .authority()
        .ok_or_else(|| TransportError::new("WebSocket/TCP URL is missing authority".to_owned()))?;
    if uri.path() != "/" || uri.query().is_some() {
        return Err(TransportError::new(
            "WebSocket/TCP URLs must not include a path or query".to_owned(),
        ));
    }
    Ok(authority.as_str().to_owned())
}
