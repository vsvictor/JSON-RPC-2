//! WebSocket established with the HTTP/1.1 Upgrade mechanism.

use json_rpc_2_transport::TcpTuningConfig;
use tokio::net::TcpStream;
#[cfg(not(feature = "tls"))]
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::{MaybeTlsStream, client_async};

use super::{WebSocketCarrier, WebSocketClientTransport};
use json_rpc_2_transport::{
    ClientCloseFuture, TransportError, connect_tcp_stream, transport_error,
};
#[cfg(feature = "tls")]
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, pem::PemObject},
};
#[cfg(feature = "tls")]
use std::net::SocketAddr;
#[cfg(feature = "tls")]
use std::sync::Arc;
#[cfg(feature = "tls")]
use tokio_tungstenite::Connector;

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
        #[cfg(feature = "tls")]
        {
            Self::connect_with_connector(url, tuning, None).await
        }
        #[cfg(not(feature = "tls"))]
        {
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

    #[cfg(feature = "tls")]
    /// Connect using a caller-provided CA bundle for certificate and hostname verification.
    pub async fn connect_with_ca_pem(
        url: &str,
        ca_pem: &str,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        let certificates = CertificateDer::pem_slice_iter(ca_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(transport_error)?;
        Self::connect_with_certificates(url, &certificates, tuning).await
    }

    #[cfg(feature = "tls")]
    /// Connect using explicitly trusted roots while retaining hostname verification.
    pub async fn connect_with_certificates(
        url: &str,
        certificates: &[CertificateDer<'static>],
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_connector(url, tuning, Some(tls_connector(certificates)?)).await
    }

    #[cfg(feature = "tls")]
    /// Connect to a resolved address while verifying the hostname in the WSS URL.
    pub async fn connect_with_certificates_to_addr(
        url: &str,
        address: SocketAddr,
        certificates: &[CertificateDer<'static>],
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        let connector = tls_connector(certificates)?;
        let url = url.to_owned();
        let (socket, _) =
            connect_session_to_addr(&url, address, &tuning, connector.clone()).await?;
        Ok(Self::new(socket, Http1Upgrade, move || {
            let url = url.clone();
            let tuning = tuning.clone();
            let connector = connector.clone();
            Box::pin(async move {
                let (socket, _) =
                    connect_session_to_addr(&url, address, &tuning, connector).await?;
                Ok((socket, Http1Upgrade))
            })
        }))
    }

    #[cfg(feature = "tls")]
    /// Connect to a resolved address using a caller-provided PEM CA bundle.
    pub async fn connect_with_ca_pem_to_addr(
        url: &str,
        address: SocketAddr,
        ca_pem: &str,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        let certificates = CertificateDer::pem_slice_iter(ca_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(transport_error)?;
        Self::connect_with_certificates_to_addr(url, address, &certificates, tuning).await
    }

    #[cfg(feature = "tls")]
    async fn connect_with_connector(
        url: &str,
        tuning: TcpTuningConfig,
        connector: Option<Connector>,
    ) -> Result<Self, TransportError> {
        let url = url.to_owned();
        let (socket, _) = connect_session(&url, &tuning, connector.clone()).await?;
        Ok(Self::new(socket, Http1Upgrade, move || {
            let url = url.clone();
            let tuning = tuning.clone();
            let connector = connector.clone();
            Box::pin(async move {
                let (socket, _) = connect_session(&url, &tuning, connector).await?;
                Ok((socket, Http1Upgrade))
            })
        }))
    }
}

#[cfg(feature = "tls")]
fn tls_connector(certificates: &[CertificateDer<'static>]) -> Result<Connector, TransportError> {
    if certificates.is_empty() {
        return Err(TransportError::new(
            "WebSocket TLS requires at least one trusted certificate".to_owned(),
        ));
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate.clone()).map_err(transport_error)?;
    }
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(transport_error)?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(Connector::Rustls(Arc::new(config)))
}

async fn connect_session(
    url: &str,
    tuning: &TcpTuningConfig,
    #[cfg(feature = "tls")] connector: Option<Connector>,
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
        Some("wss") => {
            #[cfg(feature = "tls")]
            {
                tokio_tungstenite::connect_async_tls_with_config(
                    url,
                    None,
                    tuning.no_delay,
                    connector,
                )
                .await
                .map_err(transport_error)
            }
            #[cfg(not(feature = "tls"))]
            {
                connect_async_with_config(url, None, tuning.no_delay)
                    .await
                    .map_err(transport_error)
            }
        }

        other => Err(TransportError::new(format!(
            "unsupported WebSocket scheme {}",
            other.unwrap_or("<missing>")
        ))),
    }
}

#[cfg(feature = "tls")]
async fn connect_session_to_addr(
    url: &str,
    address: SocketAddr,
    tuning: &TcpTuningConfig,
    connector: Connector,
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
    if uri.scheme_str() != Some("wss") {
        return Err(TransportError::new(
            "TLS WebSocket URL must use the wss scheme".to_owned(),
        ));
    }
    let stream = connect_tcp_stream(address, tuning).await?;
    tokio_tungstenite::client_async_tls_with_config(url, stream, None, Some(connector))
        .await
        .map_err(transport_error)
}
