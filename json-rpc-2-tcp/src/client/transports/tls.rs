//! JSON lines and application-defined binary frames over verified TLS 1.2/1.3.

use std::{net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, ServerName, pem::PemObject},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

use json_rpc_2_transport::TcpTuningConfig;
use json_rpc_2_transport::{
    CONTROL_PING, CONTROL_PONG, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, RpcClientTransport, TLS_ALPN, TransportError, TransportMessage,
    expects_response, transport_error,
};

// Keep the mixed-stream helpers available for TLS-only feature builds too.
#[cfg(not(feature = "tcp"))]
#[allow(dead_code)]
#[path = "tcp.rs"]
mod tcp;
#[cfg(feature = "tcp")]
use super::tcp;
pub(crate) use tcp::{read_frame, validate_message, write_message};

type Connection = (TlsStream<TcpStream>, BytesMut);

enum ConnectionState {
    Ready(Box<Connection>),
    Invalidated,
    Closed,
}

/// TLS client for JSON lines and application-defined binary frames over TCP.
///
/// The supplied CA bundle is required; certificates and hostnames are always
/// verified. An interrupted exchange discards its connection and is not replayed.
pub struct TlsClientTransport {
    address: SocketAddr,
    server_name: String,
    connector: TlsConnector,
    tuning: TcpTuningConfig,
    framing: Option<Arc<dyn json_rpc_2_transport::StreamFraming>>,
    connection: Mutex<ConnectionState>,
}

impl TlsClientTransport {
    pub async fn connect(
        address: SocketAddr,
        server_name: &str,
        ca_pem: &str,
    ) -> Result<Self, TransportError> {
        Self::connect_with_tuning(address, server_name, ca_pem, TcpTuningConfig::default()).await
    }

    pub async fn connect_with_tuning(
        address: SocketAddr,
        server_name: &str,
        ca_pem: &str,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        let mut roots = RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
            roots
                .add(certificate.map_err(transport_error)?)
                .map_err(transport_error)?;
        }
        Self::connect_with_roots(address, server_name, roots, tuning).await
    }

    pub async fn connect_with_certificates(
        address: SocketAddr,
        server_name: &str,
        certificates: &[CertificateDer<'static>],
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        let mut roots = RootCertStore::empty();
        for certificate in certificates {
            roots.add(certificate.clone()).map_err(transport_error)?;
        }
        Self::connect_with_roots(address, server_name, roots, tuning).await
    }

    async fn connect_with_roots(
        address: SocketAddr,
        server_name: &str,
        roots: RootCertStore,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        if roots.is_empty() {
            return Err(TransportError::new(
                "TLS CA bundle contains no certificates",
            ));
        }
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(transport_error)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![TLS_ALPN.to_vec()];
        let client = Self {
            address,
            server_name: server_name.to_owned(),
            connector: TlsConnector::from(Arc::new(config)),
            tuning,
            framing: None,
            connection: Mutex::new(ConnectionState::Invalidated),
        };
        let connection = client.open().await?;
        *client.connection.lock().await = ConnectionState::Ready(Box::new(connection));
        Ok(client)
    }

    pub fn with_stream_framing(
        mut self,
        framing: Arc<dyn json_rpc_2_transport::StreamFraming>,
    ) -> Self {
        self.framing = Some(framing);
        self
    }

    async fn open(&self) -> Result<Connection, TransportError> {
        let stream = json_rpc_2_transport::connect_tcp_stream(self.address, &self.tuning).await?;
        let server_name =
            ServerName::try_from(self.server_name.clone()).map_err(transport_error)?;
        let stream = self
            .connector
            .connect(server_name, stream)
            .await
            .map_err(transport_error)?;
        if stream.get_ref().1.alpn_protocol() != Some(TLS_ALPN) {
            return Err(TransportError::new(
                "TLS server did not negotiate the JSON-RPC TLS ALPN",
            ));
        }
        Ok((stream, BytesMut::new()))
    }
}

impl RpcClientTransport for TlsClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects = expects_response(&message);
            self.send_line(message, expects).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move { self.send_line(message, expects_response).await })
    }

    fn send_message<'a>(
        &'a self,
        message: TransportMessage,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move { self.exchange(message, expects_response).await })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            match self
                .send_line(Bytes::from_static(CONTROL_PING), true)
                .await?
            {
                Some(response) if response.as_ref() == CONTROL_PONG => Ok(()),
                Some(response) => Err(TransportError::new(format!(
                    "unexpected TLS ping response: {}",
                    String::from_utf8_lossy(&response),
                ))),
                None => Err(TransportError::new("TLS connection closed by server")),
            }
        })
    }

    fn kind(&self) -> &'static str {
        "tls"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            let replacement = self.open().await?;
            *connection = ConnectionState::Ready(Box::new(replacement));
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            let ConnectionState::Ready(framed) =
                std::mem::replace(&mut *connection, ConnectionState::Closed)
            else {
                return Ok(());
            };
            let (mut stream, _) = *framed;
            stream.shutdown().await.map_err(transport_error)?;
            stream
                .read_to_end(&mut Vec::new())
                .await
                .map_err(transport_error)?;
            Ok(())
        })
    }
}

impl TlsClientTransport {
    async fn send_line(
        &self,
        message: Bytes,
        expects_response: bool,
    ) -> Result<Option<Bytes>, TransportError> {
        self.exchange(message.into(), expects_response).await
    }

    async fn exchange(
        &self,
        message: TransportMessage,
        expects_response: bool,
    ) -> Result<Option<Bytes>, TransportError> {
        validate_message(&message, self.framing.as_deref()).map_err(transport_error)?;
        let mut connection = self.connection.lock().await;
        if matches!(*connection, ConnectionState::Closed) {
            return Err(json_rpc_2_transport::closed());
        }
        let mut framed = match std::mem::replace(&mut *connection, ConnectionState::Invalidated) {
            ConnectionState::Ready(framed) => framed,
            ConnectionState::Invalidated => Box::new(self.open().await?),
            ConnectionState::Closed => return Err(json_rpc_2_transport::closed()),
        };
        write_message(&mut framed.0, &message, self.framing.as_deref())
            .await
            .map_err(transport_error)?;
        let response = if expects_response {
            match read_frame(&mut framed.0, &mut framed.1, self.framing.as_deref())
                .await
                .map_err(transport_error)?
            {
                Some(response) => Some(response),
                None => return Err(TransportError::new("TLS connection closed by server")),
            }
        } else {
            None
        };
        *connection = ConnectionState::Ready(framed);
        Ok(response)
    }
}
