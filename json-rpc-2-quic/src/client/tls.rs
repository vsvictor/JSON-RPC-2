use std::{net::SocketAddr, path::Path, sync::Arc};

use json_rpc_2_transport::{TransportError, transport_error};
use rustls::pki_types::{CertificateDer, pem::PemObject};

/// Explicit server trust for QUIC, HTTP/3, and their WebSocket variants.
/// Configure before connecting; changing this value does not update live connections.
#[derive(Clone)]
pub struct ClientTlsConfig {
    roots: rustls::RootCertStore,
    ca_pem: String,
}

impl Default for ClientTlsConfig {
    fn default() -> Self {
        Self {
            roots: rustls::RootCertStore::empty(),
            ca_pem: String::new(),
        }
    }
}

impl ClientTlsConfig {
    /// Replace the trusted certificates with a PEM bundle.
    /// Invalid or empty bundles leave the previous configuration unchanged.
    pub fn set_ca_certificate_pem(&mut self, pem: &str) -> Result<&mut Self, TransportError> {
        let mut roots = rustls::RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            roots
                .add(certificate.map_err(transport_error)?)
                .map_err(transport_error)?;
        }
        if roots.is_empty() {
            return Err(TransportError::new(
                "PEM CA bundle contains no certificates",
            ));
        }
        self.roots = roots;
        self.ca_pem = pem.to_owned();
        Ok(self)
    }

    /// Load trusted certificates from a PEM file.
    pub fn set_ca_certificate_file(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<&mut Self, TransportError> {
        let pem = std::fs::read_to_string(path).map_err(transport_error)?;
        self.set_ca_certificate_pem(&pem)
    }

    pub fn ca_certificate_pem(&self) -> &str {
        &self.ca_pem
    }

    /// Create an endpoint using the transport's ALPN and the configured trust.
    /// TLS server-name verification remains enabled.
    pub fn endpoint(
        &self,
        local: SocketAddr,
        alpn: &[u8],
    ) -> Result<quinn::Endpoint, TransportError> {
        if self.roots.is_empty() {
            return Err(TransportError::new(
                "a PEM CA certificate must be configured before connecting",
            ));
        }
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(transport_error)?
        .with_root_certificates(self.roots.clone())
        .with_no_client_auth();
        tls.alpn_protocols = vec![alpn.to_vec()];
        let crypto =
            quinn::crypto::rustls::QuicClientConfig::try_from(tls).map_err(transport_error)?;
        let mut endpoint = quinn::Endpoint::client(local).map_err(transport_error)?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
        Ok(endpoint)
    }
}
