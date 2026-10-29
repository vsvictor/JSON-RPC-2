//! Multi-transport JSON-RPC-2 backend for JSON-RPC-2-GATEWAY.
//!
//! [`BackendBuilder`] is the only entry point: configure it (or call
//! [`BackendBuilder::from_env`]), [`build`](BackendBuilder::build) a [`Backend`]
//! and [`run`](Backend::run) it.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use json_rpc_2_gateway::GatewayBackendProtocol;
pub use json_rpc_2_runtime::TransportKind;
use json_rpc_2_runtime::{MultiTransportServer, RunningServer};
pub use json_rpc_2_server::{
    BoxedPlugin, FromSettings, MaintenanceTask, PluginFuture, PluginHandle, ServerPlugin,
};
use json_rpc_2_server::{RpcServer, Settings};
use json_rpc_2_transport::JsonRpcProtocol;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

type TlsIdentity = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);
type Configure = Box<dyn FnOnce(&RpcServer) -> Result<()> + Send>;

const MANDATORY: [TransportKind; 4] = [
    TransportKind::Tcp,
    TransportKind::Tls,
    TransportKind::Udp,
    TransportKind::Quic,
];

/// Builds a gateway backend with the mandatory TCP, TLS, UDP and QUIC listeners,
/// any additional JSON-RPC-2 transports, and any number of [`ServerPlugin`]s.
pub struct BackendBuilder {
    bind: IpAddr,
    tcp_port: u16,
    udp_port: u16,
    tls_port: u16,
    quic_port: u16,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    additional: Vec<(TransportKind, u16)>,
    srt_passphrase: Option<String>,
    instance_id: String,
    allow_ephemeral_ports: bool,
    log_level: Option<String>,
    configure: Vec<Configure>,
    plugins: Vec<BoxedPlugin>,
}

impl Default for BackendBuilder {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".parse().expect("valid loopback"),
            tcp_port: 8081,
            udp_port: 8081,
            tls_port: 8443,
            quic_port: 8443,
            tls_cert_file: None,
            tls_key_file: None,
            additional: Vec::new(),
            srt_passphrase: None,
            instance_id: "json-rpc-2-backend".into(),
            allow_ephemeral_ports: false,
            log_level: None,
            configure: Vec::new(),
            plugins: Vec::new(),
        }
    }
}

impl BackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads the `BACKEND_*` environment variables.
    pub fn from_env() -> Result<Self> {
        let values = std::env::vars_os()
            .filter_map(|(key, value)| {
                let key = key.into_string().ok()?;
                key.starts_with("BACKEND_").then(|| {
                    value
                        .into_string()
                        .map(|value| (key.clone(), value))
                        .map_err(|_| anyhow::anyhow!("{key} must be valid Unicode"))
                })
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Self::from_settings(Settings::new(&values))
    }

    fn from_settings(settings: Settings<'_>) -> Result<Self> {
        let defaults = Self::default();
        let bind = settings
            .nonempty("BACKEND_BIND")?
            .map(|value| value.parse())
            .transpose()
            .context("BACKEND_BIND must be an IP address")?
            .unwrap_or(defaults.bind);
        for key in [
            "BACKEND_PORT",
            "BACKEND_TCP_PORT",
            "BACKEND_UDP_PORT",
            "BACKEND_TLS_PORT",
            "BACKEND_QUIC_PORT",
        ] {
            settings.nonempty(key)?;
        }
        let port = settings.port("BACKEND_PORT", defaults.tcp_port)?;
        let tcp_port = settings.port("BACKEND_TCP_PORT", port)?;
        let udp_port = settings.port("BACKEND_UDP_PORT", port)?;
        let tls_port = settings.port("BACKEND_TLS_PORT", defaults.tls_port)?;
        let quic_port = settings.port("BACKEND_QUIC_PORT", defaults.quic_port)?;
        for name in ["MQTT", "COAP", "AMQP", "WS_MQTT", "WS_COAP", "WS_AMQP"] {
            ensure!(
                settings
                    .nonempty(&format!("BACKEND_{name}_PORT"))?
                    .is_none(),
                "{name} belongs on gateway ingress, not this backend"
            );
        }
        let mut builder = Self {
            bind,
            tcp_port,
            udp_port,
            tls_port,
            quic_port,
            tls_cert_file: settings.nonempty("BACKEND_TLS_CERT_FILE")?.map(Into::into),
            tls_key_file: settings.nonempty("BACKEND_TLS_KEY_FILE")?.map(Into::into),
            srt_passphrase: settings.nonempty("BACKEND_SRT_PASSPHRASE")?,
            instance_id: settings
                .nonempty("BACKEND_ID")?
                .unwrap_or(defaults.instance_id),
            ..Self::default()
        };
        ensure!(tcp_port != tls_port, "TCP and TLS must use different ports");
        ensure!(
            udp_port != quic_port,
            "UDP and QUIC must use different ports"
        );
        for &kind in TransportKind::ALL {
            if MANDATORY.contains(&kind) {
                continue;
            }
            let key = format!(
                "BACKEND_{}_PORT",
                kind.id().replace('-', "_").to_ascii_uppercase()
            );
            if settings.nonempty(&key)?.is_some() {
                let port = settings.port(&key, 1)?;
                builder.additional.push((kind, port));
            }
        }
        if let Some(passphrase) = &builder.srt_passphrase {
            srt_config(passphrase)?;
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        for name in [
            "DCCP",
            "SCTP",
            "UDPLITE",
            "WS_DCCP",
            "WS_SCTP",
            "WS_UDPLITE",
        ] {
            ensure!(
                settings
                    .nonempty(&format!("BACKEND_{name}_PORT"))?
                    .is_none(),
                "{name} is unavailable on this platform"
            );
        }
        Ok(builder)
    }

    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = id.into();
        self
    }

    pub fn bind(mut self, address: IpAddr) -> Self {
        self.bind = address;
        self
    }

    pub fn tcp(mut self, port: u16) -> Self {
        self.tcp_port = port;
        self
    }

    pub fn udp(mut self, port: u16) -> Self {
        self.udp_port = port;
        self
    }

    pub fn tls(mut self, port: u16) -> Self {
        self.tls_port = port;
        self
    }

    pub fn quic(mut self, port: u16) -> Self {
        self.quic_port = port;
        self
    }

    /// Adds a transport beyond TCP/TLS/UDP/QUIC; those use their dedicated methods.
    pub fn transport(mut self, kind: TransportKind, port: u16) -> Self {
        self.additional.push((kind, port));
        self
    }

    /// PEM identity shared by every TLS-based transport.
    pub fn tls_files(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.tls_cert_file = Some(cert.into());
        self.tls_key_file = Some(key.into());
        self
    }

    pub fn srt_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.srt_passphrase = Some(passphrase.into());
        self
    }

    /// Allows port zero (OS-assigned ports) for embedding and tests.
    pub fn allow_ephemeral_ports(mut self) -> Self {
        self.allow_ephemeral_ports = true;
        self
    }

    /// Registers extra host methods on the bare server before plugins are installed.
    pub fn configure(
        mut self,
        configure: impl FnOnce(&RpcServer) -> Result<()> + Send + 'static,
    ) -> Self {
        self.configure.push(Box::new(configure));
        self
    }

    /// Adds a plugin; it is installed during [`build`](Self::build), in the order added.
    pub fn plugin(mut self, plugin: impl ServerPlugin) -> Self {
        self.plugins.push(BoxedPlugin::new(plugin));
        self
    }

    /// Reads the plugin configuration from the process environment and adds the plugin.
    pub fn plugin_from_env<P: ServerPlugin + FromSettings>(self) -> Result<Self> {
        let plugin = P::from_env().with_context(|| format!("configuring {} plugin", P::NAME))?;
        Ok(self.plugin(plugin))
    }

    /// Installs a global `tracing` subscriber when the backend is built.
    ///
    /// `directive` is an `EnvFilter` expression such as `"info"` or
    /// `"info,json_rpc_2_backend=debug"`; a set `RUST_LOG` takes precedence.
    /// An already installed subscriber is left untouched.
    pub fn log_level(mut self, directive: impl Into<String>) -> Self {
        self.log_level = Some(directive.into());
        self
    }

    /// Validates the configuration, installs the plugins and prepares the transports.
    pub async fn build(mut self) -> Result<Backend> {
        if let Some(directive) = self.log_level.take() {
            init_logging(&directive)?;
        }
        let ports = [self.tcp_port, self.udp_port, self.tls_port, self.quic_port];
        ensure!(
            self.allow_ephemeral_ports || ports.iter().all(|&port| port > 0),
            "backend ports must be positive"
        );
        let identity = tls_identity(&self)?;
        // Fail on transport problems before any plugin opens a database or HTTP connection.
        self.assemble(
            (identity.0.clone(), identity.1.clone_key()),
            Arc::new(RpcServer::new()),
        )?;
        let rpc = RpcServer::new();
        install_base_methods(&rpc, &self.instance_id)?;
        document_base_methods(&rpc)?;
        for configure in std::mem::take(&mut self.configure) {
            configure(&rpc)?;
        }
        let mut maintenance = Vec::new();
        for plugin in std::mem::take(&mut self.plugins) {
            let name = plugin.name();
            let handle = plugin
                .install(&rpc)
                .await
                .with_context(|| format!("installing {name} plugin"))?;
            maintenance.extend_from_slice(handle.maintenance());
        }
        let server = self.assemble(identity, Arc::new(rpc))?;
        Ok(Backend {
            server,
            bind: self.bind,
            maintenance,
        })
    }

    fn assemble(
        &self,
        identity: TlsIdentity,
        protocol: Arc<dyn JsonRpcProtocol>,
    ) -> Result<MultiTransportServer> {
        let mut server = MultiTransportServer::new(Arc::new(GatewayBackendProtocol::new(protocol)));
        server
            .set_bind([self.bind])
            .set_tcp_port(self.tcp_port)
            .set_udp_port(self.udp_port)
            .set_tls_port(self.tls_port)
            .set_quic_port(self.quic_port)
            .set_tls(identity.0, identity.1);
        let mut configured = HashSet::new();
        for &(kind, port) in &self.additional {
            ensure!(configured.insert(kind), "duplicate {} listener", kind.id());
            ensure!(
                !MANDATORY.contains(&kind),
                "configure {} through its dedicated port method",
                kind.id()
            );
            server.set_port(kind, port);
        }
        if let Some(passphrase) = &self.srt_passphrase {
            server.set_srt_config(srt_config(passphrase)?);
        }
        server
            .validate()
            .context("invalid backend transport configuration")?;
        Ok(server)
    }
}

/// A fully configured backend, ready to listen.
pub struct Backend {
    server: MultiTransportServer,
    bind: IpAddr,
    maintenance: Vec<MaintenanceTask>,
}

impl Backend {
    /// Opens every listener and returns immediately.
    pub async fn start(&self) -> Result<RunningBackend> {
        let running = self
            .server
            .start()
            .await
            .context("starting backend transports")?;
        Ok(RunningBackend {
            running,
            bind: self.bind,
        })
    }

    /// Runs until SIGINT or SIGTERM (Ctrl+C on non-Unix targets), then closes gracefully.
    pub async fn run_until_signal(self) -> Result<()> {
        self.run(termination_signal()).await
    }

    /// Runs all listeners and periodic maintenance until `shutdown` completes.
    pub async fn run(self, shutdown: impl Future<Output = Result<()>>) -> Result<()> {
        let running = self.start().await?;
        for (transport, address) in running.listeners() {
            tracing::info!(transport = transport.id(), %address, "backend listening");
        }
        // Keep maintenance scoped to this future so shutdown/error cannot leak a spawned task.
        let result = {
            let maintenance = self.maintenance();
            tokio::pin!(maintenance);
            tokio::select! {
                result = shutdown => result,
                () = &mut maintenance => unreachable!("maintenance runs until shutdown"),
            }
        };
        let close = running.close().await;
        result.and(close)
    }

    async fn maintenance(&self) {
        let mut tasks = tokio::task::JoinSet::new();
        for task in &self.maintenance {
            let task = task.clone();
            tasks.spawn(async move {
                let mut interval = tokio::time::interval(task.interval());
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if let Err(error) = task.run().await {
                        tracing::warn!(%error, "plugin maintenance failed");
                    }
                }
            });
        }
        // Dropping the set aborts the tasks, so shutdown cannot leak them.
        std::future::pending::<()>().await;
    }
}

fn init_logging(directive: &str) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => EnvFilter::try_new(value).context("invalid RUST_LOG")?,
        Err(std::env::VarError::NotPresent) => EnvFilter::try_new(directive)
            .with_context(|| format!("invalid log level {directive:?}"))?,
        Err(error) => return Err(error.into()),
    };
    // A subscriber installed earlier (e.g. by the host or a previous backend) wins.
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    Ok(())
}

async fn termination_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

/// Open listeners of a started [`Backend`].
pub struct RunningBackend {
    running: RunningServer,
    bind: IpAddr,
}

impl RunningBackend {
    pub fn listeners(&self) -> impl Iterator<Item = (TransportKind, SocketAddr)> + '_ {
        self.running.listeners()
    }

    pub fn address(&self, kind: TransportKind) -> Option<SocketAddr> {
        self.running.address(kind, self.bind)
    }

    pub async fn close(self) -> Result<()> {
        self.running
            .close()
            .await
            .context("closing backend transports")
    }
}

fn install_base_methods(rpc: &RpcServer, instance_id: &str) -> Result<()> {
    rpc.register("health", |_| async {
        Ok(serde_json::json!({"status": "ok"}))
    })?;
    rpc.register("echo", |params| async move { Ok(params) })?;
    let id = instance_id.to_owned();
    rpc.register("identify", move |_| {
        let id = id.clone();
        async move { Ok(serde_json::json!(id)) }
    })?;
    Ok(())
}

fn document_base_methods(registry: &RpcServer) -> Result<()> {
    registry.document_method(
        "health",
        serde_json::json!({"type": "object"}),
        serde_json::json!({}),
    )?;
    registry.document_method(
        "echo",
        serde_json::json!({"type": ["object", "array"]}),
        serde_json::json!({"message": "example"}),
    )?;
    registry.document_method(
        "identify",
        serde_json::json!({"type": "object"}),
        serde_json::json!({}),
    )?;
    Ok(())
}

fn srt_config(passphrase: &str) -> Result<json_rpc_2_core::SrtConfig> {
    let config = json_rpc_2_core::SrtConfig {
        passphrase: passphrase.to_owned(),
    };
    config.validate().map_err(anyhow::Error::msg)?;
    Ok(config)
}

fn tls_identity(builder: &BackendBuilder) -> Result<TlsIdentity> {
    let cert_file = builder
        .tls_cert_file
        .as_ref()
        .context("BACKEND_TLS_CERT_FILE is required for TLS and QUIC")?;
    let key_file = builder
        .tls_key_file
        .as_ref()
        .context("BACKEND_TLS_KEY_FILE is required for TLS and QUIC")?;
    let certificates = CertificateDer::pem_file_iter(cert_file)
        .context("opening TLS certificate PEM file")?
        .collect::<Result<Vec<_>, _>>()
        .context("parsing TLS certificate chain")?;
    ensure!(
        !certificates.is_empty(),
        "TLS certificate chain must not be empty"
    );
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|_| anyhow::anyhow!("cannot read a private key from BACKEND_TLS_KEY_FILE"))?;
    // Validate the identity before any listener is opened.
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certificates.clone(), key.clone_key())
        .context("invalid TLS certificate/private key pair")?;
    Ok((certificates, key))
}

#[cfg(test)]
mod tests;
