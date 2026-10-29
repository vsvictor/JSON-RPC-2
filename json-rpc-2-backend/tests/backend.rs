use std::path::PathBuf;
use std::{sync::Arc, time::Duration};

use json_rpc_2_backend::BackendBuilder;
use json_rpc_2_backend::{PluginHandle, ServerPlugin};
use json_rpc_2_runtime::TransportKind;
use json_rpc_2_server::RpcServer;
use rustls::pki_types::CertificateDer;

struct PingPlugin;

impl ServerPlugin for PingPlugin {
    const NAME: &'static str = "ping";

    async fn install(self, server: &RpcServer) -> anyhow::Result<PluginHandle> {
        server.register("ping_plugin", |_params| async {
            Ok(serde_json::json!("pong"))
        })?;
        Ok(PluginHandle::new())
    }
}

fn transport_config() -> (
    BackendBuilder,
    PathBuf,
    PathBuf,
    tempfile::TempDir,
    CertificateDer<'static>,
) {
    let directory = tempfile::tempdir().unwrap();
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_file = directory.path().join("cert.pem");
    let key_file = directory.path().join("key.pem");
    std::fs::write(&cert_file, identity.cert.pem()).unwrap();
    std::fs::write(&key_file, identity.key_pair.serialize_pem()).unwrap();
    (
        BackendBuilder::new()
            .allow_ephemeral_ports()
            .tcp(0)
            .udp(0)
            .tls(0)
            .quic(0)
            .tls_files(&cert_file, &key_file),
        cert_file,
        key_file,
        directory,
        identity.cert.into(),
    )
}

#[tokio::test]
async fn invalid_transport_identity_and_port_conflicts_fail_explicitly() {
    let error = BackendBuilder::new().build().await.err().unwrap();
    assert!(error.to_string().contains("BACKEND_TLS_CERT_FILE"));
    let (_, cert_path, key_path, _directory, _) = transport_config();
    let error = BackendBuilder::new()
        .tls_files(&cert_path, "")
        .build()
        .await
        .err()
        .unwrap();
    assert!(!error.to_string().is_empty());
    let valid = || {
        BackendBuilder::new()
            .allow_ephemeral_ports()
            .tcp(0)
            .udp(0)
            .tls(0)
            .quic(0)
            .tls_files(&cert_path, &key_path)
    };
    assert!(valid().tcp(8081).tls(8081).build().await.is_err());
    assert!(valid().udp(8081).quic(8081).build().await.is_err());
    assert!(valid().build().await.is_ok());
    assert!(
        valid()
            .transport(TransportKind::Http1, 0)
            .transport(TransportKind::Http1, 0)
            .build()
            .await
            .is_err()
    );
    assert!(
        valid()
            .transport(TransportKind::Srt, 0)
            .build()
            .await
            .is_err()
    );
    assert!(
        valid()
            .transport(TransportKind::Tcp, 0)
            .build()
            .await
            .is_err()
    );
    assert!(
        BackendBuilder::new()
            .tcp(0)
            .tls_files(&cert_path, &key_path)
            .build()
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("positive")
    );

    let different = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(&key_path, different.key_pair.serialize_pem()).unwrap();
    assert!(valid().build().await.is_err());
    std::fs::write(&key_path, "invalid-private-key").unwrap();
    let error = valid().build().await.err().unwrap();
    assert!(!format!("{error:#}").contains("invalid-private-key"));
    std::fs::write(&cert_path, "").unwrap();
    let error = valid().build().await.err().unwrap();
    assert!(error.to_string().contains("empty"));
}

#[tokio::test]
async fn tcp_tls_and_quic_serve_the_same_dispatcher() {
    use json_rpc_2_quic::client::transports::quic::QuicClientTransport;
    use json_rpc_2_tcp::client::transports::{tcp::TcpClientTransport, tls::TlsClientTransport};
    use json_rpc_2_transport::{QUIC_ALPN, RpcClientTransport, TcpTuningConfig};

    tokio::time::timeout(Duration::from_secs(15), async {
        let (builder, _, _, _directory, certificate) = transport_config();
        let backend = test_backend(builder).await;
        let running = backend.start().await.unwrap();
        assert_eq!(running.listeners().count(), 4);
        let tcp = TcpClientTransport::connect(running.address(TransportKind::Tcp).unwrap())
            .await
            .unwrap();
        let tls = TlsClientTransport::connect_with_certificates(
            running.address(TransportKind::Tls).unwrap(),
            "localhost",
            std::slice::from_ref(&certificate),
            TcpTuningConfig::default(),
        )
        .await
        .unwrap();

        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate.clone()).unwrap();
        let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls_config.alpn_protocols = vec![QUIC_ALPN.to_vec()];
        let mut endpoint =
            quinn::Endpoint::client((std::net::Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls_config).unwrap(),
        )));
        let quic = QuicClientTransport::connect(
            endpoint,
            running.address(TransportKind::Quic).unwrap(),
            "localhost",
        )
        .await
        .unwrap();

        for transport in [&tcp as &dyn RpcClientTransport, &tls, &quic] {
            let response = transport
                .send(br#"{"jsonrpc":"2.0","id":1,"method":"health"}"#.to_vec().into())
                .await
                .unwrap()
                .unwrap();
            let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(response["result"], serde_json::json!({"status": "ok"}));
            transport.close().await.unwrap();
        }
        // Certificate verification is mandatory: an unrelated trust root must fail.
        let unrelated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        assert!(
            TlsClientTransport::connect_with_certificates(
                running.address(TransportKind::Tls).unwrap(),
                "localhost",
                &[unrelated.cert.into()],
                TcpTuningConfig::default(),
            )
            .await
            .is_err()
        );
        running.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_port_is_rejected_before_plugin_connections() {
    let result = BackendBuilder::new().tcp(0).build().await;
    assert!(result.err().unwrap().to_string().contains("positive"));
}

#[tokio::test]
async fn tcp_backend_responds_and_shuts_down() {
    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpStream,
    };

    let (builder, _, _, _tls_directory, _) = transport_config();
    let (backend, _plugin_directory) = test_backend_with_dir(builder).await;
    let running = backend.start().await.unwrap();
    assert_eq!(running.listeners().count(), 4);
    let address = running.address(TransportKind::Tcp).unwrap();
    let stream = TcpStream::connect(address).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    {
        writer
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"rpc.list_methods\",\"params\":{}}\n",
            )
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        let names = response["result"].as_array().unwrap();
        assert!(names.contains(&json!("health")));
        assert!(names.contains(&json!("echo")));
        assert!(names.contains(&json!("ping_plugin")));
    }
    for (method, params, expected) in [
        ("health", json!({}), json!({"status": "ok"})),
        ("echo", json!({"test": true}), json!({"test": true})),
        ("identify", json!({}), json!("json-rpc-2-backend")),
    ] {
        writer
            .write_all(
                format!(
                    "{}\n",
                    json!({
                        "jsonrpc": "2.0", "id": 1, "method": method, "params": params
                    })
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["result"], expected);
    }
    for (method, expected) in [("rpc.getShift", -32601)] {
        writer
            .write_all(
                format!(
                    "{}\n",
                    json!({
                        "jsonrpc": "2.0", "id": 2, "method": method, "params": {}
                    })
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["error"]["code"], expected);
    }
    drop(writer);
    drop(reader);
    tokio::time::timeout(Duration::from_secs(5), running.close())
        .await
        .unwrap()
        .unwrap();
}

async fn test_backend(builder: BackendBuilder) -> json_rpc_2_backend::Backend {
    test_backend_with_dir(builder).await.0
}

/// Plugin configuration lives in a temporary directory that must outlive the backend.
async fn test_backend_with_dir(
    builder: BackendBuilder,
) -> (json_rpc_2_backend::Backend, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let builder = builder.plugin(PingPlugin);
    (builder.build().await.unwrap(), directory)
}

#[tokio::test]
async fn invalid_log_level_is_rejected_and_valid_is_accepted() {
    // RUST_LOG would override the directive, so this test only runs without it.
    if std::env::var_os("RUST_LOG").is_some() {
        return;
    }
    let error = BackendBuilder::new().log_level("info,=[").build().await;
    assert!(error.err().unwrap().to_string().contains("log level"));
    let (builder, _, _, _tls_directory, _) = transport_config();
    assert!(builder.log_level("warn").build().await.is_ok());
}

struct NeedsKey;

impl json_rpc_2_backend::FromSettings for NeedsKey {
    fn from_settings(settings: &json_rpc_2_server::Settings<'_>) -> anyhow::Result<Self> {
        settings.required("JSON_RPC_2_BACKEND_TEST_UNSET_KEY")?;
        Ok(Self)
    }
}

impl ServerPlugin for NeedsKey {
    const NAME: &'static str = "needs-key";

    async fn install(self, _server: &RpcServer) -> anyhow::Result<PluginHandle> {
        Ok(PluginHandle::new())
    }
}

#[test]
fn plugin_from_env_names_the_plugin_on_configuration_errors() {
    let error = BackendBuilder::new()
        .plugin_from_env::<NeedsKey>()
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("configuring needs-key plugin"));
    assert!(format!("{error:#}").contains("JSON_RPC_2_BACKEND_TEST_UNSET_KEY is required"));
}
