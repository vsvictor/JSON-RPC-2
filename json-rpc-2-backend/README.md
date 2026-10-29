# json-rpc-2-backend

English | [Українська](README.uk.md)

A single-builder, multi-transport JSON-RPC-2 backend for JSON-RPC-2-GATEWAY.
`BackendBuilder` is the only entry point: it opens the mandatory TCP, TLS, UDP and
QUIC listeners (plus any other JSON-RPC-2 transport), speaks the gateway v3 framing
(`json-rpc-2-gateway`), registers `health`/`echo`/`identify`, and installs any number
of `ServerPlugin`s (`json-rpc-2-server`).

```rust
let backend = BackendBuilder::from_dotenv()?   // loads .env, then BACKEND_* variables
    .log_level("info")                      // RUST_LOG takes precedence
    .plugin_from_env::<MyPluginConfig>()?   // ServerPlugin + FromSettings
    .configure(|rpc| rpc.register("ping", |_| async { Ok("pong".into()) }).map_err(Into::into))
    .build()
    .await?;
backend.run_until_signal().await?; // or run(your_shutdown_future)
```

- `build()` validates ports, the TLS/QUIC identity and transports before any plugin
  opens a database or HTTP connection.
- Plugins are installed in the order added and may return periodic maintenance tasks;
  `run` drives them until shutdown.
- `Backend::start()` returns a `RunningBackend` (`listeners`, `address`, `close`);
  `allow_ephemeral_ports()` permits port `0` (tests/embedding).

Writing a plugin:

```rust
impl FromSettings for MyPluginConfig {
    fn from_settings(settings: &Settings<'_>) -> anyhow::Result<Self> { /* MY_* keys */ }
}

impl ServerPlugin for MyPluginConfig {
    const NAME: &'static str = "my-plugin";

    async fn install(self, server: &RpcServer) -> anyhow::Result<PluginHandle> {
        let service = Arc::new(MyService::connect(self).await?);
        server.register_typed("my.method", service.clone(), |service, params: MyParams| async move {
            service.handle(params).await
        })?;
        server.register_aliases("rpc.", &["my.method"])?;
        Ok(PluginHandle::new().with_maintenance(Duration::from_secs(60), service, |service| async move {
            service.cleanup().await
        }))
    }
}
```

`RpcServer` logs every failed method call (`warn`, method and error code), so plugins
need no logging wrapper.

`register_typed` deserializes params into a `Deserialize + JsonSchema` type
(any mismatch is `-32602 Invalid params`) and documents the method with its schema
and `#[schemars(example = ...)]`. `NonBlank`, `NonNull` and `keep_null` cover common
field rules; `parse_params` / `document_params` are available when a handler must
inspect raw params first. `Settings::duration_secs` / `duration_millis` and
`RateLimitConfig::from_prefix(settings, "MY_RATE")` read common plugin settings.
`default_instance_id` sets an id unless `BACKEND_ID` or `instance_id` chose one.

Environment: `BACKEND_BIND`, `BACKEND_ID`, `BACKEND_{TCP,UDP,TLS,QUIC,…}_PORT`,
`BACKEND_TLS_CERT_FILE`, `BACKEND_TLS_KEY_FILE`, `BACKEND_SRT_PASSPHRASE` (10..79 bytes).
MQTT/CoAP/AMQP ports are rejected (gateway ingress only).

Configuration parsing uses `json_rpc_2_server::Settings`, shared with plugins.
The backend rejects explicitly blank `BACKEND_*` values and invalid ports;
only `BackendBuilder::from_dotenv` loads `.env` (existing variables win).
