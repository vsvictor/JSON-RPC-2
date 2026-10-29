# json-rpc-2-backend

English | [Українська](README.uk.md)

A single-builder, multi-transport JSON-RPC-2 backend for JSON-RPC-2-GATEWAY.
`BackendBuilder` is the only entry point: it opens the mandatory TCP, TLS, UDP and
QUIC listeners (plus any other JSON-RPC-2 transport), speaks the gateway v3 framing
(`json-rpc-2-gateway`), registers `health`/`echo`/`identify`, and installs any number
of `ServerPlugin`s (`json-rpc-2-server`).

```rust
let backend = BackendBuilder::from_env()?   // BACKEND_* variables
    .plugin(MyPluginConfig::from_env()?)    // any ServerPlugin
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

Environment: `BACKEND_BIND`, `BACKEND_ID`, `BACKEND_{TCP,UDP,TLS,QUIC,…}_PORT`,
`BACKEND_TLS_CERT_FILE`, `BACKEND_TLS_KEY_FILE`, `BACKEND_SRT_PASSPHRASE` (10..79 bytes).
MQTT/CoAP/AMQP ports are rejected (gateway ingress only).
