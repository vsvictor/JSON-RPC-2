# json-rpc-2-backend

[English](README.md) | Українська

Мультитранспортний JSON-RPC-2 бекенд для JSON-RPC-2-GATEWAY з єдиним builder.
`BackendBuilder` — єдина точка входу: відкриває обов'язкові слухачі TCP, TLS, UDP і
QUIC (та будь-які інші транспорти JSON-RPC-2), працює з gateway v3 фреймінгом
(`json-rpc-2-gateway`), реєструє `health`/`echo`/`identify` і встановлює довільну
кількість `ServerPlugin` (`json-rpc-2-server`).

```rust
let backend = BackendBuilder::from_env()?   // змінні BACKEND_*
    .log_level("info")                      // RUST_LOG має пріоритет
    .plugin_from_env::<MyPluginConfig>()?   // ServerPlugin + FromSettings
    .configure(|rpc| rpc.register("ping", |_| async { Ok("pong".into()) }).map_err(Into::into))
    .build()
    .await?;
backend.run_until_signal().await?; // або run(ваш_future_завершення)
```

- `build()` перевіряє порти, TLS/QUIC-ідентичність і транспорти до того, як плагіни
  відкриють з'єднання з базою чи HTTP.
- Плагіни встановлюються в порядку додавання й можуть повертати періодичні задачі;
  `run` виконує їх до завершення роботи.
- `Backend::start()` повертає `RunningBackend` (`listeners`, `address`, `close`);
  `allow_ephemeral_ports()` дозволяє порт `0` (тести/вбудовування).

Написання плагіна:

```rust
impl FromSettings for MyPluginConfig {
    fn from_settings(settings: &Settings<'_>) -> anyhow::Result<Self> { /* ключі MY_* */ }
}

impl ServerPlugin for MyPluginConfig {
    const NAME: &'static str = "my-plugin";

    async fn install(self, server: &RpcServer) -> anyhow::Result<PluginHandle> {
        let service = Arc::new(MyService::connect(self).await?);
        server.register_with("my.method", service.clone(), |service, params| async move {
            service.handle(params).await
        })?;
        server.register_aliases("rpc.", &["my.method"])?;
        Ok(PluginHandle::new().with_maintenance(Duration::from_secs(60), service, |service| async move {
            service.cleanup().await
        }))
    }
}
```

`RpcServer` логує кожен невдалий виклик методу (`warn`, метод і код помилки), тож
плагінам не потрібна обгортка для логування.

Середовище: `BACKEND_BIND`, `BACKEND_ID`, `BACKEND_{TCP,UDP,TLS,QUIC,…}_PORT`,
`BACKEND_TLS_CERT_FILE`, `BACKEND_TLS_KEY_FILE`, `BACKEND_SRT_PASSPHRASE` (10..79 байт).
Порти MQTT/CoAP/AMQP відхиляються (лише вхід шлюзу).

Конфігурація читається через `json_rpc_2_server::Settings`, спільний із плагінами.
Backend відхиляє явно порожні значення `BACKEND_*` і некоректні порти;
ані він, ані `Settings` не завантажують файли `.env`.
