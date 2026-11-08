# json-rpc-2-callback

[English](README.md) | Українською

Виклики від сервера до клієнта як сповіщення JSON-RPC 2.0. Бекенд ніколи не пише в з’єднання клієнтів: він просить Control Plane шлюзу (`GatewayControl`) надіслати повідомлення зареєстрованому клієнту, тож доставка централізована.

- Бекенд: `register_client(connection_id, client_id)` (id з’єднання з `TransportContext`), далі `notify_client(client_id, method, params)`. Тримайте `GATEWAY_CONTROL_ADDR` (наприклад `gateway:8500`) у внутрішній мережі; Control Plane без автентифікації.
- Клієнт: `CallbackListener::spawn(source, handler)` отримує декодовані `CallbackMessage` і повторює спробу після помилки джерела, доки його не зупинено.
- Колбеки використовують той самий транспорт, що й пряме з'єднання. `json-rpc-2-ffi` реалізує `PushSource` для TCP, TLS (клієнтський демультиплексор push у `TcpClientTransport::recv_push` / `TlsClientTransport::recv_push`), QUIC, SRT і WebSocket-over-SRT. Решта транспортів повертають `Unsupported`; `start_callbacks` тоді завершується `InvalidConfiguration`.
- Feature `quic` дає `PushSource` для `QuicClientTransport`.

Тест: `cargo test -p json-rpc-2-callback --features quic`.
