# json-rpc-2-callback

English | [Українською](README.uk.md)

Server-to-client callbacks as JSON-RPC 2.0 notifications. A backend never writes to client connections: it asks the gateway Control Plane (`GatewayControl`) to push to a registered client, so delivery is centralized.

- Backend: `register_client(connection_id, client_id)` (connection id from `TransportContext`), then `notify_client(client_id, method, params)`. Keep `GATEWAY_CONTROL_ADDR` (for example `gateway:8500`) on an internal network; the Control Plane is unauthenticated.
- Client: `CallbackListener::spawn(source, handler)` receives decoded `CallbackMessage`s and retries after a source error until stopped.
- Callbacks use the same transport as the direct connection. `json-rpc-2-ffi` implements `PushSource` for TCP, TLS (client-side push demultiplexer in `TcpClientTransport::recv_push` / `TlsClientTransport::recv_push`), QUIC, SRT and WebSocket-over-SRT. Other transports return `Unsupported`; `start_callbacks` then fails with `InvalidConfiguration`.
- Feature `quic` provides a `PushSource` for `QuicClientTransport`.

Test: `cargo test -p json-rpc-2-callback --features quic`.
