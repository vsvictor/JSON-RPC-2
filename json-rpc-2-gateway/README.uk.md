# json-rpc-2-gateway

[English](README.md) | **Українська**

Backend-адаптер wire-контракту JSON-RPC-2-GATEWAY v3. Він не залежить від застосунку:
обгорніть будь-який `JsonRpcProtocol` (наприклад, `RpcServer`) у
`GatewayBackendProtocol` і передайте його в `MultiTransportServer`.

```rust
use std::sync::Arc;
use json_rpc_2_gateway::GatewayBackendProtocol;

let protocol = Arc::new(GatewayBackendProtocol::new(Arc::new(rpc_server)));
```

- `GatewayBackendProtocol` розбирає кадри запитів (id з'єднання, тег транспорту, payload),
  передає payload з `TransportContext` і кодує кадри відповіді, відсутності відповіді
  (notification) та помилки. На `PING` відповідає `PONG`; звичайний JSON проходить без змін.
- `GatewayFraming` — кодек `StreamFraming` для потоків TCP/TLS.
