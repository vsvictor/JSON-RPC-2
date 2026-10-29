# json-rpc-2-gateway

**English** | [Українська](README.uk.md)

Backend-side adapter for the JSON-RPC-2-GATEWAY v3 wire contract. It is independent of
any application: wrap any `JsonRpcProtocol` (for example `RpcServer`) in
`GatewayBackendProtocol` and give it to `MultiTransportServer`.

```rust
use std::sync::Arc;
use json_rpc_2_gateway::GatewayBackendProtocol;

let protocol = Arc::new(GatewayBackendProtocol::new(Arc::new(rpc_server)));
```

- `GatewayBackendProtocol` decodes request frames (connection id, transport tag, payload),
  dispatches the payload with a `TransportContext`, and encodes response, no-response
  (notification) and error frames. `PING` is answered with `PONG`; plain JSON passes through.
- `GatewayFraming` is the `StreamFraming` codec for TCP/TLS streams.
