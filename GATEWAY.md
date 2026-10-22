# Gateway backend framing

Backend listeners built with `MultiTransportServer` can opt into opaque gateway
framing before `start()`:

```rust,no_run
let mut server = MultiTransportServer::new(rpc);
server.enable_gateway_framing();
server.set_bind(["127.0.0.1".parse()?]).set_tcp_port(8081);
let running = server.start().await?;
```

Only enable this on trusted internal listeners: the gateway supplies the client
identity. This is not an authentication mechanism. Ordinary JSON-RPC clients
continue to work without gateway framing.

The transport-independent `json_rpc_2_transport::gateway` module provides
`encode_request`, `decode_response`, `GatewayBackendProtocol`, and the gateway
ping/pong constants. `GatewayBackendProtocol` wraps any `JsonRpcProtocol` and
forwards session lifecycle callbacks. It decodes only the technical envelope,
never JSON-RPC. Business payload bytes are passed unchanged to the inner
protocol's `handle_message_with_context` with the gateway connection ID and
source transport. Client push must use the gateway Control Plane, not the
shared backend connection.

Wire messages are single-line ASCII, so the same contract works with newline
streams, WebSocket text frames, HTTP bodies, broker messages, and datagrams:

* Request: `JRPC-GW/2 <connection_id> <source_transport> <base64_payload>`
* Response: `JRPC-GW/2 RESPONSE <base64_payload>`
* Completed notification: `JRPC-GW/2 NONE`
* Invalid envelope or oversized response: `JRPC-GW/2 ERROR`
* Negotiation/liveness: `JRPC-GW/2 PING` -> `JRPC-GW/2 PONG`

Use `RpcClientTransport::send_with_expectation(frame, true)` for every framed
exchange. Notifications receive a transport acknowledgement, so a gateway
does not need to inspect JSON-RPC IDs or wait indefinitely for a nonexistent
business response. Never retry an exchange after it has been sent: it may have
executed even if the acknowledgement was lost.

Base64 introduces allocation and roughly 33% wire overhead; this compatibility
profile is not zero-copy. The codec limit is `MAX_GATEWAY_PAYLOAD_BYTES`
(12,582,528 bytes), allowing metadata and encoding within the 16 MiB message
limit. Individual carriers can impose smaller limits, including UDP packet
limits and broker payload limits; their errors are propagated.

Legacy `JRPC-GW/1` TCP/TLS metadata remains supported separately.
