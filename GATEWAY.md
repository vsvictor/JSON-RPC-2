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

## Binary wire contract (version 3)

All integers are unsigned, big-endian. Frames contain no delimiter or Base64:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic bytes `JRPC-GW\x03` |
| 8 | 1 | Kind: request=1, response=2, none=3, error=4, ping=5, pong=6 |
| 9 | 2 | Connection ID length |
| 11 | 2 | Source transport length |
| 13 | 4 | Opaque payload length |
| 17 | variable | Connection ID, source transport, original payload bytes |

Only requests carry metadata. Both metadata fields are 1..128 ASCII
alphanumeric/hyphen bytes. Responses carry only payload (which may be empty);
none/error/ping/pong have zero lengths. Readers validate the fixed header before
allocating or reading the rest, and require the exact declared frame size.

TCP/TLS recognize binary frames alongside existing newline JSON messages.
Binary frames have **no trailing newline** and support arbitrary bytes, including
newlines and invalid UTF-8. WebSocket uses binary frames for gateway envelopes;
ordinary JSON clients retain text frames. HTTP uses binary request/response
bodies with `application/octet-stream`. Datagrams and brokers carry binary
messages subject to their own size limits.

Use `RpcClientTransport::send_message(encode_request(&payload, id, transport)?, true)`
for every framed exchange. Consume the returned `Bytes` with `decode_response`
to retain the payload as a shared slice of the receive buffer.
Notifications receive a transport acknowledgement, so a gateway
does not need to inspect JSON-RPC IDs or wait indefinitely for a nonexistent
business response. Never retry an exchange after it has been sent: it may have
executed even if the acknowledgement was lost.

## Ownership and copy boundaries

`TransportMessage` owns a small `header: Bytes` and an independent
`payload: Bytes`. Encoding clones the payload handle, not its bytes; decoding
uses `Bytes::slice`. `JsonRpcProtocol::handle_owned_message` transfers ingress
buffer ownership into async dispatch. Borrowed protocol methods remain for
source compatibility, but consumers using them may need to copy.

TCP/TLS write the two segments without concatenation, using vectored writes
with correct partial-write handling. HTTP/1 emits separate body frames;
QUIC and HTTP/3 emit separate stream writes/data frames. Backend responses use
the same segmented contract. No payload-sized framing allocation is needed on
these outbound paths.

This is **not a universal kernel/network zero-copy guarantee**. TLS/QUIC
encryption, receive-buffer growth/reassembly, WebSocket masking and contiguous
frame APIs, datagram codecs, and broker libraries may allocate or copy.
Contiguous-only adapters explicitly call `TransportMessage::into_bytes` at
their boundary; they still avoid Base64 and its roughly 33% wire expansion.
The gateway's message bus, balancer codec, and owned dispatch do not copy
payload bytes. Business JSON parsing remains a backend responsibility.

The codec limit is `MAX_GATEWAY_PAYLOAD_BYTES` (16,776,943 bytes),
reserving 17 bytes for the header and 256 for metadata within the 16 MiB
message limit. Transport-specific limits may be smaller; errors are propagated.

Legacy `JRPC-GW/1` TCP/TLS metadata remains supported separately.
The Base64 `JRPC-GW/2` profile is retired, not silently negotiated or replayed.
Upgrade gateways and internal backends together; startup ping/pong rejects
incompatible backends before accepting ingress traffic.
