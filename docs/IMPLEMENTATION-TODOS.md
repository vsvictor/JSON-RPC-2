# Implementation coverage of session todos

This file documents how the items tracked in the session `todos` table are
implemented in the workspace. It is meant to answer **what was done**, **where
it lives**, and **why it matters**.

Status notation:

- **done** — implemented and wired into the current workspace

## 1. Workspace split and architectural refactor

| Todo ID | Status | What it means in the codebase |
|---|---|---|
| `move-code` | done | Client/server logic was moved out of the old monolith into split crates: `json-rpc-2-core`, `json-rpc-2-client`, `json-rpc-2-server`, with `json-rpc-2` kept as the facade crate. Transport families are additionally grouped into `json-rpc-2-tcp`, `json-rpc-2-datagram`, `json-rpc-2-quic`, and `json-rpc-2-websockets`. |
| `new-patterns` | done | The refactor adopted typestate builders, explicit transport abstractions, shared multiplexing for stream-style transports, and the added `ws-tcp` / `ws-quic` transport variants. |
| `split-crates` | done | The workspace now has separate crates for core protocol types, client runtime, server runtime, config, CLI, facade, and FFI. |
| `split-tests` | done | Tests are no longer concentrated in one place: client, server, property, black-box e2e, interop, fuzz, and benchmark coverage are split by concern and crate. |
| `update-consumers` | done | The root demo/runtime responsibilities were migrated into `json-rpc-2-cli`, and FFI bindings were updated to the split architecture and expanded transport surface. |
| `validate-refactor` | done | Validation now includes full workspace tests, feature-matrix compilation, black-box transport tests, and fuzz target compilation checks. |

## 2. Client/session model, cancellation, limits, and runtime policy

| Todo ID | Status | What it means in the codebase |
|---|---|---|
| `client-actor-session` | done | `RpcClient` uses an actor/session model with a bounded outbound queue, a pending-response table, request timeouts, cancellation cleanup, and shared metrics. |
| `timeouts-cancellation` | done | Request timeouts are first-class API, pending entries are removed on drop/cancel/shutdown, and QUIC/HTTP3 propagate cancellation down to dedicated transport streams. |
| `client-session-redesign` | done | The actor/session design is the permanent client runtime model now; it owns dispatch, in-flight accounting, cancellation, and transport recovery orchestration. |
| `server-limits-config` | done | Client and server limits are represented with typed config objects; server-side connection limits and request concurrency limits are explicit, not implicit. |
| `reconnect-backoff-policy` | done | `ReconnectPolicy` now supports initial cooldown, exponential backoff, additive jitter, and an optional max elapsed reconnect budget. |
| `typed-config-layer` | done | Environment and CLI parsing/validation live in `json-rpc-2-config`, so the example runtime no longer duplicates config rules ad hoc. |

### Main implementation points

- `json-rpc-2-client/src/lib.rs`
  - `RpcClientConfig`
  - `ClientLivenessConfig`
  - `ReconnectPolicy`
  - `ClientLimits` (`TransportLimits` compatibility alias)
  - actor/session loop (`run_session`)
- `json-rpc-2-config/src/lib.rs`
  - env loading with overlay support
  - typed command parsing
  - validation of reconnect and timeout settings

## 3. Server-core immutability, dispatch, and transport modeling

| Todo ID | Status | What it means in the codebase |
|---|---|---|
| `immutable-server-core` | done | The server dispatch table is runtime-immutable and optimized for read-heavy execution. Method registration remains startup-oriented and copy-on-write. |
| `server-config-immutability` | done | Server construction enforces a clearer startup/build phase versus runtime/serve phase. This keeps runtime behavior simpler and safer. |
| `parallel-batch-dispatch` | done | Server-side JSON-RPC batch items are executed concurrently, and responses are correlated by `id` rather than by preserving array position. |
| `bytes-server-hot-path` | done | The server protocol path now returns `Bytes` and serializes through `BytesMut`, reducing copies compared to `Vec<u8>`-only flow. |
| `transport-capabilities` | done | Transport kinds expose explicit structured capabilities: multiplexing mode, TLS/plain security, socket family, and ping support. |

### Main implementation points

- `json-rpc-2-server/src/lib.rs`
  - immutable dispatch map
  - concurrent batch handling
  - `Bytes` response path
- `json-rpc-2-transport/src/lib.rs`
  - `TransportCapabilitySet`
  - `TransportMultiplexing`
  - `TransportSecurity`
  - `TransportSocket`
- `json-rpc-2-runtime/src/multi.rs`
  - `TransportKind` capability mapping
  - capability-based validation

## 4. Logging, metrics, health, and observability

| Todo ID | Status | What it means in the codebase |
|---|---|---|
| `structured-logging-metrics` | done | Library crates emit `tracing` events with correlation metadata such as transport, peer, method, request id, latency, and error code. |
| `logging-metrics` | done | The runtime exposes client/server metrics snapshots and structured transport logs instead of ad hoc `println!` in library code. |
| `metrics-exporters` | done | Health data is available not only as in-process snapshot/JSON, but also through a Prometheus text exporter (`prometheus()` and `/metrics`). |

### Main implementation points

- `json-rpc-2-client/src/lib.rs`
  - `ClientMetricsSnapshot`
- `json-rpc-2-server/src/lib.rs`
  - `ServerMetricsSnapshot`
- `json-rpc-2-runtime/src/multi.rs`
  - `HealthHandle::snapshot()`
  - `HealthHandle::json()`
  - `HealthHandle::prometheus()`
  - `/healthz`, `/metrics.json`, `/metrics`

## 5. Transport set, feature flags, and FFI

| Todo ID | Status | What it means in the codebase |
|---|---|---|
| `transport-feature-flags` | done | Transport families are feature-gated across facade, client, server, and FFI crates to reduce compile time and dependency surface. |
| `reliable-udp-transport` | done | A reliable UDP transport was added with typed retry/dedup config, client retransmission logic, server-side duplicate suppression, CLI/config wiring, and FFI exposure. |
| `rtp-transports` | done | RTP/RTCP over UDP was added as a separate datagram transport with explicit RTP headers, RTCP control packets, shared retry/dedup policy, builder/config wiring, tests, and FFI exposure. |
| `rudp-transports` | done | RUDP was added as a separate reliable datagram transport with its own packet format, explicit sequencing, shared retry/dedup policy, builder/config wiring, and FFI exposure. |
| `srt-transports` | done | Native libsrt caller/listener transport owns standard handshake, ARQ, congestion control, and encryption; JSON-RPC framing, runtime/config wiring, and FFI exposure remain separate. |
| `udplite-transports` | done | UDP-Lite was added as a separate datagram transport with checksum-coverage config, shared retry/dedup policy, builder/config wiring, Linux/Android socket support, and FFI exposure. |
| `websocket-over-udp` | done | WebSocket-style framing now also runs over the reliable UDP carrier through dedicated client/server transports, server builder wiring, config/env support, tests, and FFI entry points. |
| `websocket-over-rtp` | done | WebSocket-style framing now also runs over the RTP/RTCP carrier through dedicated client/server transports, shared WebSocket crate wiring, config/env support, tests, and FFI entry points. |
| `websocket-over-rudp` | done | WebSocket-style framing now also runs over the RUDP carrier through dedicated client/server transports, shared WebSocket crate wiring, config/env support, tests, and FFI entry points. |
| `websocket-over-srt` | done | The dedicated WebSocket-style envelope runs over standard SRT message delivery, not the former private JSRT carrier. |
| `websocket-over-udplite` | done | WebSocket-style framing now also runs over the UDP-Lite carrier through dedicated client/server transports, shared WebSocket crate wiring, config/env support, Linux/Android socket support, tests, and FFI entry points. |
| `dccp-transports` | done | Raw DCCP and WebSocket-over-DCCP transports were added with platform-aware socket support, builder/config/env wiring, tests, docs, tools, and FFI exposure on Linux/Android. |
| `sctp-transports` | done | Raw SCTP and WebSocket-over-SCTP transports were added with platform-aware socket support, builder/config/env wiring, tests, docs, tools, and FFI exposure on Linux/Android. |
| `iot-transports` | done | `json-rpc-2-iot` owns MQTT 3.1.1 and AMQP 0-9-1 clients and the CoAP client adapter; all IoT server adapters and ingress lifecycle are owned by the separate `gateway-iot` crate. |
| `websocket-over-iot` | done | WebSocket-style IoT clients remain in the JSON-RPC-2 workspace; WebSocket-style server adapters and broker ingress lifecycle live in `gateway-iot`. |
| `ffi-config-redesign` | done | FFI now exposes clearer transport-specific connection options and explicit typed connect entry points. |
| `docs-contracts-ffi` | done | FFI and transport semantics are documented alongside the richer runtime contracts and feature model. |

### Main implementation points

- `json-rpc-2-ffi/src/lib.rs`
  - explicit connect methods
  - typed option records
  - transport capability mapping on the FFI side
- `json-rpc-2-core/src/udp_reliable.rs`
  - reliable UDP packet/header format
  - `UdpReliabilityConfig`
- `json-rpc-2-core/src/rtp.rs`
  - RTP packet/header format for JSON-RPC data
  - RTCP APP control packet format for ACK/ping/pong
- `json-rpc-2-core/src/rudp.rs`
  - RUDP packet/header format for JSON-RPC data and control traffic
- `json-rpc-2-core/src/srt.rs`
  - SRT configuration; legacy private packet helpers are not used by native SRT
- `json-rpc-2-core/src/udp_lite.rs`
  - UDP-Lite checksum-coverage config shared across runtime/config/FFI layers
- `json-rpc-2-core/src/mqtt.rs`
  - legacy wire helpers, not the standard broker transport implementation
- `json-rpc-2-core/src/coap.rs`
  - CoAP header/message wire format for JSON-RPC payloads
- `json-rpc-2-core/src/amqp.rs`
  - legacy wire helpers, not the standard broker transport implementation
- `json-rpc-2-datagram/src/client/transports/udp.rs`
  - retry / ACK / ping-pong client transport
- `json-rpc-2-datagram/src/client/transports/rtp.rs`
  - RTP request/response client transport with RTCP control handling
- `json-rpc-2-datagram/src/client/transports/rudp.rs`
  - RUDP request/response client transport with explicit sequence numbers
- `json-rpc-2-datagram/src/client/transports/srt.rs`
  - native libsrt request/response caller
- `json-rpc-2-datagram/src/client/transports/udplite.rs`
  - UDP-Lite request/response client transport with configurable checksum coverage
- `json-rpc-2-iot/src/client/transports/mqtt.rs`
  - MQTT 3.1.1 request/reply client using an external broker
- `json-rpc-2-iot/src/client/transports/coap.rs`
  - CoAP request/response client transport over UDP
- `json-rpc-2-iot/src/client/transports/amqp.rs`
  - AMQP 0-9-1 request/reply client using an external broker
- `json-rpc-2-datagram/src/server/transports/udp.rs`
  - deduplication and cached-response replay
- `json-rpc-2-datagram/src/server/transports/rtp.rs`
  - RTP server transport with RTCP ACK/ping/pong handling
- `json-rpc-2-datagram/src/server/transports/rudp.rs`
  - RUDP server transport with deduplication and cached-response replay
- `json-rpc-2-datagram/src/server/transports/srt.rs`
  - standard libsrt listener and JSON-RPC message dispatch
- `json-rpc-2-datagram/src/server/transports/udplite.rs`
  - UDP-Lite server transport with deduplication and cached-response replay
- `gateway-iot/src/server/transports/mqtt.rs`
  - MQTT broker subscriber with request/reply topic routing
- `gateway-iot/src/server/transports/coap.rs`
  - CoAP server transport with request/response correlation over UDP
- `gateway-iot/src/server/transports/amqp.rs`
  - AMQP broker consumer with queue routing and response correlation
- `json-rpc-2-websockets/src/client/transports/udp.rs`
  - WebSocket-style framing over reliable UDP on the client
- `json-rpc-2-websockets/src/server/transports/udp.rs`
  - WebSocket-style framing over reliable UDP on the server
- `json-rpc-2-websockets/src/server/transports/rtp.rs`
  - WebSocket-style framing over RTP/RTCP on the server
- `json-rpc-2-websockets/src/server/transports/rudp.rs`
  - WebSocket-style framing over RUDP on the server
- `json-rpc-2-websockets/src/server/transports/srt.rs`
  - WebSocket-style framing over standard libsrt messages on the server
- `json-rpc-2-websockets/src/server/transports/udplite.rs`
  - WebSocket-style framing over UDP-Lite on the server
- `gateway-iot/src/server/transports/mqtt_websocket.rs`
  - WebSocket-style framing over MQTT on the server
- `gateway-iot/src/server/transports/coap_websocket.rs`
  - WebSocket-style framing over CoAP on the server
- `gateway-iot/src/server/transports/amqp_websocket.rs`
  - WebSocket-style framing over AMQP on the server
- `gateway-iot/src/server/mod.rs`
  - gateway-owned IoT ingress lifecycle and broker adapter startup/shutdown
- `json-rpc-2-iot/src/lib.rs`
  - client-only IoT transport family
- `json-rpc-2-websockets/src/client/transports/rudp.rs`
  - public WebSocket-over-RUDP client wrapper
- `json-rpc-2-websockets/src/client/transports/srt.rs`
  - public WebSocket-over-SRT client wrapper
- `json-rpc-2-websockets/src/client/transports/udplite.rs`
  - public WebSocket-over-UDP-Lite client wrapper
- `tools/check-features.sh`
  - supported feature matrix compilation

## 13. RTP/RTCP transport

Recent follow-up work added an RTP/RTCP transport beside the existing reliable
UDP adapter.

- Shared packet/wire contract: `json-rpc-2-core/src/rtp.rs`
- Server transport: `json-rpc-2-datagram/src/server/transports/rtp.rs`
- Client transport: `json-rpc-2-datagram/src/client/transports/rtp.rs`
- Multi-transport server wiring:
  - `TransportKind::Rtp`
  - `MultiTransportServer::set_rtp_port(...)`
- Config/env wiring in `json-rpc-2-config`:
  - transport id `rtp`
  - `JSON_RPC_RTP_PORT`
- FFI wiring in `json-rpc-2-ffi`:
  - `FfiTransportKind::Rtp`
  - `RtpConnectOptions`
  - `connect_rtp(...)`

Why it matters:

- it adds RTP sequence numbers, timestamps, and SSRC metadata without mixing
  transport framing into the JSON-RPC protocol layer;
- it keeps control traffic explicit through RTCP APP packets for ACK/ping/pong;
- it reuses the same bounded retry/dedup policy as the reliable UDP transport,
  so operational tuning stays consistent across the UDP-family transports.

## 6. Testing, fuzzing, interop, and performance measurement

| Todo ID | Status | What it means in the codebase |
|---|---|---|
| `testing-hardening` | done | Property tests, black-box e2e matrix tests, reconnect/shutdown tests, and fuzz targets were added to harden behavior. |
| `tests-docs` | done | Ordering, transport semantics, and runtime behavior are documented separately from the JSON-RPC protocol core. |
| `benchmark-suite` | done | `criterion` benchmarks were added for client and server hot paths, including server batch dispatch. |
| `external-interop-tests` | done | External-client interop coverage was added for HTTP/JSON-RPC using `jsonrpsee` against the project’s HTTP/1.1 transport. |
| `transport-framing-fuzz` | done | Fuzzing now targets not only protocol semantics but also transport framing boundaries for TCP, UDP, HTTP/1.1, WebSocket, and QUIC control traffic separation. |

### Main implementation points

- `json-rpc-2-server/tests/property.rs`
- `json-rpc-2-runtime/tests/server.rs`
- `json-rpc-2-runtime/tests/e2e_matrix.rs`
- `json-rpc-2-server/tests/interop_external.rs`
- `json-rpc-2-server/fuzz/`
- `json-rpc-2-server/benches/rpc_dispatch.rs`
- `json-rpc-2-client/benches/client_hot_path.rs`

## 7. Example/runtime extraction

| Todo ID | Status | What it means in the codebase |
|---|---|---|
| `extract-cli-example` | done | The old root `src/main.rs` responsibilities were moved into `json-rpc-2-cli`, so the root of the repository is a workspace container rather than an overloaded application crate. |

### Main implementation points

- `json-rpc-2-cli/src/main.rs`
- `json-rpc-2-config/src/lib.rs`
- workspace root `Cargo.toml`

## 8. Documentation map

Use these files together:

- `README.md` — high-level architecture, feature surface, metrics, benchmarks
- `TRANSPORTS.md` — transport semantics, framing, ordering, reconnect/shutdown behavior
- `GLOSSARY.md` — definitions of the architecture and runtime terms used across the project
- `../json-rpc-2-ffi/README.md` — FFI-specific API/documentation

## 9. Documentation rule used for this project

For todo-backed implementation work, the expected documentation shape is now:

1. update the high-level README when user-visible behavior changes;
2. update transport/runtime docs when protocol-carrier behavior changes;
3. add or update a glossary when new architectural terms appear;
4. ensure each tracked todo item is represented in documentation, not only in code or commit history.

## 10. TCP tuning and kernel-level socket policy

Recent follow-up work added explicit TCP tuning support rather than relying on
the operating-system defaults implicitly.

- Shared config type: `json-rpc-2-transport::TcpTuningConfig`
- Congestion profile enum: `json-rpc-2-transport::TcpCongestionControl`
- Server wiring: `MultiTransportServer::set_tcp_tuning(...)`
- Config/env wiring: `json-rpc-2-config` parses
  `JSON_RPC_TCP_NODELAY`, `JSON_RPC_TCP_CONGESTION`,
  `JSON_RPC_TCP_FASTOPEN`, and `JSON_RPC_TCP_FASTOPEN_QUEUE_LEN`
- Client wiring:
  - `TcpClientTransport::connect_with_tuning(...)`
  - `Http1ClientTransport::connect_with_tuning(...)`
  - `TcpWebSocketClientTransport::connect_with_tuning(...)`
  - `Http1WebSocketClientTransport::connect_with_tuning(...)`

Why it matters:

- `TCP_NODELAY` is now an explicit policy instead of an incidental default.
- BBR can be requested intentionally on supported Linux/Android kernels.
- TCP Fast Open can be enabled on supported listeners and on supported direct
  client connect paths, while unsupported combinations fail explicitly instead
  of silently pretending to apply the setting.

## 11. Reliable UDP transport

Recent follow-up work also added a dedicated UDP transport with application-level
reliability rather than relying on TCP-style streams.

- Shared packet/wire contract: `json-rpc-2-core/src/udp_reliable.rs`
- Server transport: `json-rpc-2-datagram/src/server/transports/udp.rs`
- Client transport: `json-rpc-2-datagram/src/client/transports/udp.rs`
- Multi-transport server wiring:
  - `TransportKind::Udp`
  - `MultiTransportServer::set_udp_port(...)`
  - `MultiTransportServer::set_udp_reliability(...)`
- Config/env wiring in `json-rpc-2-config`:
  - `JSON_RPC_UDP_PORT`
  - `JSON_RPC_UDP_RETRANSMIT_MS`
  - `JSON_RPC_UDP_MAX_RETRANSMITS`
  - `JSON_RPC_UDP_MAX_PACKET_SIZE`
  - `JSON_RPC_UDP_DEDUP_CACHE_CAPACITY`
  - `JSON_RPC_UDP_DEDUP_CACHE_TTL_MS`
- FFI wiring in `json-rpc-2-ffi`:
  - `FfiTransportKind::Udp`
  - `UdpConnectOptions`
  - `connect_udp(...)`

Why it matters:

- it adds a low-handshake, datagram-oriented transport for high-load scenarios;
- reliability is explicit and bounded through ACK/retry policy;
- duplicate requests are suppressed safely on the server;
- transport semantics stay distinct from the transport-agnostic JSON-RPC core.

## 12. WebSocket over UDP

On top of the reliable UDP carrier, the workspace now also ships a dedicated
WebSocket-style framing layer for applications that want text/binary message
semantics without moving to TCP or QUIC.

- Shared payload framing:
  - `json-rpc-2-core::WebSocketFrame`
  - `json-rpc-2-core::WebSocketFrameKind`
- Client transport:
  - `json-rpc-2-client::transports::websocket::udp::UdpWebSocketClientTransport`
- Server transport:
  - `json-rpc-2-server::transports::websocket::udp::UdpWebSocketTransport`
- Multi-transport builder/runtime:
  - `TransportKind::WebSocketUdp`
  - `set_ws_udp_port(...)`
- Config/env:
  - transport id `ws-udp`
  - `JSON_RPC_WS_UDP_PORT`
- FFI:
  - `FfiTransportKind::WsUdp`
  - `connect_ws_udp(...)`

Why it matters:

- it keeps the abstract WebSocket/session layer separated from the carrier;
- it reuses the existing UDP ACK/retry/dedup behavior rather than duplicating
  reliability logic;
- it makes the transport matrix more consistent: TCP, UDP, QUIC, HTTP, and
  WebSocket families can now all be exercised through the same multi-transport
  runtime.

## 14. WebSocket over RTP/RTCP

On top of the RTP/RTCP carrier, the workspace now also ships a dedicated
WebSocket-style framing layer.

- Client transport:
  - `json-rpc-2-client::transports::websocket::rtp::RtpWebSocketClientTransport`
- Server transport:
  - `json-rpc-2-server::transports::websocket::rtp::RtpWebSocketTransport`
- Grouped public surface:
  - `json-rpc-2-websockets::client::transports::rtp`
  - `json-rpc-2-websockets::server::transports::rtp`
- Multi-transport builder/runtime:
  - `TransportKind::WebSocketRtp`
  - `set_ws_rtp_port(...)`
- Config/env:
  - transport id `ws-rtp`
  - `JSON_RPC_WS_RTP_PORT`
- FFI:
  - `FfiTransportKind::WsRtp`
  - `connect_ws_rtp(...)`

Why it matters:

- it keeps the abstract WebSocket layer separated from the RTP/RTCP wire model;
- it reuses the RTP data + RTCP control carrier instead of inventing another
  WebSocket-specific reliability scheme;
- it extends the dedicated `json-rpc-2-websockets` crate so every WebSocket
  carrier stays grouped in one public API surface.

## 13. DCCP and WebSocket over DCCP

The workspace now also ships DCCP-based transports for platforms whose kernels
expose DCCP sockets (currently Linux/Android).

- Client transports:
  - `json-rpc-2-client::transports::dccp::DccpClientTransport`
  - `json-rpc-2-client::transports::websocket::dccp::DccpWebSocketClientTransport`
- Server transports:
  - `json-rpc-2-server::transports::dccp::DccpTransport`
  - `json-rpc-2-server::transports::websocket::dccp::DccpWebSocketTransport`
- Multi-transport builder/runtime:
  - `TransportKind::Dccp`
  - `TransportKind::WebSocketDccp`
  - `set_dccp_port(...)`
  - `set_ws_dccp_port(...)`
- Config/env:
  - transport ids `dccp`, `ws-dccp`
  - `JSON_RPC_DCCP_PORT`
  - `JSON_RPC_WS_DCCP_PORT`
- FFI:
  - `FfiTransportKind::Dccp`
  - `FfiTransportKind::WsDccp`
  - `DccpConnectOptions`
  - `connect_dccp(...)`
  - `connect_ws_dccp(...)`

Why it matters:

- it expands the transport matrix with a datagram carrier that keeps congestion
  control without imposing stream semantics;
- it preserves the existing protocol/transport split by implementing DCCP only
  as transport adapters below the abstract JSON-RPC core;
- it keeps unsupported platforms explicit by surfacing a clear unsupported
  error instead of silently pretending DCCP exists everywhere.

## 14. SCTP and WebSocket over SCTP

The workspace now also ships SCTP-based transports for platforms whose kernels
expose SCTP sockets (currently Linux/Android).

- Client transports:
  - `json-rpc-2-client::transports::sctp::SctpClientTransport`
  - `json-rpc-2-client::transports::websocket::sctp::SctpWebSocketClientTransport`
- Server transports:
  - `json-rpc-2-server::transports::sctp::SctpTransport`
  - `json-rpc-2-server::transports::websocket::sctp::SctpWebSocketTransport`
- Multi-transport builder/runtime:
  - `TransportKind::Sctp`
  - `TransportKind::WebSocketSctp`
  - `set_sctp_port(...)`
  - `set_ws_sctp_port(...)`
- Config/env:
  - transport ids `sctp`, `ws-sctp`
  - `JSON_RPC_SCTP_PORT`
  - `JSON_RPC_WS_SCTP_PORT`
- FFI:
  - `FfiTransportKind::Sctp`
  - `FfiTransportKind::WsSctp`
  - `SctpConnectOptions`
  - `connect_sctp(...)`
  - `connect_ws_sctp(...)`

Why it matters:

- it adds a carrier with built-in message boundaries and a path toward explicit
  multi-stream policies;
- it keeps the protocol/transport split intact by implementing SCTP only in
  transport adapters below the abstract JSON-RPC core;
- it keeps unsupported platforms explicit by surfacing a clear unsupported
  error instead of silently pretending SCTP exists everywhere.

## 15. Shared WebSocket abstraction

The workspace now treats WebSocket the same way it treats JSON-RPC transport:
the protocol/session contract is abstract, while each concrete carrier only
implements establishment and delivery details.

- Core abstraction:
  - `json-rpc-2-core::WebSocketMessage`
  - `json-rpc-2-core::WebSocketProtocol`
  - `json-rpc-2-core::WebSocketFrame`
  - `json-rpc-2-core::encode_websocket_message(...)`
  - `json-rpc-2-core::decode_websocket_message(...)`
- Server adapters:
  - `json-rpc-2-server::transports::websocket::JsonRpcWebSocket`
  - shared datagram bridge via `protocol_adapter(...)`
- Client adapters:
  - `json-rpc-2-client::transports::websocket::WebSocketClientTransport<_>`
  - `json-rpc-2-client::transports::websocket::DatagramWebSocketClientTransport<_>`

Why it matters:

- it removes duplicated WebSocket text/binary/close framing logic from UDP,
  DCCP, and SCTP adapters;
- it keeps the abstract WebSocket contract in the shared core instead of
  scattering equivalent types across server and client crates;
- it makes future WebSocket carriers cheaper to add because they can reuse the
  same protocol and datagram framing layer;
- public WebSocket-family reexports are now grouped in the dedicated
  `json-rpc-2-websockets` crate, so the TCP/datagram/QUIC family crates only
  expose their non-WebSocket transports.
