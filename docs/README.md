# Документація JSON-RPC-2

Compatibility facade over the split workspace crates:

- `json-rpc-2-core`: JSON-RPC 2.0 wire types, shared protocol constants,
  and transport wire codecs without a network runtime
- `json-rpc-2-transport`: client/server transport contracts, runtime helpers,
  metrics, multiplexing, and socket tuning
- `json-rpc-2-client`: transport-agnostic client session runtime
- `json-rpc-2-config`: typed environment / CLI config loader for the example runtime
- `json-rpc-2-datagram`: non-WebSocket datagram/message-oriented transports
- `json-rpc-2-iot`: client-only MQTT, CoAP, and AMQP transports
- `json-rpc-2-quic`: non-WebSocket QUIC/HTTP3 transports
- `json-rpc-2-server`: transport-agnostic dispatcher and typestate builder
- `json-rpc-2-runtime`: multi-transport server orchestration and health endpoints
- `json-rpc-2-tcp`: non-WebSocket TCP/TLS/HTTP1 transports
- `json-rpc-2-websockets`: all WebSocket-family transports and shared WebSocket
  adapter surface
- `json-rpc-2-cli`: extracted demo / CLI binary (`JSON_RPC_2`)

This crate re-exports those crates so existing code can keep depending on
`json-rpc-2` while the implementation lives in the split architecture.

Документація проєкту зібрана в цьому каталозі:

- [Implementation coverage of todos](IMPLEMENTATION-TODOS.md)
- [Glossary](GLOSSARY.md)
- [Каталог транспортів: короткі описи](TRANSPORT-CATALOG.md)
- [Transport semantics](TRANSPORTS.md)
- [Standards conformance and interoperability](CONFORMANCE.md)
- [Retry and deduplication guarantees](RETRY-SEMANTICS.md)
- [Архітектурна схема](ARCHITECTURE.md)
- [Native SRT: збирання та протокол](SRT.md)
- [MQTT/AMQP broker integration](BROKER-INTEGRATION.md)
- [FFI для Kotlin та Swift](FFI.md)

**Compatibility scope:** MQTT 3.1.1 and AMQP 0-9-1 use external brokers;
the RPC server is a subscriber/consumer, not an embedded broker. SRT uses
the native libsrt stack. JSON-RPC routing/framing remains an application
binding, and datagram/broker WebSocket adapters use a private message envelope,
not RFC 6455 over UDP or MQTT-over-WebSocket. See the conformance matrix before
connecting an external peer.

## Build the workspace

From the repository root, run `tools/build-all.sh` (or
`tools/build-all.sh --release`). This builds every workspace crate and target
with all features supported by the target platform, including the FFI libraries
and UniFFI binding generator. Tests and benches are compiled but not executed;
the separate cargo-fuzz project is not a workspace member and is excluded.
Additional Cargo build options are forwarded unchanged.

The script builds and caches pinned Haivision libsrt with OpenSSL encryption
using `tools/build-srt.sh`; native prerequisites are listed in
[SRT build documentation](SRT.md).

## Client certificate trust

For QUIC, HTTP/3, and their WebSocket variants, configure the server certificate
before connecting. `json_rpc_2::client::ClientTlsConfig` provides
`set_ca_certificate_pem(&str)` and `set_ca_certificate_file(path)`.
These replace the trusted PEM bundle after validation; invalid input preserves
the previous trust. Use `endpoint(local_address, alpn)` to create the endpoint
passed to the transport's existing `connect` method. Select `QUIC_ALPN`,
`H3_ALPN`, or `WS_QUIC_ALPN` for the corresponding transport (WebSocket/HTTP3
also uses `H3_ALPN`). Endpoint creation requires a Tokio runtime.
Server-name verification remains enabled. Trust changes apply to new endpoints,
not established connections. Supply only public certificates, never the server key.
For TLS/TCP, use `json_rpc_2::client::transports::tls::TlsClientTransport::connect(
address, server_name, ca_pem)`; it performs the verified TLS handshake before sending
newline-delimited JSON-RPC.

```rust,ignore
use json_rpc_2::{QUIC_ALPN, client::ClientTlsConfig};
use json_rpc_2::client::transports::quic::QuicClientTransport;

let mut tls = ClientTlsConfig::default();
tls.set_ca_certificate_file("certs/cert.pem")?;
let endpoint = tls.endpoint("0.0.0.0:0".parse()?, QUIC_ALPN)?;
let transport = QuicClientTransport::connect(endpoint, address, "mobilespace.local").await?;
```

## Architecture highlights

- **Transport ownership**: TCP, datagram, and QUIC family crates own their
  client and server adapters; `json-rpc-2-iot` owns only IoT clients. IoT
  server adapters and broker ingress lifecycle are owned by the separate
  `gateway-iot` crate. WebSocket clients remain in the JSON-RPC-2 workspace;
  IoT WebSocket server adapters belong to `gateway-iot`.
- **Explicit limits**: `ClientLimits` and `ServerLimits` distinguish the
  client queue/in-flight budget from server connection/request limits.
  Legacy `TransportLimits` aliases remain available in the corresponding APIs.
- **Architecture checks**: `python3 tools/check-architecture.py` rejects
  forbidden production dependencies, including networking dependencies in core.
  CI checks Linux/macOS, feature combinations, wire profiles, and supported
  public facade paths.
- **Immutable protocol core**: `RpcServer` now dispatches through an immutable
  method registry optimized for read-heavy runtime traffic; registrations remain
  copy-on-write and are intended for startup-time configuration.
- **Actor-based client session**: `RpcClient` now runs through a bounded
  outbound session queue with tracked pending calls, per-request timeout
  enforcement, cancellation cleanup, shared metrics, and transport-aware
  cancellation hooks for multiplexed QUIC and HTTP/3 streams.
- **Structured telemetry**: library crates emit `tracing` events instead of
  `println!`/`eprintln!`, with correlation metadata for transport, peer,
  method, request id, latency, and remote error code when present.
  Run `RUST_LOG=debug tools/server-all.sh --bind 0.0.0.0` to include full
  JSON-RPC request/response payloads, notifications, and control events.
  Payload events identify received requests and outgoing responses; outgoing
  responses are logged before transport delivery, not as delivery confirmations.
  These logs can contain sensitive application data; use `RUST_LOG=info`
  to suppress debug payload logging.
- **Config objects**: client/server policies are grouped into dedicated config
  types rather than growing unbounded lists of ad-hoc setters.
- **Typed transport capabilities**: transport kinds expose structured
  capability metadata (`TransportCapabilitySet`, `TransportMultiplexing`,
  `TransportSecurity`, `TransportSocket`) so carrier/TLS/ordering semantics are
  explicit in the type model.
- **Shared WebSocket abstraction**: transport-neutral WebSocket message,
  protocol, and datagram-frame types now live in `json-rpc-2-core`, while
  concrete carriers only implement session establishment and delivery.
- **Kernel-aware TCP tuning**: TCP-based transports can opt into
  `TCP_NODELAY`, explicit congestion-control selection (`system` or `bbr`),
  and TCP Fast Open on supported operating systems.

### Direct-crate migration

The facade keeps `MultiTransportServer`, `RunningServer`,
`server::multi`, and transport-family paths available. Consumers importing
implementation crates directly need these changes:

| Previous direct owner | Current owner |
|---|---|
| `json-rpc-2-core` transport contracts, runtime helpers, capability model, control constants, TCP tuning | `json-rpc-2-transport` |
| `json-rpc-2-server::MultiTransportServer`, `RunningServer`, `TransportKind`, health/orchestration API | `json-rpc-2-runtime` |
| `json-rpc-2-server` WebSocket adapter reexports | `json-rpc-2-websockets` |

`json-rpc-2-server` now builds the dispatcher without concrete transport
dependencies. Legacy client/server transport feature names are compatibility
no-ops; enable transport features on the facade or runtime/family crates instead.
`TransportLimits` aliases are retained for compatibility, but new code should
use explicit `ClientLimits` / `ServerLimits`.

## Transports

The JSON-RPC-2 workspace contains client and server adapters for:

- TCP
- TLS-encrypted TCP
- reliable UDP
- RTP/RTCP over UDP
- RUDP
- SRT (native libsrt)
- UDP-Lite (Linux/Android only)
- DCCP
- SCTP
- raw QUIC
- HTTP/1.1
- HTTP/3
- WebSocket over TCP
- WebSocket over UDP
- WebSocket over RTP/RTCP
- WebSocket over RUDP
- WebSocket-style messages over SRT
- WebSocket over UDP-Lite (Linux/Android only)
- WebSocket over DCCP
- WebSocket over SCTP
- WebSocket over QUIC
- WebSocket over HTTP/1.1 Upgrade
- WebSocket over HTTP/3 extended CONNECT

MQTT, CoAP, AMQP, and their WebSocket-style server adapters are provided by
`gateway-iot`; the JSON-RPC-2 workspace retains only their client adapters.

The server side keeps the JSON-RPC protocol separate from message delivery.
Transport implementations fan out independent stream sessions through a shared
`StreamMultiplexor`, while `RpcServer` owns only JSON-RPC parsing, dispatching,
batch handling, and error generation.

### Feature flags

Transport families can now be compiled out when not needed:

- `tcp`
- `tls`
- `udp`
- `rtp`
- `rudp`
- `srt`
- `udplite`
- `dccp`
- `sctp`
- `mqtt`
- `coap`
- `amqp`
- `quic`
- `http1`
- `http3`
- `websocket`

The facade crate forwards those feature flags to the split transport-family
crates (`json-rpc-2-tcp`, `json-rpc-2-datagram`, `json-rpc-2-iot`,
`json-rpc-2-quic`, `json-rpc-2-websockets`) plus the shared client/server
cores, and the FFI crate mirrors the same transport groups.

## Liveness and reconnects

Transport-level liveness checks stay separate from JSON-RPC application
traffic:

- TCP, TLS/TCP, reliable UDP, and raw QUIC use reserved non-JSON control payloads.
- RTP/RTCP uses RTCP APP control packets for ACK/ping/pong on the same UDP socket.
- RUDP and UDP-Lite use dedicated datagram control packets for ACK/ping/pong;
  UDP-Lite can reduce checksum coverage to keep partially corrupted payloads
  visible to the application.
- SRT delegates handshake, encryption, congestion control, and retransmission
  to libsrt; its JSON-RPC binding carries separate application liveness messages.
- DCCP uses the same reserved non-JSON control payloads as raw TCP/QUIC.
- MQTT and AMQP reuse their own protocol control frames (MQTT `PINGREQ`/`PINGRESP`, AMQP heartbeat frames) for liveness.
- CoAP uses empty confirmable messages as pings, acknowledged the same way as any other CoAP request.
- HTTP/1.1 and HTTP/3 use `GET /.well-known/json-rpc-2/ping`.
- WebSocket transports use native Ping/Pong control frames.

`RpcClient::set_ping_frequency(frequency_ms)` enables periodic transport-level
keepalive pings; `0` disables them. `RpcClient::set_max_missed_pongs(count)`
controls how many missed pong ticks trigger recovery. When that threshold is
reached, the client first calls `restore()` on the current transport and then
falls back to `reconnect()` if restore fails.

`RpcClient::set_request_timeout(_)/set_request_timeout_millis(_)` now control a
real JSON-RPC request timeout, independent of transport liveness.

`ReconnectPolicy` also supports initial reconnect cooldown, exponential
backoff, additive jitter and an optional maximum elapsed reconnect budget for a
single failure streak.

## TCP tuning

TCP-based transports share a typed `TcpTuningConfig`:

- `no_delay` enables `TCP_NODELAY` by default for lower per-call latency.
- `congestion_control` keeps the system default or requests `bbr` on
  Linux/Android kernels that expose it.
- `fast_open` enables TCP Fast Open where the OS and the specific transport
  adapter support it.

This remains opt-in rather than automatic:

- **BBR** is deployment-specific and only meaningful when the target kernel
  exposes it.
- **TCP Fast Open** is enabled for TCP listeners on
  Linux/Android/macOS/iOS in this crate; client-side Fast Open is available for
  raw TCP and HTTP/1.1 connect paths on Linux/Android. Unsupported explicit
  requests fail with a clear socket error instead of silently falling back.

## Reliable UDP transport

Reliable UDP is implemented as an application-level message transport rather
than as a byte stream:

- one JSON-RPC message maps to one datagram, bounded by
  `UdpReliabilityConfig::max_packet_size`;
- requests are retried with ACK/response-driven retransmission policy;
- the server deduplicates per-peer packet ids and replays cached responses for
  duplicates;
- oversized messages fail explicitly because fragmentation/reassembly is not
  part of the current contract.

## RTP/RTCP transport

RTP/RTCP is implemented as a JSON-RPC datagram transport on top of UDP:

- RTP data packets carry one JSON-RPC request or response each, including RTP
  sequence numbers, timestamps, and SSRC values.
- RTCP APP packets carry transport-level ACK, ping, and pong control traffic.
- The transport reuses `UdpReliabilityConfig` for retransmit timing,
  deduplication, and packet-size limits, so reliability policy stays consistent
  across the UDP-family transports.
- As with reliable UDP, one JSON-RPC message must fit into one datagram and
  oversized messages fail explicitly.

## RUDP transport

RUDP is implemented as a distinct reliable datagram transport beside the base
reliable UDP adapter:

- RUDP packets carry explicit packet sequence numbers and per-exchange packet
  ids in their own wire format.
- Reliability policy still comes from `UdpReliabilityConfig`, so retransmit
  timing, deduplication TTL, and packet-size bounds stay aligned with the rest
  of the UDP-family transports.
- One JSON-RPC message still maps to one datagram; fragmentation/reassembly is
  intentionally out of scope for the current contract.

## SRT transport

SRT uses the native Haivision libsrt library, not a private encrypted UDP codec:

- caller/listener sessions perform the standard SRT handshake;
- libsrt owns ARQ, congestion control, and passphrase-based encryption;
- `srt` and `ws-srt` use `SrtConfig`; the reliable-UDP application cache is not
  an SRT delivery guarantee;
- build hosts need encrypted libsrt >= 1.5, pkg-config, and libclang for
  bindings (`brew install srt pkgconf` plus Xcode command-line tools on macOS,
  or `apt install libsrt-openssl-dev pkg-config libclang-dev` on Debian/Ubuntu);
- passphrases must contain 10–79 bytes; this adapter enforces AES-256 encryption
  and rejects a missing or mismatched passphrase rather than falling back
  to cleartext.

The previous `JSRT` wire format is not accepted as a standard SRT connection.
JSON-RPC message framing over SRT remains project-defined: an arbitrary SRT
video sender is not automatically a JSON-RPC peer.

## IoT client transports and server ownership

MQTT and AMQP client endpoints connect to an **existing broker**. Configure the
same request topic/queue for the RPC client and gateway service. Replies are
routed to per-client destinations with correlation metadata, including when
multiple clients reuse the same JSON-RPC ID.

Use `MqttConfig` / `AmqpConfig` for client broker authentication, routing, TLS,
and operation deadlines, then call the client transport's
`connect_with_config`. `json-rpc-2-iot` exports client transports only.
CoAP, MQTT, AMQP, and WebSocket-style IoT server adapters, broker session
handling, and ingress lifecycle belong to the separate `gateway-iot` crate.

Server configuration and lifecycle are provided by the gateway workspace: configure `GatewayIotIngress` from `gateway-iot`, not `MultiTransportServer`.

Client and server must use matching `request_topic` / `request_queue` values.
`MultiTransportServer` rejects IoT server transports rather than silently
starting a partial service.

These APIs intentionally change the old private-profile topology. Port `0`
does not create a local MQTT or AMQP broker. Broker acknowledgements and
reliability do not imply exactly-once execution of application side effects.

The CLI client accepts `--mqtt-url`, `--mqtt-topic`, `--mqtt-ws-topic`,
`--amqp-url`, `--amqp-queue`, and `--amqp-ws-queue`; credentials belong in the
broker URL. `mqtts://` and `amqps://` enable verified TLS; `--mqtt-ca` accepts
an additional PEM CA bundle for MQTT. Operation and response deadlines have
separate `--mqtt-*-timeout-ms` / `--amqp-*-timeout-ms` options. The equivalent
environment variables are listed by `--help`. The CLI server does not host IoT
transports; use `gateway-iot` for IoT ingress.
TCP_NODELAY/BBR/Fast Open settings apply only to adapter-owned TCP/HTTP1
sockets, not to broker-library sockets.

## UDP-Lite transport

The workspace also ships a UDP-Lite transport for partially checksummed
datagrams on Linux/Android:

- it keeps the same application-level retry, deduplication, and packet-size
  policy as the other UDP-family transports;
- socket coverage is configured through `UdpLiteConfig`, where `0` means full
  packet coverage and values `>= 8` request partial coverage;
- `udplite` and `ws-udplite` are Linux/Android-only because they depend on
  kernel UDP-Lite socket support.

## Ordering semantics

- **TCP / HTTP/1.1 / WebSocket over TCP or HTTP/1.1** serialize request/response
  handling on one logical session; completion order follows transport order.
- **Reliable UDP / RTP/RTCP / RUDP / UDP-Lite** are datagram-based and may complete **out of
  order**; replies are correlated by transport message id and JSON-RPC `id`.
- **DCCP / WebSocket over DCCP** keep datagram boundaries and congestion
  control, but the current implementation keeps one in-flight exchange per
  connected session, so completion order is still serialized per session.
- **SCTP / WebSocket over SCTP** preserve message boundaries on one connected
  association. The current adapter uses one-to-one `SOCK_STREAM` with `MSG_EOR`
  reassembly and keeps the door
  open for explicit stream-selection policy later; capability-wise these
  transports are modeled as multiplexed because the carrier supports
  multi-streaming.
- **WebSocket over UDP** keeps WebSocket-style text/binary message framing on
  top of the same reliable UDP carrier, so completion may also be **out of
  order** and is still correlated by JSON-RPC `id`.
- **WebSocket over RTP/RTCP** keeps the same WebSocket-style text/binary/close
  framing, but carries it inside RTP data packets with RTCP ACK/ping/pong
  control traffic; completion may also be **out of order** and is still
  correlated by JSON-RPC `id`.
- **WebSocket over RUDP / UDP-Lite** keep the same dedicated WebSocket
  text/binary/close framing while switching the underlying datagram carrier to
  RUDP or UDP-Lite; completion may also be
  **out of order** and is still correlated by JSON-RPC `id`.
- **SRT / WebSocket over SRT** preserve native SRT message ordering within a
  connection, but execute independent RPC handlers concurrently and correlate
  responses that may complete out of order.
- **MQTT / AMQP** route replies to a per-client destination and correlate
  exchanges independently of broker connection arrival order.
- **QUIC / HTTP/3 / WebSocket over QUIC or HTTP/3** allow independent streams,
  so concurrent RPC calls may complete **out of order** even when issued in
  order.
- **Batch responses** are correlated by `id`, not by array position. The client
  preserves caller order when returning `BatchResponse` values.

## Metrics and health surface

The client exposes a metrics snapshot with active requests, queue depth,
timeouts, cancellations, reconnect attempts, and missed pong streaks. The
server exposes dispatcher metrics plus runtime transport metrics for active
sessions/streams. `RunningServer::health_handle(Arc<RpcServer>)` now builds an
optional external health surface with:

- `snapshot()` for in-process polling
- `json()` for lightweight exporters
- `prometheus()` for Prometheus text exposition
- `router()` (with `http1`) exposing `/healthz`, `/metrics.json`, and `/metrics`

## Multi-transport server

`MultiTransportServer` exposes one `JsonRpcProtocol` implementation over every
enabled transport. Socket transports listen on each bind address. IoT broker
consumers are outside this runtime and are managed by `gateway-iot`. The
preferred entry point is
`MultiTransportServer::builder(protocol)`, which uses typestate to require at
least one enabled transport and either a socket bind or broker configuration
before `build()` or `start()` are available. Configure it with `set_bind`, one port setter per
transport (`set_tcp_port`, `set_udp_port`, `set_rtp_port`, `set_rudp_port`,
`set_srt_port`, `set_dccp_port`, `set_sctp_port`, `set_quic_port`,
`set_http1_port`, `set_http3_port`, `set_ws_tcp_port`, `set_ws_udp_port`,
`set_ws_rtp_port`, `set_ws_rudp_port`, `set_ws_srt_port`,
`set_udplite_port`, `set_ws_udplite_port`,
`set_ws_dccp_port`, `set_ws_sctp_port`, `set_ws_quic_port`,
`set_ws_http1_port`, `set_ws_http3_port`), `set_tls` for QUIC-based
transports, `set_tcp_tuning` for TCP socket policy, `set_udp_reliability` for
UDP-family retry/dedup policy, `set_udplite_config` for UDP-Lite checksum
coverage, and `set_srt_config` for SRT passphrase policy. `start()` validates
the final configuration and returns a `RunningServer` with graceful shutdown.
`listeners()` reports local socket listeners.
UDP-Lite, DCCP, and SCTP transports currently require Linux or Android kernel
support.

## Validation and fuzzing

- External broker contracts are explicit ignored integration tests, not
  successful no-op tests when no broker exists:

  ```sh
  export JSON_RPC_TEST_MQTT_URL=mqtt://127.0.0.1:1883
  export JSON_RPC_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:5672/%2f
  cargo test -p json-rpc-2-iot --test broker_rpc -- --ignored
  cargo test -p json-rpc-2-iot --test broker_sessions -- --ignored
  cargo test -p json-rpc-2-iot --test broker_auth -- --ignored
  cargo test -p json-rpc-2-websockets --no-default-features --features websocket,mqtt,amqp --test broker_sessions -- --ignored
  ```

- `tools/check-features.sh` runs the supported feature-flag matrix for the
  facade and FFI crates.
- `json-rpc-2-server/fuzz/` contains `cargo fuzz` targets for raw wire payloads
  and structured batch/id edge cases plus transport framing coverage for TCP,
  UDP, HTTP/1.1, WebSocket message boundaries, and QUIC control separation.

## Benchmarks and interop

- `cargo bench -p json-rpc-2-server` measures single-call and batch dispatch.
- `cargo bench -p json-rpc-2-client` measures request hot-path overhead.
- `json-rpc-2-server/tests/interop_external.rs` covers HTTP/JSON-RPC interop
  against external client tooling plus raw wire-level HTTP checks.
