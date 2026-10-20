# json-rpc-2

Compatibility facade over the split workspace crates:

- `json-rpc-2-core`: JSON-RPC 2.0 wire types and shared protocol constants
- `json-rpc-2-client`: transport-agnostic client plus transport adapters
- `json-rpc-2-config`: typed environment / CLI config loader for the example runtime
- `json-rpc-2-datagram`: non-WebSocket datagram/message-oriented transports
- `json-rpc-2-iot`: non-WebSocket IoT transports (MQTT, CoAP, AMQP)
- `json-rpc-2-quic`: non-WebSocket QUIC/HTTP3 transports
- `json-rpc-2-server`: transport-agnostic server, typestate dispatcher builder,
  multi-transport runtime, and transport adapters
- `json-rpc-2-tcp`: non-WebSocket TCP/HTTP1 transports
- `json-rpc-2-websockets`: all WebSocket-family transports and shared WebSocket
  adapter surface
- `json-rpc-2-cli`: extracted demo / CLI binary (`JSON_RPC_2`)

This crate re-exports those crates so existing code can keep depending on
`json-rpc-2` while the implementation lives in the split architecture.

Additional project documentation:

- [Implementation coverage of todos](IMPLEMENTATION-TODOS.md)
- [Glossary](GLOSSARY.md)
- [Transport semantics](TRANSPORTS.md)

## Architecture highlights

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

## Transports

Client and server adapters cover:

- TCP
- reliable UDP
- RTP/RTCP over UDP
- RUDP
- SRT-style encrypted reliable UDP
- UDP-Lite (Linux/Android only)
- DCCP
- SCTP
- MQTT (over TCP)
- CoAP (over UDP)
- AMQP (over TCP)
- raw QUIC
- HTTP/1.1
- HTTP/3
- WebSocket over TCP
- WebSocket over UDP
- WebSocket over RTP/RTCP
- WebSocket over RUDP
- WebSocket over SRT-style UDP
- WebSocket over UDP-Lite (Linux/Android only)
- WebSocket over DCCP
- WebSocket over SCTP
- WebSocket over MQTT
- WebSocket over CoAP
- WebSocket over AMQP
- WebSocket over QUIC
- WebSocket over HTTP/1.1 Upgrade
- WebSocket over HTTP/3 extended CONNECT

The server side keeps the JSON-RPC protocol separate from message delivery.
Transport implementations fan out independent stream sessions through a shared
`StreamMultiplexor`, while `RpcServer` owns only JSON-RPC parsing, dispatching,
batch handling, and error generation.

### Feature flags

Transport families can now be compiled out when not needed:

- `tcp`
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

- TCP, reliable UDP, and raw QUIC use reserved non-JSON control payloads.
- RTP/RTCP uses RTCP APP control packets for ACK/ping/pong on the same UDP socket.
- RUDP, SRT, and UDP-Lite use dedicated datagram control packets for ACK/ping/pong; SRT additionally encrypts payloads with a shared passphrase, while UDP-Lite can reduce checksum coverage to keep partially corrupted payloads visible to the application.
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

## SRT-style transport

The workspace also ships an SRT-style UDP transport optimized for reliable
delivery over unstable links:

- the carrier keeps message boundaries and retry/dedup behavior from the
  UDP-family model;
- payloads are encrypted with a shared-passphrase `SrtConfig` before they are
  put on the wire;
- `srt` and `ws-srt` require explicit SRT configuration in addition to the
  usual UDP reliability policy.

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
- **Reliable UDP / RTP/RTCP / RUDP / SRT / UDP-Lite** are datagram-based and may complete **out of
  order**; replies are correlated by transport message id and JSON-RPC `id`.
- **DCCP / WebSocket over DCCP** keep datagram boundaries and congestion
  control, but the current implementation keeps one in-flight exchange per
  connected session, so completion order is still serialized per session.
- **SCTP / WebSocket over SCTP** preserve message boundaries on one connected
  association. The current adapter uses `SOCK_SEQPACKET` and keeps the door
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
- **WebSocket over RUDP / SRT / UDP-Lite** keep the same dedicated WebSocket
  text/binary/close framing while switching the underlying datagram carrier to
  RUDP, encrypted SRT-style packets, or UDP-Lite; completion may also be
  **out of order** and is still correlated by JSON-RPC `id`.
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
enabled transport on every bind address. The preferred entry point is
`MultiTransportServer::builder(protocol)`, which uses typestate to require at
least one bind address and at least one enabled transport before `build()` or
`start()` are available. Configure it with `set_bind`, one port setter per
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
UDP-Lite, DCCP, and SCTP transports currently require Linux or Android kernel
support.

## Validation and fuzzing

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
