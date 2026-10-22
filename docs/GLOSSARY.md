# Glossary

This file explains the main architectural and runtime terms used in the
workspace documentation.

## Actor/session model

A client design where request dispatch is coordinated by a dedicated background
task. Instead of each API call talking to the transport independently, calls
are funneled through one session actor that owns the outbound queue, pending
table, timeout handling, and cancellation cleanup.

## Backpressure

A mechanism that slows producers down when the consumer cannot keep up. In this
project it is implemented with a **bounded** outbound `mpsc` queue in the
client session so request generation does not grow memory without limit.

## BBR

Bottleneck Bandwidth and RTT. A modern TCP congestion-control algorithm that
models available bandwidth and latency instead of reacting only to packet loss.
In this workspace it is exposed as an optional per-socket tuning choice for
TCP-based transports on Linux/Android platforms that support it.

## Batch dispatch

JSON-RPC allows sending an array of requests in one message. This project now
processes server-side batch items concurrently where possible. Response
correlation is by `id`, not by preserving the input order.

## Capability model

A typed description of what a transport can do. Here it is represented by
`TransportCapabilitySet`, which records:

- multiplexing mode
- security mode (plain or TLS)
- socket family (TCP, UDP, DCCP, or SCTP)
- whether transport-level ping is supported

## Cancellation propagation

When a client request is dropped or times out, cleanup happens in the session
actor. For multiplexed transports such as QUIC and HTTP/3, cancellation also
propagates down to the dedicated stream so abandoned work does not linger.

## Correlation metadata

Structured log fields that tie related events together. Typical examples here:
transport kind, peer address, request id, method name, latency, and response
error code.

## Datagram

A self-contained message packet delivered by transports such as UDP. Unlike a
byte stream, a datagram preserves message boundaries but may arrive late, out
of order, or not at all unless reliability is added above the socket layer.

## DCCP

Datagram Congestion Control Protocol. It keeps datagram message boundaries like
UDP while adding congestion-control behavior closer to TCP, but still does not
guarantee delivery. In this workspace it is exposed as a connected datagram
transport on Linux/Android platforms.

## SCTP

Stream Control Transmission Protocol. It preserves message boundaries like a
packet transport, supports multiple logical streams inside one association, and
can support multi-homing. In this workspace it is exposed as a connected
one-to-one `SOCK_STREAM` transport on Linux/Android platforms, preserving user
records through `MSG_EOR` rather than treating the association as a byte stream.

## Deduplication cache

A server-side cache that remembers recently seen request packet ids for a given
peer. In the reliable UDP transport it prevents retransmitted requests from
running the same JSON-RPC handler multiple times and allows cached responses to
be replayed safely.

## FFI

Foreign Function Interface. In this repository, `json-rpc-2-ffi` exposes the
Rust client to Kotlin and Swift through UniFFI-generated bindings.

## Feature flag

A Cargo feature used to compile in only the transport families that a consumer
needs. This reduces dependency surface and compile time.

## Framing

The rule by which one complete transport message is delimited. Examples:

- TCP: newline-delimited JSON documents
- QUIC: one exchange per bidirectional stream
- HTTP/1.1 and HTTP/3: request body framing
- WebSocket: one JSON document per frame

## WebSocket carrier

The concrete transport that establishes and owns a WebSocket session. Examples
in this workspace include raw TCP, HTTP/1.1 Upgrade, HTTP/3 extended CONNECT,
QUIC stream bridging, and datagram-style carriers such as reliable UDP, DCCP,
and SCTP.

## WebSocket frame envelope

The transport-neutral binary envelope this workspace uses for datagram-style
WebSocket adapters (`ws-udp`, `ws-dccp`, `ws-sctp`). It carries text, binary,
and close semantics without claiming RFC 6455 over raw datagram sockets.

## WebSocket family crate

The dedicated `json-rpc-2-websockets` crate that groups the public
WebSocket-related transport surface of the workspace. It owns the WebSocket
session adapters and concrete carriers, and re-exports the shared abstractions
from `json-rpc-2-core` and transport contracts from `json-rpc-2-transport`.
It depends on `json-rpc-2-tcp`,
`json-rpc-2-datagram`, `json-rpc-2-quic`, and `json-rpc-2-iot` for the underlying
non-WebSocket carriers, without depending on the client or server runtime crates.

## Head-of-line blocking

A transport effect where one delayed or lost unit of data prevents later units
from being delivered or completed promptly. Avoiding head-of-line blocking is
one of the reasons multiplexed QUIC-style transports and datagram-style UDP
carriers can be attractive for high-load scenarios.

## TCP Fast Open (TFO)

An optimization that allows data exchange to begin earlier in the TCP
handshake path than a classic connect-then-write sequence. In this project it
is exposed as an opt-in TCP tuning flag, with support depending on operating
system and on whether the transport adapter owns raw socket creation.

## Fuzzing

Automated generation of malformed or unusual inputs to find parser or protocol
bugs. This project fuzzes both JSON-RPC wire semantics and transport framing
boundaries.

## Graceful shutdown

Stopping a transport without abruptly dropping in-flight work. Examples here:
HTTP/3 GOAWAY-like shutdown, WebSocket close handshake, and QUIC application
close with draining.

## Health surface

The exported runtime health and metrics API. In this project it includes:

- in-process snapshots
- JSON serialization
- Prometheus text exposition
- optional HTTP routes such as `/healthz`, `/metrics.json`, and `/metrics`

## Hot path

The most frequently executed runtime path where allocations and copies matter
most. For this project, request parsing, dispatch, response serialization, and
transport write paths are typical hot paths.

## Immutable protocol core

A design where the protocol dispatcher is effectively fixed at runtime.
Registration happens during startup/build, while request handling uses an
immutable, read-optimized structure.

## Interop / interoperability

Compatibility with external implementations, not just with this repository’s
own client and server. Interop tests verify that the project speaks JSON-RPC in
a way other tools can understand.

## Jitter

A small randomized delay added to reconnect waits so multiple clients do not
reconnect in lockstep and overwhelm the server or network.

## Liveness check

A transport-level signal used to verify that the carrier is still responsive.
This project keeps liveness traffic separate from JSON-RPC application
messages.

## Multiplexed transport

A transport where multiple concurrent exchanges can proceed independently over
separate streams within one logical connection. QUIC and HTTP/3 are the main
examples here.

## Reliable UDP

This project's UDP transport adds application-level ACK, retry, ping/pong, and
deduplication semantics on top of raw UDP datagrams. It keeps UDP's message
boundaries and low handshake overhead without trying to emulate a full TCP byte
stream.

## RUDP

Reliable User Datagram Protocol. In this workspace it is modeled as a
dedicated reliable datagram carrier with its own packet format, explicit
sequence numbers, ACK/retry behavior, and per-packet ids for deduplication and
response replay.

## RTP

Real-time Transport Protocol. In this workspace it is used as a UDP-based
message carrier for JSON-RPC where each request or response rides in one RTP
packet with sequence number, timestamp, and SSRC metadata.

## RTCP

Real-time Transport Control Protocol. Here it is used through minimal RTCP APP
packets that carry transport-level ACK, ping, and pong control messages for the
RTP transport.

## RTCP mux

Running RTP data packets and RTCP control packets on the same UDP socket. This
workspace uses that model so the RTP transport stays a single-port deployment.

## SRT

Secure Reliable Transport. The transport uses Haivision libsrt for standard
caller/listener handshake, message-oriented reliable delivery, congestion
control, and passphrase-based encryption. JSON-RPC framing over the carrier is
project-defined; the old private `JSRT` packet format is not this transport.

## UDP-Lite

User Datagram Protocol Lite (RFC 3828). It keeps datagram boundaries like UDP
but allows checksum coverage to protect only part of the packet. In this
workspace it is exposed as a Linux/Android UDP-family transport with shared
application-level retry/dedup semantics and configurable checksum coverage.

## WebSocket over UDP

A dedicated transport in this workspace that keeps WebSocket-style
text/binary/close message framing while using the reliable UDP carrier instead
of HTTP Upgrade or QUIC stream bridging. It is not intended as a claim of RFC
6455 interoperability over plain UDP.

## WebSocket over RTP

A sibling transport to WebSocket/UDP that keeps the same dedicated
text/binary/close framing but carries it inside RTP data packets with RTCP APP
control traffic on the same UDP socket.

## WebSocket over RUDP

A sibling transport to WebSocket/UDP and WebSocket/RTP that keeps the same
dedicated text/binary/close framing but carries it inside RUDP packets.

## WebSocket over SRT

A sibling transport to WebSocket/UDP, WebSocket/RTP, and WebSocket/RUDP that
keeps the same dedicated text/binary/close framing but carries it inside the
standard SRT message transport. The WebSocket-style envelope is still
project-defined, not RFC 6455 over SRT.

## WebSocket over UDP-Lite

A sibling transport to WebSocket/UDP, WebSocket/RTP, WebSocket/RUDP, and
WebSocket/SRT that keeps the same dedicated text/binary/close framing but
carries it over UDP-Lite datagrams.

## WebSocket over DCCP

A sibling transport to WebSocket/UDP that keeps the same dedicated
text/binary/close framing but uses a connected DCCP carrier instead of the
reliable UDP adapter.

## WebSocket over SCTP

A sibling transport to WebSocket/UDP and WebSocket/DCCP that keeps the same
dedicated text/binary/close framing but uses a connected SCTP carrier.

## MQTT

Message Queuing Telemetry Transport. The adapter is a standard MQTT 3.1.1
broker client with connection/subscription lifecycle and request/reply topics.
Both the JSON-RPC client and service connect outward to an external broker;
the RPC service is not itself an MQTT broker.

## CoAP

Constrained Application Protocol. In this workspace it is modeled as a
UDP-family, message-oriented JSON-RPC carrier that reuses the real CoAP header
layout (version, type, token, message id) with a custom method/response code
for JSON-RPC payloads.

## AMQP

Advanced Message Queuing Protocol. The adapter uses AMQP 0-9-1 connection,
channel, publish/consume, reply queue, and correlation semantics through an
external broker such as RabbitMQ. AMQP 1.0 is a different protocol and is not
the selected binding.

## WebSocket over MQTT

A sibling transport to WebSocket/TCP that keeps the same dedicated
text/binary/close framing but carries it inside MQTT `PUBLISH` packets.

## WebSocket over CoAP

A sibling transport to WebSocket/UDP that keeps the same dedicated
text/binary/close framing but carries it inside CoAP request/response
messages.

## WebSocket over AMQP

A sibling transport to WebSocket/TCP that keeps the same dedicated
text/binary/close framing but carries it inside AMQP content frames.

## Notification

A JSON-RPC request without an `id`. Notifications do not expect a response.

## Prometheus exporter

A metrics representation in Prometheus text exposition format, suitable for
scraping by Prometheus-compatible observability systems.

## Property testing

A testing style where many generated inputs are checked against expected
properties rather than only against a small set of handwritten examples.

## Reconnect policy

The client policy that controls what happens after repeated liveness failures:
whether to try `restore()` first, how long to wait between reconnect attempts,
how much jitter to add, and how long one reconnect streak may continue.

## Retransmit budget

The maximum number of retry attempts the reliable UDP transport makes after the
initial send. Once the budget is exhausted, the request fails instead of
waiting forever for a lost ACK or response.

## Serialized transport

A transport where exchanges are handled in transport order on one logical
carrier and do not have independent concurrent completion semantics.

## Stream multiplexor

A shared utility used by stream-capable transports to track and limit active
sessions/streams and to coordinate shutdown.

## Transport adapter

The layer that binds the abstract JSON-RPC protocol core to a concrete carrier
such as TCP, QUIC, HTTP/1.1, HTTP/3, or WebSocket.

## Typestate pattern

A type-driven builder pattern where certain methods are available only in valid
construction states. Here it is used so some server builder operations are only
possible after required configuration has been supplied.

## Zero-copy-ish path

An optimization direction where data is passed through `Bytes`/`BytesMut`
instead of repeatedly allocating and copying fresh `Vec<u8>` buffers. It is
not literally zero-copy everywhere, but it reduces unnecessary copying on hot
paths.
