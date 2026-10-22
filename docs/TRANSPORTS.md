# Transport semantics

This document describes transport behavior separately from JSON-RPC 2.0
semantics.

Protocol names do not imply full standards compliance. See
[the conformance matrix](CONFORMANCE.md) for supported wire subsets and
external interoperability evidence, and [retry semantics](RETRY-SEMANTICS.md)
for delivery and deduplication limits.

## Framing

| Transport | Framing |
|---|---|
| TCP | one compact JSON document per newline-delimited line |
| TLS/TCP | one compact JSON document per newline-delimited line inside a verified TLS connection |
| UDP | one JSON-RPC message per reliable transport datagram |
| RTP/RTCP | one JSON-RPC message per RTP packet, with RTCP APP control packets multiplexed on the same UDP socket |
| RUDP | one JSON-RPC message per reliable RUDP packet |
| SRT | one JSON-RPC message per encrypted reliable SRT-style packet |
| UDP-Lite | one JSON-RPC message per reliable UDP-Lite datagram |
| DCCP | one JSON-RPC message per connected DCCP datagram session |
| SCTP | one JSON-RPC message per connected SCTP packet session |
| MQTT-like | one JSON-RPC message per private PUBLISH-like packet on a persistent TCP connection; not a standard broker connection |
| CoAP subset | one JSON-RPC message per POST (0.02) / Content (2.05) pair over UDP, without general option support |
| AMQP-like | one JSON-RPC message per private body-frame envelope on a persistent TCP connection; no AMQP connection/channel lifecycle |
| QUIC | one JSON-RPC exchange per bidirectional QUIC stream |
| HTTP/1.1 | `POST /` with JSON body |
| HTTP/3 | `POST /` with JSON body on an independent request stream |
| WebSocket/TCP | one JSON document per WebSocket data frame |
| WebSocket/UDP | one WebSocket-style text or binary message per reliable transport datagram |
| WebSocket/RTP | one WebSocket-style text or binary message per RTP packet, with RTCP APP control packets multiplexed on the same UDP socket |
| WebSocket/RUDP | one WebSocket-style text or binary message per reliable RUDP packet |
| WebSocket/SRT | one WebSocket-style text or binary message per encrypted reliable SRT-style packet |
| WebSocket/UDP-Lite | one WebSocket-style text or binary message per reliable UDP-Lite datagram |
| WebSocket/DCCP | one WebSocket-style text or binary message per connected DCCP datagram session |
| WebSocket/SCTP | one WebSocket-style text or binary message per connected SCTP packet session |
| WebSocket/MQTT | one WebSocket-style text or binary message per MQTT `PUBLISH` packet on a persistent TCP connection |
| WebSocket/CoAP | one WebSocket-style text or binary message per CoAP request/response pair over UDP |
| WebSocket/AMQP | one WebSocket-style text or binary message per AMQP content frame on a persistent TCP connection |
| WebSocket/QUIC | one JSON document per WebSocket data frame over a dedicated QUIC stream bridge |
| WebSocket/HTTP/1.1 | one JSON document per WebSocket data frame after Upgrade on `ws-http1`; use `ws-tls` for `wss://` |
| WebSocket/TLS | one JSON document per WebSocket data frame after HTTP/1.1 Upgrade inside TLS; the runtime exposes a dedicated `ws-tls` listener |
| WebSocket/HTTP/3 | one JSON document per WebSocket data frame after extended CONNECT |

## Keepalive behavior

- TCP / TLS/TCP / reliable UDP / RUDP / SRT / UDP-Lite / DCCP / SCTP / raw QUIC use reserved non-JSON control payloads.
- RTP/RTCP uses RTCP APP packets for ACK, ping, and pong control traffic.
- The MQTT-like profile uses `PINGREQ`/`PINGRESP` packet layouts; the AMQP-like profile uses the heartbeat frame layout.
- The CoAP subset uses an empty confirmable message as a ping, acknowledged locally with an empty ACK. Standard CoAP ping expects an empty Reset instead.
- HTTP/1.1 / HTTP/3 use `GET /.well-known/json-rpc-2/ping`.
- Stream WebSocket transports use native Ping/Pong control frames; message-oriented WebSocket-style envelopes use the underlying carrier's liveness mechanism.

Keepalive traffic never enters the JSON-RPC method dispatcher.

## Reconnect behavior

- Client liveness checks are configured through `ClientLivenessConfig`.
- Missed pongs are counted independently from request timeouts.
- When the missed-pong threshold is hit, the client first tries `restore()`
  and then `reconnect()` according to `ReconnectPolicy`.
- `ReconnectPolicy` can apply exponential backoff, additive jitter and a
  maximum elapsed reconnect budget to avoid reconnect storms.
- Request timeouts are enforced per JSON-RPC call through the actor/session
  layer.

## TCP tuning

- TCP-based transports share `TcpTuningConfig`.
- `TCP_NODELAY` is enabled by default for lower request/response latency.
- `TcpCongestionControl::Bbr` is available as an opt-in Linux/Android socket
  setting; if the kernel does not expose `bbr`, socket setup fails explicitly
  instead of silently falling back.
- `fast_open` enables listener-side TCP Fast Open for TCP-bound servers on
  supported operating systems.
- Client-side TCP Fast Open is available on Linux/Android for raw TCP and
  HTTP/1.1 connect paths; adapters that delegate socket creation to external
  libraries may only inherit the `no_delay` part of the tuning profile.
- TLS/TCP uses the same TCP tuning options as raw TCP.

## TLS/TCP semantics

- TLS/TCP carries the same newline-delimited JSON-RPC messages and reserved
  ping/pong control payloads as raw TCP, inside a TLS 1.2 or TLS 1.3 session.
- The client requires an explicit trusted PEM CA bundle and a server name.
  Certificate-chain and hostname verification are always enabled.
- The server uses the configured certificate chain and private key. TLS/TCP
  negotiates the `json-rpc-2-tls` ALPN identifier so it cannot be confused
  with a different application protocol on the same endpoint.
- Calls on one connection are serialized; reconnect creates a new verified
  TLS session, and an interrupted request is never replayed.

## WebSocket over TLS semantics

- `ws-tls` is an RFC 6455 WebSocket connection established with the ordinary
  HTTP/1.1 Upgrade handshake inside TLS; it uses the `/ws` endpoint.
- The server uses the configured TLS certificate and private key and leaves
  ALPN unset for broad HTTP/1.1 client compatibility.
- Clients using an explicit CA bundle verify both the certificate chain and
  the URL hostname; no certificate-validation bypass is provided.
- The dedicated `ws-tls` listener has its own TCP port, so plain `ws-http1`
  and TLS-protected WSS can run simultaneously.

## Reliable UDP semantics

- UDP transport is **message-oriented**: one JSON-RPC message must fit into one
  datagram bounded by `UdpReliabilityConfig::max_packet_size`.
- The current contract intentionally does **not** fragment or reassemble large
  payloads. Oversized messages fail explicitly instead of silently splitting
  data across packets.
- Client requests are retried according to `UdpReliabilityConfig` until the
  transport receives an ACK, a response, or the retry budget is exhausted.
- The server deduplicates request packet ids per peer while the corresponding
  entry remains in the live response cache. Eviction, expiry, restart, or a
  changed peer identity can permit repeated handler execution; this is not an
  exactly-once guarantee.
- UDP ping/pong packets are transport-level control traffic, not JSON-RPC
  messages.
- Because the transport is datagram-based, completion order is not tied to send
  order; correlation is by JSON-RPC `id` and transport packet id.

## RTP/RTCP semantics

- RTP/RTCP is **message-oriented**: one JSON-RPC request or response maps to
  one RTP packet, still bounded by `UdpReliabilityConfig::max_packet_size`.
- RTP headers add sequence numbers, timestamps, and SSRC values to each JSON-RPC
  payload, but JSON-RPC correlation still uses the transport `message_id` plus
  the JSON-RPC `id`.
- RTCP APP packets provide transport-level ACK and ping/pong control messages.
- The transport reuses the same retransmit, timeout, and deduplication policy
  as reliable UDP; the difference is the wire format, not the upper-layer
  reliability contract.
- Because RTP/RTCP is datagram-based, concurrent replies may complete out of
  send order.

## RUDP semantics

- RUDP is **message-oriented**: one JSON-RPC request or response maps to one
  RUDP packet, still bounded by `UdpReliabilityConfig::max_packet_size`.
- The wire format keeps explicit packet sequence numbers and per-exchange
  packet ids separate from the reliable UDP packet contract used by the
  `udp` transport.
- Retransmit timing, retry budget, deduplication, and oversized-message
  handling reuse `UdpReliabilityConfig`.
- Because RUDP is datagram-based, concurrent replies may complete out of send
  order.

## SRT semantics

- SRT uses the native libsrt caller/listener protocol over UDP.
- libsrt owns handshake, ARQ, congestion control, message delivery, and
  passphrase-based encryption; the reliable-UDP application cache is not used
  as an SRT delivery guarantee.
- `srt` and `ws-srt` require `SrtConfig` and the native libsrt build dependency.
- JSON-RPC framing and application ping/pong over SRT are project-defined.
  A standard SRT connection is not automatically a JSON-RPC application peer.

## UDP-Lite semantics

- UDP-Lite is **message-oriented**: one JSON-RPC request or response maps to
  one UDP-Lite datagram, still bounded by `UdpReliabilityConfig::max_packet_size`.
- The transport keeps the same application-level ACK/retry/dedup policy as the
  reliable UDP transport, but the socket checksum coverage is configured
  through `UdpLiteConfig`.
- Coverage `0` means full packet coverage; values `>= 8` request partial
  coverage while still protecting the UDP-Lite header.
- `udplite` and `ws-udplite` are Linux/Android-only in this workspace because
  they depend on kernel UDP-Lite socket support.

## WebSocket over UDP semantics

- WebSocket/UDP reuses the same reliable UDP carrier policy, but its payload is
  a WebSocket-style text/binary/close frame envelope rather than raw JSON-RPC
  bytes.
- Client JSON-RPC requests are sent as text frames, so the server responds with
  text frames too unless another client deliberately uses binary frames.
- Transport-level liveness still uses the reliable UDP ping/pong packets, not
  application WebSocket frames.
- This is a dedicated transport in this workspace, not RFC 6455 over HTTP.

## WebSocket over RTP semantics

- WebSocket/RTP uses the same dedicated text/binary/close frame envelope as
  WebSocket/UDP, but carries it inside RTP data packets instead of the custom
  reliable-UDP packet format.
- RTCP APP packets carry ACK/ping/pong transport control on the same UDP
  socket.
- As with WebSocket/UDP, completion may happen out of send order and is
  correlated by JSON-RPC `id` plus the transport message id.
- This is a dedicated transport in this workspace, not RFC 6455 over RTP.

## WebSocket over RUDP semantics

- WebSocket/RUDP reuses the same dedicated text/binary/close frame envelope as
  WebSocket/UDP, but carries it inside RUDP packets instead of the baseline
  reliable-UDP wire format.
- Transport-level liveness still uses carrier ACK/ping/pong packets rather
  than application WebSocket frames.
- Completion may happen out of send order and is correlated by JSON-RPC `id`
  plus the transport packet id.

## WebSocket over SRT semantics

- WebSocket/SRT keeps the dedicated text/binary/close frame envelope, but uses
  standard libsrt message delivery underneath.
- SRT reliability is provided by libsrt, not by wrapping reliable-UDP packets.
- The envelope is a project-specific binding, not RFC 6455 over SRT.

## WebSocket over UDP-Lite semantics

- WebSocket/UDP-Lite keeps the same dedicated text/binary/close frame envelope
  as WebSocket/UDP, but carries it inside UDP-Lite datagrams.
- Transport-level liveness still uses carrier ACK/ping/pong packets rather
  than application WebSocket frames.
- Completion may happen out of send order and is correlated by JSON-RPC `id`
  plus the transport packet id.

## DCCP semantics

- DCCP preserves datagram boundaries and adds transport-level congestion
  control without guaranteeing delivery.
- In this workspace, one JSON-RPC request or response maps to one DCCP
  datagram on a connected session.
- The current client/server adapters keep one in-flight exchange per session,
  so request/response ordering is serialized even though the carrier itself is
  datagram-oriented.
- Transport-level ping/pong uses the same reserved non-JSON control payloads
  as raw TCP/QUIC.
- DCCP transports are available only on Linux/Android builds where the kernel
  exposes DCCP sockets.

## WebSocket over DCCP semantics

- WebSocket/DCCP uses the same dedicated text/binary/close frame envelope as
  WebSocket/UDP, but rides over a connected DCCP session instead of the
  reliable UDP carrier.
- It is a workspace-specific transport adapter, not RFC 6455 over HTTP.
- Ordering remains serialized per DCCP session in the current implementation.

## SCTP semantics

- SCTP preserves message boundaries and supports multi-streaming plus
  multi-homing at the carrier level.
- In this workspace, one JSON-RPC request or response maps to one SCTP packet
  on a connected `SOCK_SEQPACKET` association.
- The current adapter does not yet expose explicit stream-id or multi-home
  selection policy through the public API, but it keeps the carrier semantics
  isolated below the transport-agnostic JSON-RPC core.
- Transport-level ping/pong uses the same reserved non-JSON control payloads
  as raw TCP/QUIC.
- SCTP transports are available only on Linux/Android builds where the kernel
  exposes SCTP sockets.

## WebSocket over SCTP semantics

- WebSocket/SCTP uses the same dedicated text/binary/close frame envelope as
  WebSocket/UDP and WebSocket/DCCP, but rides over a connected SCTP
  association.
- It is a workspace-specific transport adapter, not RFC 6455 over HTTP.

## MQTT semantics

- Both sides are MQTT 3.1.1 clients of an external broker. The RPC service
  subscribes to the request route; it does not bind a private MQTT listener.
- Connection, subscription, publish acknowledgement, and keepalive are handled
  by a maintained MQTT stack rather than a PUBLISH-only codec.
- Reply routing and correlation are part of the project-defined JSON-RPC
  binding. Configure matching routes on the client and service.
- QoS acknowledgements do not guarantee exactly-once application side effects.

## CoAP semantics

- CoAP carries JSON-RPC payloads inside CoAP request/response messages over
  UDP, reusing the real CoAP header layout (version, type, token, message id)
  with a custom method/response code for JSON-RPC payloads.
- CoAP is **message-oriented**, like the other UDP-family transports, so
  concurrent replies may complete out of send order.
- Transport-level liveness uses an empty confirmable CoAP message as a ping,
  acknowledged the same way as any other CoAP request.

## AMQP semantics

- Both sides use AMQP 0-9-1 connections to an external broker, such as RabbitMQ.
- Connection/channel negotiation, queue declaration, publish/consume,
  acknowledgements, and heartbeats use a maintained AMQP library.
- Request/reply queues and correlation properties implement the JSON-RPC
  application binding; AMQP 1.0 is not the selected protocol.
- Acknowledgements and redelivery cannot guarantee exactly-once application
  side effects across crashes.

## WebSocket over MQTT / CoAP / AMQP semantics

- WebSocket/MQTT and WebSocket/AMQP keep the same dedicated text/binary/close
  frame envelope as WebSocket/TCP, but carry it inside MQTT `PUBLISH` packets
  or AMQP content frames respectively.
- WebSocket/CoAP keeps the same dedicated frame envelope as WebSocket/UDP, but
  carries it inside CoAP request/response messages.
- These are workspace-specific transport adapters, not RFC 6455 over
  MQTT/CoAP/AMQP.

## Cancellation behavior

- Dropping a client-side request future always removes it from the actor's
  pending table.
- QUIC and HTTP/3 additionally propagate cancellation down to the dedicated
  carrier stream (`RESET_STREAM` / request-stream cancellation), so abandoned
  multiplexed calls do not linger until their natural timeout.
- Serialized carriers cannot cancel a single in-flight exchange independently;
  they still perform session-level cleanup immediately.
- Raw TCP drops an interrupted active connection and opens a fresh connection
  for the next call. The interrupted request is never automatically replayed,
  and cancellation while queued does not disturb another active exchange.

## Shutdown behavior

- HTTP/3 transports send GOAWAY-style shutdown via `h3.shutdown(0)` and allow
  in-flight streams to drain.
- WebSocket transports send a Close frame with a bounded wait for peer
  acknowledgement.
- QUIC transports close connections with application code `0`.

`ServerShutdownPolicy` controls the close-handshake timeout used by transport
adapters.

## Multiplexing and ordering

- Serialized carriers: TCP, TLS/TCP, DCCP, HTTP/1.1 keep-alive session,
  WebSocket/TCP, WebSocket/DCCP, WebSocket/HTTP/1.1.
- Multiplexed carriers: UDP, RTP/RTCP, RUDP, SRT, UDP-Lite, SCTP, CoAP, WebSocket/UDP,
  WebSocket/RTP, WebSocket/RUDP, WebSocket/SRT, WebSocket/UDP-Lite, WebSocket/SCTP,
  WebSocket/CoAP, QUIC, HTTP/3, WebSocket/QUIC, WebSocket/HTTP/3.
- Broker delivery is correlated by per-client reply destination and exchange
  metadata, not by TCP response arrival order. SRT preserves message ordering
  within a native connection, but dispatches independent RPC handlers
  concurrently and correlates out-of-order responses.

For multiplexed carriers, concurrent calls may complete out of order. The
client correlates replies strictly by JSON-RPC `id`.

Server-side batch dispatch also completes out of order: independent batch items
are executed concurrently and the batch response array is correlated by `id`
rather than by original array position.
