# json-rpc-2-ffi

A thin [UniFFI](https://mozilla.github.io/uniffi-rs/) wrapper around the
`json-rpc-2` Rust crate's client, compiled into a native library and
consumed from Kotlin (Android/JVM, via JNA) and Swift (iOS).

**No JSON-RPC protocol logic is reimplemented here.** This crate only:
- drives the existing `json_rpc_2::client::RpcClient` and every client
  transport the base crate ships (`tcp`, `tls`, `udp`, `rtp`, `rudp`, `srt`, `udplite`, `dccp`,
  `sctp`, `http1`, `ws-tcp`, `ws-udp`, `ws-rtp`, `ws-rudp`, `ws-srt`, `ws-udplite`,
  `ws-dccp`, `ws-sctp`, `ws-http1`, `quic`, `http3`, `ws-quic`, `ws-http3`),
  delegating to a local `AnyTransport` enum that
  simply forwards to whichever concrete transport was selected,
- replicates the original CLI example's TLS endpoint setup (trusting a
  caller-supplied PEM) for the TLS-based transports (`tls`, `quic`, `http3`,
  `ws-quic`, `ws-http3`),
- runs everything on a dedicated background Tokio runtime and exposes
  **blocking** functions (`connect`, `call`, `notify`, `disconnect`) so
  UniFFI's proc-macros stay simple; the generated Kotlin/Swift bindings are
  then wrapped with coroutines/`async` in the consuming projects so callers
  still get a non-blocking API.

## Layout

- `src/lib.rs` — the `#[uniffi::export]`-annotated `RpcClient` object and
  `FfiError` error type.
- `uniffi-bindgen.rs` — entry point for the `uniffi-bindgen` CLI (only
  built behind the `uniffi-bindgen` feature, kept out of the mobile
  `cdylib`/`staticlib`).
- `build-mobile.sh` — builds every Android ABI `.so`, the iOS
  `XCFramework`, and regenerates the Kotlin/Swift bindings, then copies
  everything into the sibling `JSON_RPC_2_KMP` repository:
  - `JSON_RPC_2_KMP/jsonrpc-client/src/main/jniLibs/<abi>/libjson_rpc_2_ffi.so`
  - `JSON_RPC_2_KMP/jsonrpc-client/src/main/kotlin/uniffi/json_rpc_2_ffi/json_rpc_2_ffi.kt`
  - `JSON_RPC_2_KMP/ios/JsonRpc2Client/Frameworks/JsonRpc2FFI.xcframework`
  - `JSON_RPC_2_KMP/ios/JsonRpc2Client/Sources/JsonRpc2Client/json_rpc_2_ffi.swift`
- `bindings/` — scratch output of the script above, regenerated on every
  run (git-ignored; not meant to be committed).

## Prerequisites

```sh
rustup target add \
    aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android \
    aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
cargo install cargo-ndk
```

An Android NDK and Xcode command line tools must also be installed.
`ANDROID_NDK_HOME` defaults to `~/Library/Android/sdk/ndk/27.0.12077973` in
the script; override it if you use a different NDK version.

## Rebuilding after a code change

```sh
./build-mobile.sh
```

Then rebuild the consumers:
- Android: `cd ../../JSON_RPC_2_KMP && ./gradlew :app:assembleDebug`
- iOS: open/build `JSON_RPC_2_KMP/ios/JsonRpc2Client` with Xcode or
  `swift build`/`xcodebuild` (see its own README).

## Transport & API surface

All 29 transports `json-rpc-2` ships are exposed via `FfiTransportKind`
(`Tcp`, `Tls`, `Udp`, `Rtp`, `Rudp`, `Srt`, `UdpLite`, `Dccp`, `Sctp`, `Mqtt`, `Coap`, `Amqp`, `Http1`, `WsTcp`, `WsUdp`,
`WsRtp`, `WsRudp`, `WsSrt`, `WsUdpLite`, `WsDccp`, `WsSctp`, `WsMqtt`, `WsCoap`, `WsAmqp`, `WsHttp1`, `Quic`, `Http3`,
`WsQuic`, `WsHttp3`) and
both:

- a generic `RpcClient.connect(kind, options)` constructor with
  `ConnectOptions { host, port, ca_pem, server_name, srt_passphrase, udplite_send_coverage, udplite_recv_coverage }`
- explicit transport-specific entry points:
  `connect_tcp`, `connect_udp`, `connect_rtp`, `connect_rudp`, `connect_srt`, `connect_udplite`,
  `connect_tls`, `connect_dccp`, `connect_sctp`, `connect_mqtt`, `connect_coap`, `connect_amqp`, `connect_http1`, `connect_ws_tcp`,
  `connect_ws_udp`, `connect_ws_rtp`, `connect_ws_rudp`, `connect_ws_srt`, `connect_ws_udplite`,
  `connect_ws_dccp`, `connect_ws_sctp`, `connect_ws_mqtt`, `connect_ws_coap`, `connect_ws_amqp`, `connect_ws_http1`, `connect_quic`,
  `connect_http3`, `connect_ws_quic`, `connect_ws_http3`

- `tcp`/`udp`/`rtp`/`rudp`/`dccp`/`sctp`/`mqtt`/`coap`/`amqp`/`http1`/`ws-tcp`/`ws-udp`/`ws-rtp`/
  `ws-rudp`/`ws-dccp`/`ws-sctp`/`ws-mqtt`/`ws-coap`/`ws-amqp`/`ws-http1` need only `host`/`port`.
- `srt`/`ws-srt` additionally require a shared passphrase
  (`ConnectOptions.srt_passphrase` or `SrtConnectOptions.passphrase`).
- `udplite`/`ws-udplite` optionally accept outbound/inbound checksum coverage
  values (`0` = full coverage, `>= 8` = partial coverage) and are available
  only on Linux/Android builds.
- `quic`/`http3`/`ws-quic`/`ws-http3` additionally require `ca_pem` (a PEM-encoded
  certificate to trust — the reference server uses a self-signed dev
  certificate, see `../certs/cert.pem`); `server_name` defaults to `host`.
- `tls` uses TLS over TCP and also requires `ca_pem`; `server_name` defaults
  to `host`. Use `connect_tls` or the generic `RpcClient.connect`.
- `dccp`/`ws-dccp` and `sctp`/`ws-sctp` are available only on Linux/Android builds.

For a separate certificate-setting step, create
`RpcClientBuilder::new(kind, options)`, call
`builder.set_ca_certificate_pem(pem_contents)`, then `builder.connect()`.
The setter validates the PEM bundle immediately and preserves the previous
certificate on error. It accepts certificate contents, not a file path or a
private key. Set trust before connecting; existing clients are not modified.
Existing `ConnectOptions.ca_pem` and transport-specific constructors remain supported.
- `mqtt`/`ws-mqtt` and `amqp`/`ws-amqp` carry JSON-RPC over a persistent TCP
  connection; `coap`/`ws-coap` carries it over UDP.

Default ports for each transport are documented in the base repository's
`.env` (`JSON_RPC_TCP_PORT`, `JSON_RPC_TLS_PORT`, `JSON_RPC_UDP_PORT`, `JSON_RPC_RTP_PORT`,
`JSON_RPC_RUDP_PORT`, `JSON_RPC_SRT_PORT`, `JSON_RPC_UDPLITE_PORT`, `JSON_RPC_DCCP_PORT`,
`JSON_RPC_SCTP_PORT`, `JSON_RPC_MQTT_PORT`, `JSON_RPC_COAP_PORT`, `JSON_RPC_AMQP_PORT`,
`JSON_RPC_HTTP1_PORT`, `JSON_RPC_QUIC_PORT`,
`JSON_RPC_HTTP3_PORT`, `JSON_RPC_WS_TCP_PORT`, `JSON_RPC_WS_UDP_PORT`,
`JSON_RPC_WS_RTP_PORT`, `JSON_RPC_WS_RUDP_PORT`, `JSON_RPC_WS_SRT_PORT`, `JSON_RPC_WS_UDPLITE_PORT`,
`JSON_RPC_WS_DCCP_PORT`, `JSON_RPC_WS_SCTP_PORT`, `JSON_RPC_WS_MQTT_PORT`, `JSON_RPC_WS_COAP_PORT`,
`JSON_RPC_WS_AMQP_PORT`, `JSON_RPC_WS_QUIC_PORT`,
`JSON_RPC_WS_HTTP1_PORT`, `JSON_RPC_WS_HTTP3_PORT`);
the Kotlin/Swift wrappers mirror them as defaults too.

`params`/results cross the FFI boundary as JSON-encoded strings (UniFFI has
no native `serde_json::Value` support), so pair this with any JSON library
on the Kotlin/Swift side.

The FFI surface now also mirrors the richer Rust-side runtime policy:

- `set_ping_frequency(...)`
- `set_max_missed_pongs(...)`
- `set_request_timeout_ms(...)`

and includes transport-specific option records (`TcpConnectOptions`,
`UdpConnectOptions`, `RtpConnectOptions`, `DccpConnectOptions`, `SctpConnectOptions`,
`WebSocketConnectOptions`, `QuicConnectOptions`) as the preferred typed input
for the explicit connect functions, including `RudpConnectOptions` and
`SrtConnectOptions`, plus `UdpLiteConnectOptions`, `MqttConnectOptions`,
`CoapConnectOptions`, and `AmqpConnectOptions`.
