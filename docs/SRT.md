# Native SRT

The `srt` feature (enabled by default) uses **Haivision libsrt**, through the
maintained `tstrans-srt-sys` 0.8 C bindings. It does not use the legacy `JSRT`
codec. Existing `SrtCrypto`/`SrtPacket` utilities remain legacy codec APIs only.
This intentionally changes network compatibility.

## Build and deployment

The launch scripts in `tools/` build **Haivision/srt 1.5.7** directly from
https://github.com/Haivision/srt, pinned to commit
`899348d8318eb9a3c5a5b6ec43c4a1114288773a`. The shared library is built with
OpenSSL encryption enabled and cached under `target/native/srt/`. No system
installation or root access is needed. The first build needs internet access,
Git, CMake, a C++ compiler, OpenSSL development files, pkg-config and libclang:

* macOS: `brew install cmake openssl@3 pkg-config llvm`
* Debian/Ubuntu: `apt install git cmake build-essential libssl-dev pkg-config libclang-dev`

To use the same pinned library with Cargo directly:

```sh
bash tools/build-srt.sh cargo build --workspace
bash tools/build-srt.sh cargo test -p json-rpc-2-datagram --no-default-features --features srt
```

The wrapper selects the local headers/library through `PKG_CONFIG_PATH` and
sets the runtime library search path for its command. Deploy the shared library
from the local installation together with its OpenSSL runtime dependencies.
For cross compilation, use a target-specific system installation as below;
the wrapper builds for the host.

### Cargo and IDE integration

Run `bash tools/build-srt.sh` once to prepare the pinned native library.
The workspace's `.cargo/config.toml` then supplies its pkg-config and shared
library paths to Cargo automatically, including Cargo commands launched by
RustRover. No special IDE launcher or IDE environment settings are required.
The paths are relative to the workspace root, not a particular user's directory.
After preparing the library, reload the Cargo project in the IDE.

Existing environment variables take precedence over these defaults. If you
already set `PKG_CONFIG_PATH`, `LD_LIBRARY_PATH`, or `DYLD_LIBRARY_PATH`, include
the corresponding local installation directory in your existing search paths,
or use the wrapper, which prepends the required paths. Programs launched
directly rather than through Cargo still need the matching runtime library
search path.

### Alternative: system installation

Install libsrt **1.5 or later with encryption enabled**, pkg-config and
libclang (bindgen):

* macOS: `brew install srt pkg-config llvm`
* Debian/Ubuntu: `apt install libsrt-openssl-dev pkg-config libclang-dev`
  (some distributions name the package `libsrt-gnutls-dev` or `libsrt-dev`).

Nonstandard prefixes need `PKG_CONFIG_PATH` and potentially `LIBCLANG_PATH`.
Deploy the matching libsrt shared library and its crypto runtime dependencies.
Cross builds need target libsrt headers/libraries and target pkg-config
configuration. The crate's build script requires system libsrt and fails
explicitly when unavailable; it never silently disables SRT or selects the
binding's unencrypted vendored fallback. Do not set `SRT_FORCE_VENDORED` or
`SRT_NO_PKG_CONFIG`; those binding overrides are not supported by this adapter.
Other transports can build without SRT using `--no-default-features`.

## API

```rust,ignore
let server = SrtTransport::bind(address, SrtConfig {
    passphrase: "a strong shared secret".into(),
}).await?;
let address = server.local_addr()?;
let running = server.start(protocol);
let client = SrtClientTransport::connect(address, "a strong shared secret").await?;
```

`SrtWebSocketTransport` has the same async `bind`/`local_addr` API.
There is deliberately no `new(tokio::net::UdpSocket, ...)`: libsrt owns the
socket, handshake, listener/caller lifecycle, encryption and retransmission.
IPv6 listeners set `SRTO_IPV6ONLY=1` so the runtime can bind separate IPv4 and
IPv6 listeners on the same port without wildcard address conflicts.
The passphrase must contain 10–79 **bytes**, as required by libsrt; it is
configured as `SRTO_PASSPHRASE` with AES-256 (`SRTO_PBKEYLEN=32`) and enforced
encryption, never hashed or used in a private application crypto scheme.

Both endpoints use standard SRT live/message mode, with too-late packet drop
disabled for reliable RPC delivery. Any standard encrypted libsrt peer can
complete the handshake and send/receive these messages. SRT is a carrier,
not an RPC protocol: an independent peer must implement the application
framing below to exchange RPCs, just as a TCP peer must implement TCP RPC
framing. WebSocket/SRT additionally encodes a WebSocket frame inside the
application payload; it is not an HTTP WebSocket Upgrade endpoint.

## Application message framing (version 1)

Each frame starts with **one SRT message of exactly 18 bytes**:

| Offset | Bytes | Meaning |
| --- | --- | --- |
| 0 | 4 | ASCII `JRPC` |
| 4 | 1 | Version `1` |
| 5 | 1 | Kind: request=1, response=2, empty/notification ACK=3, ping=4, pong=5, drain=6, drained=7 |
| 6 | 8 | Correlation ID, unsigned big-endian |
| 14 | 4 | Payload length, unsigned big-endian |

The next SRT messages contain the payload in ordered chunks of at most 1316
bytes, until exactly the declared length has been read. No other frame can
interleave its chunks. ACK, ping, pong, drain and drained have zero payload. Payloads are
bounded by `MAX_MESSAGE_BYTES`; invalid headers/lengths close the session.
Complete JSON-RPC documents (including batches) are opaque payloads.
Notifications receive only an application ACK, never a JSON-RPC response.
There is no private handshake, packet ARQ, encryption or application replay.

Client requests are correlated independently, so late replies after
cancellation cannot satisfy subsequent calls. Cancellation during a partial
write closes that connection to prevent corrupting framing. Dropped request
futures remove their pending entries. There are at most 1024 outstanding
client calls; server concurrency follows `TransportRuntime` limits.
Frame fragments are written in order, but server handlers execute concurrently
and responses may arrive out of request order, correlated by application ID.
A slow abandoned handler does not serialize subsequent RPCs. The default live
playout latency is approximately 120ms per direction; 300ms local round-trip
deadlines leave little scheduling margin and are not a network latency guarantee.
Handshake and write timeouts are bounded (4s/5s); receiving a partial frame
times out after 5s. The compatibility `UdpReliabilityConfig` determines only
the RPC timeout as `retransmit_interval * (max_retransmits + 1)`; libsrt
handles transport recovery and no RPC is replayed. `reconnect` explicitly
rebuilds the connection, failing pending calls from the old generation.
The convenience `connect` methods use a six-second RPC timeout rather than
UDP's short ARQ budget, to accommodate SRT's negotiated playout latency.
`close` is idempotent. Server shutdown drains handlers up to
`TransportRuntime::close_timeout()` before aborting them.
Graceful server shutdown also sends an ordered drain barrier (ID zero) after
all in-flight handlers finish writing. The client receiver returns a matching
drained acknowledgment only after consuming every preceding response frame.
The server waits for that acknowledgment before closing libsrt, preventing
native shutdown from discarding responses still pending SRT playout. This
exchange shares the shutdown deadline; an unresponsive peer cannot stall close
indefinitely. Independent RPC peers must implement drain/drained for graceful
server shutdown; native transport ACKs or linger alone are not a substitute.

## Validation

`cargo test -p json-rpc-2-datagram --no-default-features --features srt`
runs local encrypted caller/listener, fragmentation, cancellation, notification,
batch, reconnect, limits and independent native-peer tests. No remote service
is required. An additional interop test launches `srt-live-transmit` when it
is installed (`SRT_LIVE_TRANSMIT` can select its executable), forwards native
SRT messages to a local UDP capture socket, and verifies our application frame
bytes. Missing external executable skips only that test, not native SRT.
