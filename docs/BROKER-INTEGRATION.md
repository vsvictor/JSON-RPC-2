# Real MQTT/AMQP integration

From the workspace root:

```sh
docker compose -f json-rpc-2-iot/tests/brokers/compose.yaml up -d --wait
fixture_user=rpc-test
fixture_password=rpc-test
export MQTT_BROKER_URL="mqtt://${fixture_user}:${fixture_password}@127.0.0.1:18883"
export AMQP_BROKER_URL="amqp://${fixture_user}:${fixture_password}@127.0.0.1:56723/%2f"
export JSON_RPC_TEST_MQTT_URL="$MQTT_BROKER_URL"
export JSON_RPC_TEST_AMQP_URL="$AMQP_BROKER_URL"
cargo test -p json-rpc-2-iot --test broker_rpc -- --include-ignored
cargo test -p json-rpc-2-iot --test broker_sessions -- --include-ignored
cargo test -p json-rpc-2-websockets --no-default-features --features websocket,mqtt,amqp --test broker_sessions -- --include-ignored
# Authenticated fixtures additionally verify bad credentials are rejected.
cargo test -p json-rpc-2-iot --test broker_auth -- --include-ignored
# Separately: these restart containers and must not overlap other broker tests.
cargo test -p json-rpc-2-iot --test broker_recovery -- --ignored --test-threads=1
docker compose -f json-rpc-2-iot/tests/brokers/compose.yaml down -v
```

Credentials above are public, isolated test fixtures. Brokers bind only loopback.
Tests explicitly fail if required environment variables are missing; ignored
tests never silently replace a broker with a fake endpoint.
Runtime/facade tests use `JSON_RPC_TEST_MQTT_URL` and `JSON_RPC_TEST_AMQP_URL`;
set these to the same URLs when invoking both test families.

## Broker contract

`MqttTransport::new(MqttConfig)` and `AmqpTransport::new(AmqpConfig)` create RPC
consumers, **not** listeners or brokers. Prefer asynchronous
`MqttTransport::connect(config).await` / `AmqpTransport::connect(config).await`
(and the equivalent WebSocket constructors) for startup: these return only after
the broker connection and subscription/consumer are established. `run` reuses
that prepared connection. Lazy `new(config)` remains available; its `run`
establishes the broker connection;
initial connect/subscribe failures are returned. After an established connection
is lost, the server recreates its consumer with one-second retry spacing.
Clients expose `connect(SocketAddr)` and `connect_with_config(config)`.
`reconnect()` recreates the unique reply subscription/queue and fails old pending
exchanges; it never replays requests whose outcome is unknown.

MQTT is standard 3.1.1, QoS 1, non-retained PUBLISH. Each application payload is:

```json
{"payload":[123,125],"reply_to":"json-rpc-2/replies/unique-client","correlation_id":"unique-request","control":false,"metadata":{"session":{"id":"550e8400-e29b-41d4-a716-446655440000","push_to":"json-rpc-2/replies/unique-client"},"kind":"rpc"}}
```

`payload` is a byte array (here `{}`). Notifications omit `reply_to`. Responses
echo `correlation_id` and omit `reply_to`. Response destinations must be exact
topics below `json-rpc-2/replies/`. Retained requests are ignored. Publishers wait
for PUBACK; MQTT PUBACK means broker receipt, not successful handler execution.
QoS 1 is at-least-once: applications must handle possible duplicate execution.
Servers subscribed to the same ordinary topic each receive the request; use
separate topics for separate services. This is not a shared-subscription extension.

AMQP is standard 0-9-1. Request bodies are unchanged bytes, using the default
exchange and configured request queue. Properties carry `correlation_id`,
`reply_to`, content type `application/json`, and type `json-rpc-2`. Replies echo
the correlation ID. Notifications have no reply-to. Clients use broker-generated
exclusive auto-delete reply queues. Request queues are non-durable and survive
consumer disconnects but not broker restart. Publications use mandatory routing
and publisher confirms. Consumer acknowledgements follow successful processing
and confirmed reply publication. Failed handling is negatively acknowledged
without requeue (dead-lettering may be configured externally). Unacknowledged
deliveries on connection loss may be redelivered; this is not exactly-once RPC.

Transport ping uses `control: true` (MQTT) or type `json-rpc-2.ping` (AMQP);
servers respond without entering the JSON-RPC handler. Payload/operation/response
limits apply to both. Connect includes subscribe/consume establishment. An idle
consumer waits indefinitely by design, but requests, publishes, acknowledgements,
connection setup, and shutdown are bounded. Cancelled calls discard their pending
correlation route so late replies cannot satisfy another request.

Credentials are supplied in broker URLs. `mqtts://` and `amqps://` enable verified
TLS using system trust; MQTT additionally accepts a PEM CA bundle. No URL or
password is included in adapter errors or peer identifiers. Configure broker ACLs
to restrict request topics/queues and private reply destinations.

WebSocket-family MQTT/AMQP adapters carry the existing WebSocket application
envelope *inside* the above broker RPC payload; they are not MQTT-over-WebSocket
or AMQP-over-WebSocket connections. Give them separate namespaces from raw RPC.

## Persistent ingress and unsolicited push

The built-in broker clients register a transport session after establishing their
private receive route. The `gateway-sessions` crate owns the session identity,
wire metadata, lifecycle kinds, lease policy, and bounded session registry;
JSON-RPC-2 broker adapters retain broker I/O and apply registry decisions to
transport session guards. Identity and push route are transport metadata,
including on notifications; they are never extracted from business JSON or RPC
IDs. The client creates a UUID identity, and the server allocates a transport
context once per registered client, calls `session_opened`, reuses it for
requests and notifications, and calls
`session_closed` on explicit client close, reconnect, lease expiration, broker
connection loss, or server shutdown. Reconnect creates a new client route and
context, without replaying application messages. Legacy external producers that
omit session metadata retain stateless behavior and do not gain push.

MQTT adds the `metadata` member shown above to its application envelope. AMQP
leaves the body unchanged and carries the same metadata JSON in the
`json-rpc-2-session` long-string header. `kind` is `rpc`, `open`, `heartbeat`,
`close`, or `push`; lifecycle messages have empty bodies and do not enter the
application protocol. Replies retain the request correlation ID. Unsolicited
push uses `kind: "push"`, no reply-to, an empty correlation ID, and the registered
client's private receive route. The client dispatches push by kind, independently
of its pending RPC map.

Use `context.push_sender` to send opaque application bytes. Both raw
`MqttClientTransport` / `AmqpClientTransport` and the broker WebSocket client
types expose `receive_push().await -> Result<Bytes, TransportError>`; this can
run concurrently with requests and also receives pushes queued before the first
call. For broker WebSockets, server push is encoded as a binary WebSocket
application frame and the client API decodes text/binary frames before returning
the payload. Context transport names are `mqtt`, `amqp`, `ws-mqtt`, and `ws-amqp`.
An incoming WebSocket Close envelope retires the broker session immediately.
The server sends transport `close` metadata to stop the client's receive loop
and heartbeat; a short-lived, bounded retirement set prevents queued heartbeats
from recreating that closed session. The registry retains closed identities for
the lease-plus-heartbeat window, with tombstone capacity bounded by the configured
session limit.

The receive queue holds 256 pushes. Overflow terminates the client reader rather
than blocking RPC replies; draining then returns a closed error, and explicit
reconnect establishes a fresh queue. Push is not a durable mailbox and queued
pushes are discarded on reconnect. Publish failures or oversize pushes terminate
the server push relay and retire that session on the next lease sweep.

Clients send heartbeat metadata every 10 seconds. Unexpected client loss is
detected by a 45-second inactivity lease, swept every 10 seconds (up to 55 seconds
without traffic). Active requests retain their session until completion or the
configured response timeout. Ingress keeps polling lifecycle messages when the
request concurrency limit is exhausted; excess application requests are rejected
without dispatch or replay (MQTT drops them; AMQP negatively acknowledges without
requeue). Rejected RPC calls time out rather than receiving synthetic business
responses. No additional application tasks or pending admission queue are created.
Explicit transport Close and heartbeats do not consume dispatcher capacity.
Defaults are a 10-second heartbeat, a 45-second inactivity lease, and a
10-second sweep: an abandoned idle session expires after 45–55 seconds under
normal broker progress, including when all dispatcher slots are occupied.
An already admitted request may retain its own session for its remaining
30-second default response timeout, followed by the idle lease (up to 85 seconds
from last ingress in the worst default case). Broker I/O and acknowledgements
have a separate 10-second default operation timeout; broker/network failure
can add operation delays. Immediate detection of another broker client's
socket disconnect is not available to an ordinary MQTT/AMQP consumer.
Broker protocols do not expose other clients' socket lifetimes
to ordinary consumers. Close is best effort if the broker is unavailable. Client
UUIDs/routes are routing identities, **not authentication**: configure broker
ACLs to prevent unauthorized producers from impersonating sessions or publishing
to another client's private route. MQTT fan-out to several gateway consumers
creates one server context on each consumer; use distinct ingress namespaces
where a single owner is required.
