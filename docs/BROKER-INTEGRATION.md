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
{"payload":[123,125],"reply_to":"json-rpc-2/replies/unique-client","correlation_id":"unique-request","control":false}
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
