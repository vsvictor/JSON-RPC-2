# Відповідність стандартам та сумісність

Це матриця фактичних можливостей, а не сертифікат відповідності.
Наявність імені протоколу у feature або типі **не означає**, що реалізовано
повний стандарт або перевірено роботу зі сторонніми серверами.
Внутрішній round-trip перевіряє узгодженість власного клієнта й сервера,
але не є доказом interoperability.

## Статуси

- **Стандартний носій, власний binding**: стандартний мережевий стек,
  але прив'язка JSON-RPC та control traffic визначені цим проєктом.
- **Обмежений піднабір**: реалізовано частину формату або поведінки;
  повну сумісність не заявлено.
- **Власний профіль**: потрібен peer з таким самим framing; стандартний
  клієнт/брокер не можна вважати сумісним.
- **External interop**: є тест зі сторонньою реалізацією; він підтверджує
  лише перевірений сценарій.

## Матриця

| Поверхня | Фактично реалізовано | Межі та статус | Перевірка |
|---|---|---|---|
| JSON-RPC 2.0 | Request, notification, result/error, batch, зіставлення за `id`, стандартні коди помилок | Незалежний від транспорту протокол; runtime-ліміти не є частиною специфікації. Повна відповідність потребує проходження всіх нормативних правил | Server/property tests; HTTP interop з `jsonrpsee`; наведені нижче сценарії |
| TCP | Потік TCP через Tokio, JSON на рядок | Стандартний носій, власний newline binding і control payload; JSON-RPC не визначає TCP framing | Внутрішні e2e та контрактні тести |
| HTTP/1.1 | `POST /`, JSON body; порожня відповідь для notification | HTTP через бібліотечний стек; власний endpoint binding | External `jsonrpsee` + незалежні HTTP запити |
| QUIC | TLS/QUIC через Quinn, обмін на bidirectional stream | Стандартний носій, власні ALPN та framing; не універсальний JSON-RPC-over-QUIC стандарт | Внутрішні e2e; незалежний QUIC peer ще не перевірений |
| HTTP/3 | HTTP/3 через `h3`/Quinn, JSON body на request stream | Стандартний носій, власний endpoint binding | Внутрішні e2e; зовнішній HTTP/3 клієнт ще не перевірений |
| Reliable UDP | ACK, retry, packet ID, response cache | Власний профіль; не raw JSON datagram, не гарантія exactly-once | Внутрішні e2e та fault-injection перевірки |
| RTP/RTCP | Базовий RTP v2 header, payload type 96, власні kind/message ID; RTCP APP `JRP2` | Обмежений піднабір RFC 3550 з власним payload/control profile; не мультимедійний стек, не загальна реалізація RTCP reports | Незалежні byte vectors для header/APP і внутрішні e2e |
| RUDP | Власний packet header, sequence/ID, ACK/retry/cache | Власний профіль; назва RUDP не визначає єдиного універсального wire-стандарту | Внутрішні e2e |
| SRT | Native Haivision libsrt: caller/listener handshake, ARQ, congestion control, passphrase encryption | Стандартний носій, власний JSON-RPC message binding. Старий `JSRT` не є цим транспортом; довільний відеопотік SRT не є RPC peer | Native interoperability та transport contracts |
| UDP-Lite | Kernel socket та checksum coverage; власний reliable payload | Стандартний носій RFC 3828, власний application binding; потрібна підтримка ОС | Платформні тести; недоступний на macOS |
| DCCP | Kernel connected datagram socket | Стандартний носій, власний application binding; підтримка залежить від ядра | Linux/Android tests з явним пропуском за відсутності підтримки |
| SCTP | Kernel `SOCK_SEQPACKET`, request/response | Стандартний носій; наявність SCTP socket не доводить підтримку API multi-streaming або multi-homing | Платформні тести; окремі multi-stream/multi-home сценарії ще потрібні |
| MQTT 3.1.1 | Бібліотечний MQTT client, broker connection/subscription lifecycle, request/reply topics, acknowledgement та keepalive | Стандартний носій через зовнішній broker, власний JSON-RPC routing binding; RPC-сервіс не є MQTT broker. MQTT 5 не заявлено | Broker integration з Mosquitto; старі core codec vectors не є тестами нового стека |
| CoAP | Header v1, token, POST 0.02, ACK/Content 2.05, payload marker | Обмежений піднабір RFC 7252: немає загального options/URI routing, discovery, Observe, blockwise чи DTLS; empty ACK ping — локальна поведінка, не стандартний RST ping response | Byte vectors для базового POST/empty CON; зовнішній CoAP endpoint ще не перевірений |
| AMQP 0-9-1 | Бібліотечний connection/channel handshake, publish/consume, request/reply queues, correlation та acknowledgements | Стандартний носій через зовнішній broker, власний JSON-RPC routing binding; не AMQP 1.0 і не embedded broker | Broker integration з RabbitMQ; старі core codec vectors не є тестами нового стека |

## WebSocket: важливий напрям інкапсуляції

| Варіант | Фактичний контракт |
|---|---|
| HTTP/1.1 Upgrade | WebSocket framing через бібліотечний стек після HTTP Upgrade; клієнт приймає `ws://` і `wss://`. Серверний адаптер слухає звичайний TCP, тож TLS потрібно завершити перед ним; внутрішні тести не замінюють Autobahn conformance suite |
| HTTP/3 extended CONNECT | WebSocket-сесія через HTTP/3 carrier; незалежний RFC 9220 interop ще потрібний |
| Raw TCP / raw QUIC | WebSocket frame stream через власне встановлення з'єднання; не стандартний `ws://`/`wss://` endpoint з HTTP Upgrade |
| UDP, RTP, RUDP, SRT, UDP-Lite, DCCP, SCTP | Власна text/binary/close envelope поверх message-oriented носія; **не RFC 6455 поверх цих сокетів** |
| MQTT / AMQP | Власна envelope поверх стандартного broker delivery. Це **WebSocket-style over MQTT**, а не MQTT over WebSocket зі стандартної MQTT специфікації |
| CoAP | Власна envelope поверх обмеженого CoAP-профілю |

## Де перевіряти докази

- `json-rpc-2-core/tests/wire_profiles.rs`: фіксовані байтові вектори,
  незалежні від парного encoder/decoder. Стандартні layout subsets
  відділені від власних payload profiles.
- `json-rpc-2-server/tests/interop_external.rs`: JSON-RPC через незалежний
  `jsonrpsee` HTTP client та raw HTTP JSON payloads.
- Інтеграційні server/facade тести: локальні transport pairs та
  контрактні сценарії. Після виділення runtime частина server e2e
  може знаходитися у `json-rpc-2-runtime/tests/`.
- `RETRY-SEMANTICS.md`: межі ACK, повторів і дедуплікації.
- `json-rpc-2/tests/transport_contract.rs`: спільний harness для socket
  transport pairs — TCP, HTTP/1.1, UDP, RTP, RUDP, SRT,
  CoAP та WebSocket варіантів. Перевіряє echo,
  notifications, mixed batch, timeout, reconnect і close.
  TCP додатково продовжує роботу після timeout без явного reconnect;
  `tcp_cancellation.rs` перевіряє timeout, abort, queued cancellation,
  ping та частково записані notification.
  MQTT/AMQP потребують окремого запуску із зовнішніми брокерами.
  Caller cancellation перевіряється для message-oriented пар.
  QUIC/HTTP3 та платформні сокети залишаються у наявних runtime e2e,
  а не в цьому спільному harness.
- `json-rpc-2/tests/public_api.rs`: compile-перевірки публічних шляхів
  facade та старих aliases limits; це не повний semver-аудит API.

Відтворювані вузькі перевірки:

```sh
cargo test -p json-rpc-2-core --test wire_profiles
cargo test -p json-rpc-2-server --features http1 --test interop_external
cargo test -p json-rpc-2 --test transport_contract --test retry_dedup --test public_api
```

## Наступні роботи для справжньої зовнішньої сумісності

1. CoAP: додати options, Content-Format, URI і коректну ping/RST семантику;
   перевірити через незалежну CoAP бібліотеку.
2. WebSocket: додати Autobahn для HTTP/1.1 та незалежного HTTP/3 peer.

Ці роботи змінюють wire/API поведінку і не маскуються під механічне
перенесення крейтів або перейменування типів.

## Першоджерела

- [JSON-RPC 2.0](https://www.jsonrpc.org/specification), розділи 4–6.
- [MQTT 3.1.1, OASIS](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html),
  розділи 2–4 та 6.
- [AMQP 0-9-1](https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf).
- [CoAP, RFC 7252](https://www.rfc-editor.org/rfc/rfc7252),
  зокрема формати повідомлень та Empty Message.
- [RTP/RTCP, RFC 3550](https://www.rfc-editor.org/rfc/rfc3550).
- [UDP-Lite, RFC 3828](https://www.rfc-editor.org/rfc/rfc3828).
- [WebSocket, RFC 6455](https://www.rfc-editor.org/rfc/rfc6455).
- [WebSocket over HTTP/3, RFC 9220](https://www.rfc-editor.org/rfc/rfc9220).
- [QUIC, RFC 9000](https://www.rfc-editor.org/rfc/rfc9000).
- [HTTP semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110);
  [HTTP/1.1, RFC 9112](https://www.rfc-editor.org/rfc/rfc9112);
  [HTTP/3, RFC 9114](https://www.rfc-editor.org/rfc/rfc9114).
- [SRT protocol specification](https://github.com/Haivision/srt/blob/master/docs/API/API.md)
  та документація upstream libsrt.
