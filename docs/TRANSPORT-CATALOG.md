# Каталог транспортів

У workspace є **30 транспортних варіантів**: 15 базових носіїв і 15
WebSocket-style адаптерів поверх них. Описи нижче коротко пояснюють, як саме
проєкт переносить JSON-RPC; це не обіцянка повної відповідності кожному
мережевому стандарту.

## Базові транспорти

| Транспорт | Короткий опис |
|---|---|
| `tcp` | TCP-потік; кожне компактне JSON-RPC повідомлення завершується новим рядком. |
| `tls` | Той самий newline framing поверх перевіреного TLS-з'єднання. |
| `udp` | Датаграми з прикладними ACK, повторами й кешем дедуплікації; один пакет на повідомлення. |
| `rtp` | JSON-RPC повідомлення в RTP-пакетах; RTCP APP використовується для ACK і keepalive. |
| `rudp` | Власний надійний датаграмний профіль із sequence numbers, ACK і повторами. |
| `srt` | Нативний SRT через libsrt: надійна доставка повідомлень, контроль перевантаження й шифрування паролем. |
| `udplite` | UDP-Lite з налаштовуваним покриттям checksum і прикладною надійністю; Linux/Android. |
| `dccp` | Під'єднаний датаграмний носій із керуванням перевантаженням, але без гарантії доставки; Linux/Android. |
| `sctp` | Під'єднана SCTP-асоціація з межами повідомлень; Linux/Android. |
| `mqtt` | MQTT 3.1.1 через зовнішній брокер; запити й відповіді маршрутизуються окремими темами. |
| `coap` | Обмежений CoAP-піднабір поверх UDP для обміну JSON-RPC повідомленнями. |
| `amqp` | AMQP 0-9-1 через зовнішній брокер із чергами запитів/відповідей і кореляцією. |
| `http1` | JSON-RPC у тілі HTTP/1.1 `POST /`. |
| `quic` | Один обмін JSON-RPC на двонапрямний QUIC stream. |
| `http3` | JSON-RPC у HTTP/3 `POST /`, з окремим request stream для кожного запиту. |

## WebSocket-style адаптери

Ці варіанти зберігають text/binary/close семантику WebSocket-повідомлення,
але використовують відповідний носій для доставки.

| Транспорт | Короткий опис |
|---|---|
| `ws-tcp` | WebSocket-повідомлення через TCP; для HTTP WebSocket-сесії використовуйте `ws-http1`. |
| `ws-udp` | WebSocket-style envelope у надійних UDP-датаграмах. |
| `ws-rtp` | WebSocket-style envelope у RTP-пакетах із RTCP APP control traffic. |
| `ws-rudp` | WebSocket-style envelope у пакетах власного RUDP-профілю. |
| `ws-srt` | WebSocket-style envelope через нативний SRT/libsrt. |
| `ws-udplite` | WebSocket-style envelope у UDP-Lite-датаграмах; Linux/Android. |
| `ws-dccp` | WebSocket-style envelope через під'єднаний DCCP; Linux/Android. |
| `ws-sctp` | WebSocket-style envelope через SCTP-асоціацію; Linux/Android. |
| `ws-mqtt` | WebSocket-style envelope у повідомленнях MQTT через зовнішній брокер. |
| `ws-coap` | WebSocket-style envelope у повідомленнях обмеженого CoAP-профілю. |
| `ws-amqp` | WebSocket-style envelope у AMQP content frames через зовнішній брокер. |
| `ws-http1` | WebSocket після HTTP/1.1 Upgrade без TLS (`ws://`); клієнтський API також приймає `wss://` адреси для підключення до `ws-tls`. |
| `ws-tls` | Стандартний WebSocket через HTTP/1.1 Upgrade поверх TLS; сервер використовує сертифікат з `set_tls`, клієнт перевіряє ланцюжок і hostname. |
| `ws-quic` | WebSocket-style повідомлення через окремий QUIC stream bridge. |
| `ws-http3` | WebSocket-сесія через HTTP/3 extended CONNECT. |

> `ws-udp`, `ws-rtp`, `ws-rudp`, `ws-srt`, `ws-udplite`, `ws-dccp`,
> `ws-sctp`, `ws-mqtt`, `ws-coap`, `ws-amqp` і `ws-quic` використовують
> визначену проєктом WebSocket-style оболонку, а не стандартний RFC 6455
> endpoint на відповідному носії. `ws-tcp` також не є HTTP Upgrade endpoint;
> для цього призначений `ws-http1`.
>
> `wss://` обслуговує окремий runtime transport kind `ws-tls`, зі своїм
> портом (`--ws-tls-port` / `JSON_RPC_WS_TLS_PORT`) та TLS сертифікатом.
> Plain `ws-http1` лишається без змін.

Детальніше про framing, порядок, keepalive й обмеження: [семантика транспортів](TRANSPORTS.md).
Фактичний рівень відповідності стандартам і зовнішні перевірки наведені в
[матриці сумісності](CONFORMANCE.md). MQTT/AMQP потребують зовнішнього брокера;
UDP-Lite, DCCP і SCTP залежать від підтримки ОС/ядра.
