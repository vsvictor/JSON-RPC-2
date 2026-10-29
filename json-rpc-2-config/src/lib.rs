use std::{
    fmt,
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use json_rpc_2::{
    AmqpConfig, MqttConfig, SrtConfig, TcpCongestionControl, TcpTuningConfig,
    TransportCapabilities, TransportKind, UdpLiteConfig, UdpReliabilityConfig,
    client::ReconnectPolicy, server::TransportLimits as ServerTransportLimits,
};

pub const SERVER_HOST: &str = "mobilespace.local";
const TRANSPORT_SLOT_COUNT: usize = TransportKind::ALL.len();
pub const USAGE: &str = "\
Usage:
  cargo run -p json-rpc-2-cli --bin JSON_RPC_2
                                  demo: server and clients in one process on the
                                  addresses of mobilespace.local, ports and
                                  certificate from .env
  cargo run -p json-rpc-2-cli --bin JSON_RPC_2 -- server [TRANSPORTS] [OPTIONS]
                                  one JSON-RPC server, runs until Ctrl+C
  cargo run -p json-rpc-2-cli --bin JSON_RPC_2 -- client [TRANSPORTS] [OPTIONS]
                                  call a running server

Settings come from (highest priority first): command-line options,
environment variables, the optional overlay file (JSON_RPC_ENV_OVERLAY),
.env in the project root (another base file: JSON_RPC_ENV). Options marked *
are required: set them in .env.

  TRANSPORTS            JSON_RPC_TRANSPORT      all (default) or a comma-separated list of
                                                tcp, tls, udp, rtp, rudp, srt, udplite, dccp, sctp, quic, http1, http3,
                                                mqtt, coap, amqp,
                                                ws-tcp, ws-udp, ws-rtp, ws-rudp, ws-srt, ws-udplite, ws-dccp, ws-sctp,
                                                ws-quic, ws-http1, ws-tls, ws-http3, ws-mqtt, ws-coap, ws-amqp
                                                (udplite/ws-udplite, dccp/ws-dccp and sctp/ws-sctp: Linux/Android only)
Server:
  --bind <IPS>          JSON_RPC_BIND           local addresses to listen on, comma-separated
                                                (required when any local-listener transport is selected):
                                                127.0.0.1,::1 (this machine only),
                                                0.0.0.0,:: (every interface, LAN),
                                                192.168.0.10 (one interface)
  --cert <PATH>         JSON_RPC_CERT           certificate chain, PEM
                                                (default: <temp dir>/json-rpc-2/cert.pem)
  --key <PATH>          JSON_RPC_KEY            private key, PEM; if unset, a self-signed
                                                pair is generated in memory and only the
                                                certificate is written
Client:
  --host <HOSTS>         JSON_RPC_HOST          server to call instead of the one in the code
                                                (mobilespace.local), comma-separated host
                                                names or IP addresses: 192.168.0.10
  --ca <PATH>           JSON_RPC_CA             certificates to trust (default: JSON_RPC_CERT)
  --server-name <NAME>  JSON_RPC_SERVER_NAME    TLS name to verify (default: the host)
  --max-missed-pongs <N> JSON_RPC_MAX_MISSED_PONGS
                                                consecutive missed pong ticks before
                                                restore() then reconnect() (default: 3)
  --request-timeout-ms <N> JSON_RPC_REQUEST_TIMEOUT_MS
                                                request timeout in milliseconds
  --reconnect-cooldown-ms <N> JSON_RPC_RECONNECT_COOLDOWN_MS
                                                initial reconnect backoff
  --reconnect-max-backoff-ms <N> JSON_RPC_RECONNECT_MAX_BACKOFF_MS
                                                max reconnect backoff
  --reconnect-jitter-ms <N> JSON_RPC_RECONNECT_JITTER_MS
                                                random extra jitter on reconnect waits
  --reconnect-max-elapsed-ms <N> JSON_RPC_RECONNECT_MAX_ELAPSED_MS
                                                total reconnect budget for one failure streak
  --tcp-nodelay <BOOL>      JSON_RPC_TCP_NODELAY
                                                disable Nagle on adapter-owned TCP sockets only
  --tcp-congestion <NAME>   JSON_RPC_TCP_CONGESTION
                                                system or bbr, adapter-owned TCP sockets only
  --tcp-fastopen <BOOL>     JSON_RPC_TCP_FASTOPEN
                                                enable TCP Fast Open on supported adapter-owned TCP sockets
  --tcp-fastopen-queue-len <N> JSON_RPC_TCP_FASTOPEN_QUEUE_LEN
                                                Fast Open listener queue length
TCP tuning applies only to tcp/tls/http1/ws-tcp/ws-http1/ws-tls, never MQTT/AMQP broker library sockets.
In mixed selections it affects only those local TCP adapters; broker socket tuning is not exposed.
  --udp-retransmit-ms <N>   JSON_RPC_UDP_RETRANSMIT_MS
                                                delay between UDP retransmits
  --udp-max-retransmits <N> JSON_RPC_UDP_MAX_RETRANSMITS
                                                number of UDP retransmits after first send
  --udp-max-packet-size <N> JSON_RPC_UDP_MAX_PACKET_SIZE
                                                maximum UDP datagram size in bytes
  --udp-dedup-cache-capacity <N> JSON_RPC_UDP_DEDUP_CACHE_CAPACITY
                                                per-peer deduplication cache size
  --udp-dedup-cache-ttl-ms <N> JSON_RPC_UDP_DEDUP_CACHE_TTL_MS
                                                deduplication cache ttl in milliseconds
  --udplite-send-cscov <N>     JSON_RPC_UDPLITE_SEND_CSCOV
                                                UDP-Lite outbound checksum coverage in bytes
                                                (0 = full coverage, >=8 = partial coverage)
  --udplite-recv-cscov <N>     JSON_RPC_UDPLITE_RECV_CSCOV
                                                UDP-Lite inbound checksum coverage in bytes
                                                (0 = full coverage, >=8 = partial coverage)
  --srt-passphrase <TEXT>       JSON_RPC_SRT_PASSPHRASE
                                                shared passphrase for srt and ws-srt
  --mqtt-url <URL>              JSON_RPC_MQTT_URL
                                                external MQTT 3.1.1 broker URL (mqtt:// or mqtts://);
                                                credentials may be in URL, mqtts uses system roots
  --mqtt-topic <TOPIC>           JSON_RPC_MQTT_TOPIC
                                                request topic for the MQTT RPC service
  --mqtt-ws-topic <TOPIC>        JSON_RPC_WS_MQTT_TOPIC
                                                request topic for WebSocket/MQTT
  --mqtt-operation-timeout-ms <N> JSON_RPC_MQTT_OPERATION_TIMEOUT_MS
                                                broker operation timeout
  --mqtt-response-timeout-ms <N> JSON_RPC_MQTT_RESPONSE_TIMEOUT_MS
                                                response timeout
  --mqtt-ca <PATH>               JSON_RPC_MQTT_CA
                                                optional PEM CA bundle for mqtts://
  --amqp-url <URL>               JSON_RPC_AMQP_URL
                                                external AMQP 0-9-1 broker URL (amqp:// or amqps://);
                                                credentials/vhost may be in URL, amqps uses system roots
  --amqp-queue <QUEUE>            JSON_RPC_AMQP_QUEUE
                                                request queue for the AMQP RPC service
  --amqp-ws-queue <QUEUE>         JSON_RPC_WS_AMQP_QUEUE
                                                request queue for WebSocket/AMQP
  --amqp-operation-timeout-ms <N> JSON_RPC_AMQP_OPERATION_TIMEOUT_MS
                                                broker operation timeout
  --amqp-response-timeout-ms <N> JSON_RPC_AMQP_RESPONSE_TIMEOUT_MS
                                                response timeout
Ports (server and client) apply to local socket transports and are required for each selected
local-listener transport. MQTT and AMQP use the external broker URL and service route above;
their legacy port options are deprecated and rejected:
  --tcp-port <N>      * JSON_RPC_TCP_PORT       TCP
  --tls-port <N>      * JSON_RPC_TLS_PORT       TLS-encrypted TCP
  --udp-port <N>      * JSON_RPC_UDP_PORT       reliable UDP
  --rtp-port <N>      * JSON_RPC_RTP_PORT       RTP/RTCP over UDP
  --rudp-port <N>     * JSON_RPC_RUDP_PORT      Reliable UDP with explicit sequencing
  --srt-port <N>      * JSON_RPC_SRT_PORT       Encrypted reliable UDP-style transport
  --udplite-port <N>  * JSON_RPC_UDPLITE_PORT   UDP-Lite with partial checksum coverage (Linux/Android only)
  --mqtt-port <N>       JSON_RPC_MQTT_PORT      deprecated; use --mqtt-url (external broker)
  --coap-port <N>     * JSON_RPC_COAP_PORT      CoAP-style constrained application transport over UDP
  --amqp-port <N>       JSON_RPC_AMQP_PORT      deprecated; use --amqp-url (external broker)
  --dccp-port <N>     * JSON_RPC_DCCP_PORT      DCCP (Linux/Android only)
  --sctp-port <N>     * JSON_RPC_SCTP_PORT      SCTP (Linux/Android only)
  --quic-port <N>     * JSON_RPC_QUIC_PORT      UDP
  --http1-port <N>    * JSON_RPC_HTTP1_PORT     TCP
  --http3-port <N>    * JSON_RPC_HTTP3_PORT     UDP
  --ws-tcp-port <N>   * JSON_RPC_WS_TCP_PORT    TCP
  --ws-udp-port <N>   * JSON_RPC_WS_UDP_PORT    UDP
  --ws-rtp-port <N>   * JSON_RPC_WS_RTP_PORT    RTP/RTCP over UDP
  --ws-rudp-port <N>  * JSON_RPC_WS_RUDP_PORT   WebSocket over reliable RUDP
  --ws-srt-port <N>   * JSON_RPC_WS_SRT_PORT    WebSocket over encrypted SRT-style UDP
  --ws-udplite-port <N> * JSON_RPC_WS_UDPLITE_PORT WebSocket over UDP-Lite (Linux/Android only)
  --ws-mqtt-port <N>    JSON_RPC_WS_MQTT_PORT   deprecated; use --mqtt-url and --mqtt-ws-topic
  --ws-coap-port <N>  * JSON_RPC_WS_COAP_PORT   WebSocket over CoAP-style transport
  --ws-amqp-port <N>    JSON_RPC_WS_AMQP_PORT   deprecated; use --amqp-url and --amqp-ws-queue
  --ws-dccp-port <N>  * JSON_RPC_WS_DCCP_PORT   DCCP (Linux/Android only)
  --ws-sctp-port <N>  * JSON_RPC_WS_SCTP_PORT   SCTP (Linux/Android only)
  --ws-quic-port <N>  * JSON_RPC_WS_QUIC_PORT   UDP
  --ws-http1-port <N> * JSON_RPC_WS_HTTP1_PORT  TCP
  --ws-tls-port <N>   * JSON_RPC_WS_TLS_PORT    TLS-encrypted TCP (WSS)
  --ws-http3-port <N> * JSON_RPC_WS_HTTP3_PORT  UDP

  -h, --help                                    show this help";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvFiles {
    pub base: PathBuf,
    pub overlay: Option<PathBuf>,
}

fn parse_u32(value: &str) -> Result<u32, String> {
    value
        .parse()
        .map_err(|_| format!("invalid integer {value}"))
}

impl EnvFiles {
    pub fn discover() -> Self {
        Self {
            base: setting("JSON_RPC_ENV")
                .map(PathBuf::from)
                .unwrap_or_else(|| workspace_root().join(".env")),
            overlay: setting("JSON_RPC_ENV_OVERLAY").map(PathBuf::from),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptProfiles {
    pub bind_ipv4: IpAddr,
    pub bind_ipv6: IpAddr,
    pub bind_lan: Vec<IpAddr>,
    pub host_ipv4: String,
    pub host_ipv6: String,
    pub e2e_bind: Vec<IpAddr>,
    pub e2e_host: Vec<String>,
    pub e2e_http1_host: String,
    pub e2e_port_base: u16,
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub transports: Vec<TransportKind>,
    pub bind: Vec<IpAddr>,
    pub hosts: Vec<String>,
    pub ports: [Option<u16>; TRANSPORT_SLOT_COUNT],
    pub max_missed_pongs: u32,
    pub request_timeout: Duration,
    pub reconnect: ReconnectPolicy,
    pub server_limits: ServerTransportLimits,
    /// Applies only to adapter-owned TCP/HTTP1/WebSocket TCP sockets, not broker library sockets.
    pub tcp_tuning: TcpTuningConfig,
    pub udp_reliability: UdpReliabilityConfig,
    pub udplite: UdpLiteConfig,
    pub srt: Option<SrtConfig>,
    pub mqtt_url: String,
    pub mqtt_topic: String,
    pub mqtt_ws_topic: String,
    pub mqtt_operation_timeout: Duration,
    pub mqtt_response_timeout: Duration,
    pub mqtt_ca: Option<PathBuf>,
    pub amqp_url: String,
    pub amqp_queue: String,
    pub amqp_ws_queue: String,
    pub amqp_operation_timeout: Duration,
    pub amqp_response_timeout: Duration,
    pub cert: PathBuf,
    pub key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
    pub server_name: Option<String>,
    pub scripts: ScriptProfiles,
}

impl fmt::Debug for RuntimeOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeOptions")
            .field("transports", &self.transports)
            .field("bind", &self.bind)
            .field("hosts", &self.hosts)
            .field("ports", &self.ports)
            .field("max_missed_pongs", &self.max_missed_pongs)
            .field("request_timeout", &self.request_timeout)
            .field("reconnect", &self.reconnect)
            .field("server_limits", &self.server_limits)
            .field("tcp_tuning", &self.tcp_tuning)
            .field("udp_reliability", &self.udp_reliability)
            .field("udplite", &self.udplite)
            .field("srt", &self.srt.as_ref().map(|_| "<redacted>"))
            .field("mqtt_url", &"<redacted>")
            .field("mqtt_topic", &self.mqtt_topic)
            .field("mqtt_ws_topic", &self.mqtt_ws_topic)
            .field("mqtt_operation_timeout", &self.mqtt_operation_timeout)
            .field("mqtt_response_timeout", &self.mqtt_response_timeout)
            .field("mqtt_ca", &self.mqtt_ca)
            .field("amqp_url", &"<redacted>")
            .field("amqp_queue", &self.amqp_queue)
            .field("amqp_ws_queue", &self.amqp_ws_queue)
            .field("amqp_operation_timeout", &self.amqp_operation_timeout)
            .field("amqp_response_timeout", &self.amqp_response_timeout)
            .field("cert", &self.cert)
            .field("key", &self.key)
            .field("ca", &self.ca)
            .field("server_name", &self.server_name)
            .field("scripts", &self.scripts)
            .finish()
    }
}

impl RuntimeOptions {
    pub fn from_env() -> Result<Self, String> {
        let options = Self::from_env_unvalidated()?;
        validate_runtime_options(&options)?;
        Ok(options)
    }

    fn from_env_unvalidated() -> Result<Self, String> {
        let mut ports = [None; TRANSPORT_SLOT_COUNT];
        for kind in TransportKind::ALL.iter().copied() {
            ports[kind.index()] = parse_setting(kind.port_setting(), parse_port)?;
        }

        let reconnect_cooldown =
            parse_setting("JSON_RPC_RECONNECT_COOLDOWN_MS", parse_duration_ms)?
                .unwrap_or(Duration::ZERO);
        let max_reconnect_backoff =
            parse_setting("JSON_RPC_RECONNECT_MAX_BACKOFF_MS", parse_duration_ms)?
                .unwrap_or(Duration::ZERO);
        let reconnect_jitter = parse_setting("JSON_RPC_RECONNECT_JITTER_MS", parse_duration_ms)?
            .unwrap_or(Duration::ZERO);
        let max_reconnect_elapsed =
            parse_setting("JSON_RPC_RECONNECT_MAX_ELAPSED_MS", parse_duration_ms)?;

        let options = Self {
            transports: parse_setting("JSON_RPC_TRANSPORT", parse_transports)?
                .unwrap_or(TransportKind::ALL.to_vec()),
            bind: parse_setting("JSON_RPC_BIND", parse_bind)?.unwrap_or_default(),
            hosts: parse_setting("JSON_RPC_HOST", parse_hosts)?
                .unwrap_or_else(|| vec![SERVER_HOST.to_owned()]),
            ports,
            max_missed_pongs: parse_setting("JSON_RPC_MAX_MISSED_PONGS", parse_positive_u32)?
                .unwrap_or(3),
            request_timeout: parse_setting("JSON_RPC_REQUEST_TIMEOUT_MS", parse_duration_ms)?
                .unwrap_or(Duration::from_secs(30)),
            reconnect: ReconnectPolicy {
                restore_before_reconnect: true,
                reconnect_cooldown,
                max_reconnect_backoff,
                reconnect_jitter,
                max_reconnect_elapsed,
            },
            server_limits: ServerTransportLimits {
                max_connections: parse_setting("JSON_RPC_SERVER_MAX_CONNECTIONS", parse_usize)?
                    .unwrap_or(4096),
                max_concurrent_requests: parse_setting(
                    "JSON_RPC_SERVER_MAX_CONCURRENT_REQUESTS",
                    parse_usize,
                )?
                .unwrap_or(1024),
            },
            tcp_tuning: TcpTuningConfig {
                no_delay: parse_setting("JSON_RPC_TCP_NODELAY", parse_bool)?.unwrap_or(true),
                congestion_control: parse_setting("JSON_RPC_TCP_CONGESTION", parse_tcp_congestion)?
                    .unwrap_or_default(),
                fast_open: parse_setting("JSON_RPC_TCP_FASTOPEN", parse_bool)?.unwrap_or(false),
                fast_open_queue_len: parse_setting(
                    "JSON_RPC_TCP_FASTOPEN_QUEUE_LEN",
                    parse_positive_u32,
                )?
                .unwrap_or(1024),
            },
            udp_reliability: UdpReliabilityConfig {
                retransmit_interval: parse_setting(
                    "JSON_RPC_UDP_RETRANSMIT_MS",
                    parse_duration_ms,
                )?
                .unwrap_or(Duration::from_millis(50)),
                max_retransmits: parse_setting("JSON_RPC_UDP_MAX_RETRANSMITS", parse_u32)?
                    .unwrap_or(5),
                max_packet_size: parse_setting("JSON_RPC_UDP_MAX_PACKET_SIZE", parse_usize)?
                    .unwrap_or(4096),
                dedup_cache_capacity: parse_setting(
                    "JSON_RPC_UDP_DEDUP_CACHE_CAPACITY",
                    parse_usize,
                )?
                .unwrap_or(4096),
                dedup_cache_ttl: parse_setting(
                    "JSON_RPC_UDP_DEDUP_CACHE_TTL_MS",
                    parse_duration_ms,
                )?
                .unwrap_or(Duration::from_secs(30)),
            },
            udplite: UdpLiteConfig {
                send_checksum_coverage: parse_setting("JSON_RPC_UDPLITE_SEND_CSCOV", parse_u16)?
                    .unwrap_or(8),
                recv_checksum_coverage: parse_setting("JSON_RPC_UDPLITE_RECV_CSCOV", parse_u16)?
                    .unwrap_or(8),
            },
            srt: setting("JSON_RPC_SRT_PASSPHRASE").map(|passphrase| SrtConfig { passphrase }),
            mqtt_url: setting("JSON_RPC_MQTT_URL")
                .unwrap_or_else(|| "mqtt://127.0.0.1:1883".to_owned()),
            mqtt_topic: setting("JSON_RPC_MQTT_TOPIC")
                .unwrap_or_else(|| "json-rpc-2/requests".to_owned()),
            mqtt_ws_topic: setting("JSON_RPC_WS_MQTT_TOPIC")
                .unwrap_or_else(|| "json-rpc-2/ws/requests".to_owned()),
            mqtt_operation_timeout: parse_setting(
                "JSON_RPC_MQTT_OPERATION_TIMEOUT_MS",
                parse_duration_ms,
            )?
            .unwrap_or(Duration::from_secs(10)),
            mqtt_response_timeout: parse_setting(
                "JSON_RPC_MQTT_RESPONSE_TIMEOUT_MS",
                parse_duration_ms,
            )?
            .unwrap_or(Duration::from_secs(30)),
            mqtt_ca: setting("JSON_RPC_MQTT_CA").map(PathBuf::from),
            amqp_url: setting("JSON_RPC_AMQP_URL")
                .unwrap_or_else(|| "amqp://guest:guest@127.0.0.1:5672/%2f".to_owned()),
            amqp_queue: setting("JSON_RPC_AMQP_QUEUE")
                .unwrap_or_else(|| "json-rpc-2.requests".to_owned()),
            amqp_ws_queue: setting("JSON_RPC_WS_AMQP_QUEUE")
                .unwrap_or_else(|| "json-rpc-2.ws.requests".to_owned()),
            amqp_operation_timeout: parse_setting(
                "JSON_RPC_AMQP_OPERATION_TIMEOUT_MS",
                parse_duration_ms,
            )?
            .unwrap_or(Duration::from_secs(10)),
            amqp_response_timeout: parse_setting(
                "JSON_RPC_AMQP_RESPONSE_TIMEOUT_MS",
                parse_duration_ms,
            )?
            .unwrap_or(Duration::from_secs(30)),
            cert: setting("JSON_RPC_CERT").map_or_else(
                || std::env::temp_dir().join("json-rpc-2").join("cert.pem"),
                PathBuf::from,
            ),
            key: setting("JSON_RPC_KEY").map(PathBuf::from),
            ca: setting("JSON_RPC_CA").map(PathBuf::from),
            server_name: setting("JSON_RPC_SERVER_NAME"),
            scripts: ScriptProfiles {
                bind_ipv4: parse_setting("JSON_RPC_BIND_IPV4", parse_single_ip)?
                    .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
                bind_ipv6: parse_setting("JSON_RPC_BIND_IPV6", parse_single_ip)?
                    .unwrap_or(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)),
                bind_lan: parse_setting("JSON_RPC_BIND_LAN", parse_bind)?
                    .unwrap_or_else(|| parse_bind("0.0.0.0,::").expect("default bind")),
                host_ipv4: setting("JSON_RPC_HOST_IPV4").unwrap_or_else(|| "127.0.0.1".to_owned()),
                host_ipv6: setting("JSON_RPC_HOST_IPV6").unwrap_or_else(|| "::1".to_owned()),
                e2e_bind: parse_setting("JSON_RPC_E2E_BIND", parse_bind)?
                    .unwrap_or_else(|| parse_bind("127.0.0.1,::1").expect("default e2e bind")),
                e2e_host: parse_setting("JSON_RPC_E2E_HOST", parse_hosts)?
                    .unwrap_or_else(|| vec!["127.0.0.1".to_owned(), "::1".to_owned()]),
                e2e_http1_host: setting("JSON_RPC_E2E_HTTP1_HOST")
                    .unwrap_or_else(|| "127.0.0.1".to_owned()),
                e2e_port_base: parse_setting("JSON_RPC_E2E_PORT_BASE", parse_port)?
                    .unwrap_or(41_400),
            },
        };
        Ok(options)
    }

    pub fn port(&self, kind: TransportKind) -> u16 {
        self.ports[kind.index()].expect("port checked")
    }

    pub fn mqtt_config(&self, websocket: bool) -> Result<MqttConfig, String> {
        let mut config = MqttConfig::default();
        config.broker_url = self.mqtt_url.clone();
        config.request_topic = if websocket {
            self.mqtt_ws_topic.clone()
        } else {
            self.mqtt_topic.clone()
        };
        config.operation_timeout = self.mqtt_operation_timeout;
        config.response_timeout = self.mqtt_response_timeout;
        config.ca_certificate = self
            .mqtt_ca
            .as_ref()
            .map(|path| {
                std::fs::read(path).map_err(|error| {
                    format!("cannot read MQTT CA file {}: {error}", path.display())
                })
            })
            .transpose()?;
        Ok(config)
    }

    pub fn amqp_config(&self, websocket: bool) -> AmqpConfig {
        let mut config = AmqpConfig::default();
        config.broker_url = self.amqp_url.clone();
        config.request_queue = if websocket {
            self.amqp_ws_queue.clone()
        } else {
            self.amqp_queue.clone()
        };
        config.operation_timeout = self.amqp_operation_timeout;
        config.response_timeout = self.amqp_response_timeout;
        config
    }

    pub fn check_ports(&self) -> Result<(), String> {
        for kind in self
            .transports
            .iter()
            .copied()
            .filter(|kind| kind.is_broker())
        {
            if self.ports[kind.index()].is_some() {
                let setting = match kind {
                    TransportKind::Mqtt | TransportKind::WebSocketMqtt => "JSON_RPC_MQTT_URL",
                    TransportKind::Amqp | TransportKind::WebSocketAmqp => "JSON_RPC_AMQP_URL",
                    _ => unreachable!("only broker transports reach this branch"),
                };
                return Err(format!(
                    "{} is an external broker endpoint, not a local listener; configure {setting} instead",
                    kind.port_setting()
                ));
            }
        }
        let missing: Vec<String> = self
            .transports
            .iter()
            .filter(|kind| !kind.is_broker() && self.ports[kind.index()].is_none())
            .map(|kind| format!("{} ({})", kind.port_setting(), kind.port_option()))
            .collect();
        if !missing.is_empty() {
            return Err(format!("port not set: {}", missing.join(", ")));
        }
        for (index, first) in self.transports.iter().enumerate() {
            for second in &self.transports[index + 1..] {
                if !first.is_broker()
                    && !second.is_broker()
                    && first.socket() == second.socket()
                    && self.port(*first) == self.port(*second)
                {
                    return Err(format!(
                        "{} and {} use the same {} port {}",
                        first.port_setting(),
                        second.port_setting(),
                        if first.id().contains("dccp") {
                            "DCCP"
                        } else if first.id().contains("sctp") {
                            "SCTP"
                        } else if first.socket().is_udp() {
                            "UDP"
                        } else {
                            "TCP"
                        },
                        self.port(*first)
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandConfig {
    Demo(RuntimeOptions),
    Server(RuntimeOptions),
    Client(RuntimeOptions),
    Help,
}

pub fn parse_command(mut args: impl Iterator<Item = String>) -> Result<CommandConfig, String> {
    let client = match args.next().as_deref() {
        None | Some("demo") => {
            let mut options = RuntimeOptions::from_env_unvalidated()?;
            options.transports = TransportKind::ALL.to_vec();
            validate_runtime_options(&options)?;
            options.check_ports()?;
            return Ok(CommandConfig::Demo(options));
        }
        Some("-h" | "--help" | "help") => return Ok(CommandConfig::Help),
        Some("server") => false,
        Some("client") => true,
        Some(other) => return Err(format!("unknown command {other}")),
    };

    let mut options = RuntimeOptions::from_env_unvalidated()?;
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "-h" | "--help" => return Ok(CommandConfig::Help),
            "--bind" if !client => options.bind = parse_bind(&value()?)?,
            "--cert" => options.cert = value()?.into(),
            "--key" if !client => options.key = Some(value()?.into()),
            "--host" if client => options.hosts = parse_hosts(&value()?)?,
            "--ca" if client => options.ca = Some(value()?.into()),
            "--server-name" if client => options.server_name = Some(value()?),
            "--max-missed-pongs" if client => {
                options.max_missed_pongs = parse_positive_u32(&value()?)
                    .map_err(|error| format!("--max-missed-pongs: {error}"))?;
            }
            "--request-timeout-ms" if client => {
                options.request_timeout = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--request-timeout-ms: {error}"))?;
            }
            "--reconnect-cooldown-ms" if client => {
                options.reconnect.reconnect_cooldown = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--reconnect-cooldown-ms: {error}"))?;
            }
            "--reconnect-max-backoff-ms" if client => {
                options.reconnect.max_reconnect_backoff = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--reconnect-max-backoff-ms: {error}"))?;
            }
            "--reconnect-jitter-ms" if client => {
                options.reconnect.reconnect_jitter = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--reconnect-jitter-ms: {error}"))?;
            }
            "--reconnect-max-elapsed-ms" if client => {
                options.reconnect.max_reconnect_elapsed = Some(
                    parse_duration_ms(&value()?)
                        .map_err(|error| format!("--reconnect-max-elapsed-ms: {error}"))?,
                );
            }
            "--tcp-nodelay" => {
                options.tcp_tuning.no_delay =
                    parse_bool(&value()?).map_err(|error| format!("--tcp-nodelay: {error}"))?;
            }
            "--tcp-congestion" => {
                options.tcp_tuning.congestion_control = parse_tcp_congestion(&value()?)
                    .map_err(|error| format!("--tcp-congestion: {error}"))?;
            }
            "--tcp-fastopen" => {
                options.tcp_tuning.fast_open =
                    parse_bool(&value()?).map_err(|error| format!("--tcp-fastopen: {error}"))?;
            }
            "--tcp-fastopen-queue-len" => {
                options.tcp_tuning.fast_open_queue_len = parse_positive_u32(&value()?)
                    .map_err(|error| format!("--tcp-fastopen-queue-len: {error}"))?;
            }
            "--udp-retransmit-ms" => {
                options.udp_reliability.retransmit_interval = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--udp-retransmit-ms: {error}"))?;
            }
            "--udp-max-retransmits" => {
                options.udp_reliability.max_retransmits = parse_u32(&value()?)
                    .map_err(|error| format!("--udp-max-retransmits: {error}"))?;
            }
            "--udp-max-packet-size" => {
                options.udp_reliability.max_packet_size = parse_usize(&value()?)
                    .map_err(|error| format!("--udp-max-packet-size: {error}"))?;
            }
            "--udp-dedup-cache-capacity" => {
                options.udp_reliability.dedup_cache_capacity = parse_usize(&value()?)
                    .map_err(|error| format!("--udp-dedup-cache-capacity: {error}"))?;
            }
            "--udp-dedup-cache-ttl-ms" => {
                options.udp_reliability.dedup_cache_ttl = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--udp-dedup-cache-ttl-ms: {error}"))?;
            }
            "--udplite-send-cscov" => {
                options.udplite.send_checksum_coverage = parse_u16(&value()?)
                    .map_err(|error| format!("--udplite-send-cscov: {error}"))?;
            }
            "--udplite-recv-cscov" => {
                options.udplite.recv_checksum_coverage = parse_u16(&value()?)
                    .map_err(|error| format!("--udplite-recv-cscov: {error}"))?;
            }
            "--srt-passphrase" => {
                options.srt = Some(SrtConfig {
                    passphrase: value()?,
                });
            }
            "--mqtt-url" => options.mqtt_url = value()?,
            "--mqtt-topic" => options.mqtt_topic = value()?,
            "--mqtt-ws-topic" => options.mqtt_ws_topic = value()?,
            "--mqtt-operation-timeout-ms" => {
                options.mqtt_operation_timeout = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--mqtt-operation-timeout-ms: {error}"))?;
            }
            "--mqtt-response-timeout-ms" => {
                options.mqtt_response_timeout = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--mqtt-response-timeout-ms: {error}"))?;
            }
            "--mqtt-ca" => options.mqtt_ca = Some(value()?.into()),
            "--amqp-url" => options.amqp_url = value()?,
            "--amqp-queue" => options.amqp_queue = value()?,
            "--amqp-ws-queue" => options.amqp_ws_queue = value()?,
            "--amqp-operation-timeout-ms" => {
                options.amqp_operation_timeout = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--amqp-operation-timeout-ms: {error}"))?;
            }
            "--amqp-response-timeout-ms" => {
                options.amqp_response_timeout = parse_duration_ms(&value()?)
                    .map_err(|error| format!("--amqp-response-timeout-ms: {error}"))?;
            }
            option if option.ends_with("-port") => {
                let kind = TransportKind::ALL
                    .iter()
                    .copied()
                    .find(|kind| kind.port_option() == option)
                    .ok_or(format!("unknown option {option}"))?;
                options.ports[kind.index()] =
                    Some(parse_port(&value()?).map_err(|error| format!("{option}: {error}"))?);
            }
            transports if !transports.starts_with('-') => {
                options.transports = parse_transports(transports)?;
            }
            other => return Err(format!("unexpected argument {other}")),
        }
    }

    validate_runtime_options(&options)?;
    options.check_ports()?;
    if client {
        Ok(CommandConfig::Client(options))
    } else if options.bind.is_empty() && options.transports.iter().any(|kind| !kind.is_broker()) {
        Err("listen address not set: JSON_RPC_BIND (--bind)".into())
    } else {
        Ok(CommandConfig::Server(options))
    }
}

pub fn load_env() -> Result<EnvFiles, Box<dyn std::error::Error + Send + Sync>> {
    let files = EnvFiles::discover();
    let root = workspace_root();
    let mut merged = std::collections::BTreeMap::new();
    let mut load_file = |path: PathBuf| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let entries = match dotenvy::from_path_iter(&path) {
            Ok(entries) => entries,
            Err(error) if error.not_found() => return Ok(()),
            Err(error) => return Err(format!("{}: {error}", path.display()).into()),
        };
        for entry in entries {
            let (key, mut value) = entry.map_err(|error| format!("{}: {error}", path.display()))?;
            if PATH_SETTINGS.contains(&key.as_str()) && !value.is_empty() {
                value = root.join(value).to_string_lossy().into_owned();
            }
            merged.insert(key, value);
        }
        Ok(())
    };
    load_file(files.base.clone())?;
    if let Some(path) = &files.overlay {
        load_file(path.clone())?;
    }
    for (key, value) in merged {
        if std::env::var_os(&key).is_none() {
            unsafe { std::env::set_var(key, value) };
        }
    }
    Ok(files)
}

pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace layout")
        .to_path_buf()
}

trait KindSettings {
    fn index(self) -> usize;
    fn port_setting(self) -> &'static str;
    fn port_option(self) -> String;
}

impl KindSettings for TransportKind {
    fn index(self) -> usize {
        TransportKind::ALL
            .iter()
            .position(|kind| *kind == self)
            .unwrap()
    }

    fn port_setting(self) -> &'static str {
        match self {
            TransportKind::Tcp => "JSON_RPC_TCP_PORT",
            TransportKind::Tls => "JSON_RPC_TLS_PORT",
            TransportKind::Udp => "JSON_RPC_UDP_PORT",
            TransportKind::Rtp => "JSON_RPC_RTP_PORT",
            TransportKind::Rudp => "JSON_RPC_RUDP_PORT",
            TransportKind::Srt => "JSON_RPC_SRT_PORT",
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportKind::UdpLite => "JSON_RPC_UDPLITE_PORT",
            TransportKind::Mqtt => "JSON_RPC_MQTT_PORT",
            TransportKind::Coap => "JSON_RPC_COAP_PORT",
            TransportKind::Amqp => "JSON_RPC_AMQP_PORT",
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportKind::Dccp => "JSON_RPC_DCCP_PORT",
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportKind::Sctp => "JSON_RPC_SCTP_PORT",
            TransportKind::Quic => "JSON_RPC_QUIC_PORT",
            TransportKind::Http1 => "JSON_RPC_HTTP1_PORT",
            TransportKind::Http3 => "JSON_RPC_HTTP3_PORT",
            TransportKind::WebSocketTcp => "JSON_RPC_WS_TCP_PORT",
            TransportKind::WebSocketUdp => "JSON_RPC_WS_UDP_PORT",
            TransportKind::WebSocketRtp => "JSON_RPC_WS_RTP_PORT",
            TransportKind::WebSocketRudp => "JSON_RPC_WS_RUDP_PORT",
            TransportKind::WebSocketSrt => "JSON_RPC_WS_SRT_PORT",
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportKind::WebSocketUdpLite => "JSON_RPC_WS_UDPLITE_PORT",
            TransportKind::WebSocketMqtt => "JSON_RPC_WS_MQTT_PORT",
            TransportKind::WebSocketCoap => "JSON_RPC_WS_COAP_PORT",
            TransportKind::WebSocketAmqp => "JSON_RPC_WS_AMQP_PORT",
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportKind::WebSocketDccp => "JSON_RPC_WS_DCCP_PORT",
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportKind::WebSocketSctp => "JSON_RPC_WS_SCTP_PORT",
            TransportKind::WebSocketQuic => "JSON_RPC_WS_QUIC_PORT",
            TransportKind::WebSocketHttp1 => "JSON_RPC_WS_HTTP1_PORT",
            TransportKind::WebSocketTls => "JSON_RPC_WS_TLS_PORT",
            TransportKind::WebSocketHttp3 => "JSON_RPC_WS_HTTP3_PORT",
        }
    }

    fn port_option(self) -> String {
        format!("--{}-port", self.id())
    }
}

const PATH_SETTINGS: [&str; 4] = [
    "JSON_RPC_CERT",
    "JSON_RPC_KEY",
    "JSON_RPC_CA",
    "JSON_RPC_MQTT_CA",
];

fn setting(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn parse_setting<T>(
    name: &str,
    parse: impl FnOnce(&str) -> Result<T, String>,
) -> Result<Option<T>, String> {
    setting(name)
        .map(|value| parse(&value).map_err(|error| format!("{name}: {error}")))
        .transpose()
}

fn parse_transports(value: &str) -> Result<Vec<TransportKind>, String> {
    if value == "all" {
        return Ok(TransportKind::ALL.to_vec());
    }
    value
        .split(',')
        .map(|id| TransportKind::from_id(id.trim()).ok_or(format!("unknown transport {id}")))
        .collect()
}

fn split_list(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
}

fn parse_bind(value: &str) -> Result<Vec<IpAddr>, String> {
    let addresses = split_list(value)
        .map(|address| {
            address
                .trim_matches(['[', ']'])
                .parse()
                .map_err(|_| format!("invalid IP address {address}"))
        })
        .collect::<Result<Vec<IpAddr>, String>>()?;
    if addresses.is_empty() {
        return Err("no address".into());
    }
    Ok(addresses)
}

fn parse_single_ip(value: &str) -> Result<IpAddr, String> {
    let mut addresses = parse_bind(value)?;
    if addresses.len() != 1 {
        return Err("expected exactly one IP address".into());
    }
    Ok(addresses.remove(0))
}

fn parse_hosts(value: &str) -> Result<Vec<String>, String> {
    let hosts: Vec<String> = split_list(value)
        .map(|host| host.trim_matches(['[', ']']).to_owned())
        .collect();
    if hosts.is_empty() {
        return Err("no host".into());
    }
    Ok(hosts)
}

fn parse_port(value: &str) -> Result<u16, String> {
    value
        .parse()
        .ok()
        .filter(|port| *port != 0)
        .ok_or(format!("invalid port {value}"))
}

fn parse_positive_u32(value: &str) -> Result<u32, String> {
    value
        .parse()
        .ok()
        .filter(|count| *count > 0)
        .ok_or(format!("invalid positive integer {value}"))
}

fn parse_u16(value: &str) -> Result<u16, String> {
    value
        .parse()
        .map_err(|_| format!("invalid integer {value}"))
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid boolean {value}")),
    }
}

fn parse_tcp_congestion(value: &str) -> Result<TcpCongestionControl, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "system" | "default" => Ok(TcpCongestionControl::SystemDefault),
        "bbr" => Ok(TcpCongestionControl::Bbr),
        _ => Err(format!(
            "invalid TCP congestion profile {value} (expected system or bbr)"
        )),
    }
}

fn parse_usize(value: &str) -> Result<usize, String> {
    value
        .parse()
        .ok()
        .filter(|count| *count > 0)
        .ok_or(format!("invalid positive integer {value}"))
}

fn parse_duration_ms(value: &str) -> Result<Duration, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|duration| *duration > 0)
        .map(Duration::from_millis)
        .ok_or(format!("invalid positive integer {value}"))
}

fn validate_runtime_options(options: &RuntimeOptions) -> Result<(), String> {
    if options.request_timeout.is_zero() {
        return Err("request timeout must be greater than zero".into());
    }
    if options.max_missed_pongs == 0 {
        return Err("max missed pongs must be at least 1".into());
    }
    if !options.reconnect.max_reconnect_backoff.is_zero()
        && !options.reconnect.reconnect_cooldown.is_zero()
        && options.reconnect.max_reconnect_backoff < options.reconnect.reconnect_cooldown
    {
        return Err(
            "JSON_RPC_RECONNECT_MAX_BACKOFF_MS must be greater than or equal to JSON_RPC_RECONNECT_COOLDOWN_MS"
                .into(),
        );
    }
    if matches!(
        options.reconnect.max_reconnect_elapsed,
        Some(limit) if limit.is_zero()
    ) {
        return Err("JSON_RPC_RECONNECT_MAX_ELAPSED_MS must be greater than zero".into());
    }
    if options.tcp_tuning.fast_open && options.tcp_tuning.fast_open_queue_len == 0 {
        return Err("JSON_RPC_TCP_FASTOPEN_QUEUE_LEN must be at least 1".into());
    }
    if options.udp_reliability.retransmit_interval.is_zero() {
        return Err("JSON_RPC_UDP_RETRANSMIT_MS must be greater than zero".into());
    }
    if options.udp_reliability.max_packet_size > 65_507 {
        return Err("JSON_RPC_UDP_MAX_PACKET_SIZE must not exceed 65507".into());
    }
    if options.udp_reliability.max_packet_size < 64 {
        return Err("JSON_RPC_UDP_MAX_PACKET_SIZE must be at least 64".into());
    }
    if options.udp_reliability.dedup_cache_capacity == 0 {
        return Err("JSON_RPC_UDP_DEDUP_CACHE_CAPACITY must be at least 1".into());
    }
    if options.udp_reliability.dedup_cache_ttl.is_zero() {
        return Err("JSON_RPC_UDP_DEDUP_CACHE_TTL_MS must be greater than zero".into());
    }
    options.udplite.validate()?;
    if options
        .transports
        .iter()
        .any(|kind| matches!(kind, TransportKind::Srt | TransportKind::WebSocketSrt))
        && options.srt.is_none()
    {
        return Err("JSON_RPC_SRT_PASSPHRASE (--srt-passphrase) is required for srt/ws-srt".into());
    }
    if options
        .transports
        .iter()
        .any(|kind| matches!(kind, TransportKind::Srt | TransportKind::WebSocketSrt))
    {
        if let Some(config) = &options.srt {
            validate_srt_passphrase(&config.passphrase)?;
        }
    }
    if options
        .transports
        .iter()
        .any(|kind| matches!(kind, TransportKind::Mqtt | TransportKind::WebSocketMqtt))
    {
        validate_broker_url(
            &options.mqtt_url,
            &["mqtt://", "mqtts://"],
            "JSON_RPC_MQTT_URL",
        )?;
        if options.mqtt_ca.is_some() && !options.mqtt_url.starts_with("mqtts://") {
            return Err("JSON_RPC_MQTT_CA requires a mqtts:// broker URL".into());
        }
        validate_mqtt_topic(&options.mqtt_topic, "JSON_RPC_MQTT_TOPIC")?;
        validate_mqtt_topic(&options.mqtt_ws_topic, "JSON_RPC_WS_MQTT_TOPIC")?;
        if options.mqtt_operation_timeout.is_zero() || options.mqtt_response_timeout.is_zero() {
            return Err("MQTT operation and response timeouts must be greater than zero".into());
        }
    }
    if options
        .transports
        .iter()
        .any(|kind| matches!(kind, TransportKind::Amqp | TransportKind::WebSocketAmqp))
    {
        validate_broker_url(
            &options.amqp_url,
            &["amqp://", "amqps://"],
            "JSON_RPC_AMQP_URL",
        )?;
        validate_amqp_queue(&options.amqp_queue, "JSON_RPC_AMQP_QUEUE")?;
        validate_amqp_queue(&options.amqp_ws_queue, "JSON_RPC_WS_AMQP_QUEUE")?;
        if options.amqp_operation_timeout.is_zero() || options.amqp_response_timeout.is_zero() {
            return Err("AMQP operation and response timeouts must be greater than zero".into());
        }
    }
    Ok(())
}

fn validate_broker_url(value: &str, schemes: &[&str], setting: &str) -> Result<(), String> {
    if !schemes.iter().any(|scheme| value.starts_with(scheme))
        || value.chars().any(char::is_whitespace)
        || value
            .split_once("://")
            .is_none_or(|(_, authority)| authority.is_empty() || authority.starts_with('/'))
    {
        return Err(format!(
            "{setting} must be a nonempty URL using {}",
            schemes.join(" or ")
        ));
    }
    Ok(())
}

fn validate_service_route(route: &str, setting: &str) -> Result<(), String> {
    if route.trim().is_empty() || route.chars().any(char::is_control) {
        return Err(format!(
            "{setting} must be a nonempty route without control characters"
        ));
    }
    Ok(())
}

fn validate_mqtt_topic(topic: &str, setting: &str) -> Result<(), String> {
    validate_service_route(topic, setting)?;
    if topic.contains(['#', '+', '\0']) {
        return Err(format!(
            "{setting} must be an exact MQTT topic without wildcards or NUL"
        ));
    }
    Ok(())
}

fn validate_amqp_queue(queue: &str, setting: &str) -> Result<(), String> {
    validate_service_route(queue, setting)?;
    if queue.starts_with("amq.") {
        return Err(format!("{setting} must not use the reserved amq. prefix"));
    }
    Ok(())
}

fn validate_srt_passphrase(passphrase: &str) -> Result<(), String> {
    let byte_len = passphrase.len();
    if !(10..=79).contains(&byte_len) {
        return Err(format!(
            "SRT passphrase must contain 10..=79 bytes (got {byte_len})"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
