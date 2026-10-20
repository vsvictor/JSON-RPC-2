use std::{
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use json_rpc_2::{
    SrtConfig, TcpCongestionControl, TcpTuningConfig, TransportCapabilities, TransportKind,
    UdpLiteConfig,
    UdpReliabilityConfig, client::ReconnectPolicy,
    server::TransportLimits as ServerTransportLimits,
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
                                                tcp, udp, rtp, rudp, srt, udplite, dccp, sctp, quic, http1, http3,
                                                mqtt, coap, amqp,
                                                ws-tcp, ws-udp, ws-rtp, ws-rudp, ws-srt, ws-udplite, ws-dccp, ws-sctp,
                                                ws-quic, ws-http1, ws-http3, ws-mqtt, ws-coap, ws-amqp
                                                (udplite/ws-udplite, dccp/ws-dccp and sctp/ws-sctp: Linux/Android only)
Server:
  --bind <IPS>        * JSON_RPC_BIND           local addresses to listen on, comma-separated:
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
                                                disable Nagle on TCP-based sockets
  --tcp-congestion <NAME>   JSON_RPC_TCP_CONGESTION
                                                system or bbr
  --tcp-fastopen <BOOL>     JSON_RPC_TCP_FASTOPEN
                                                enable TCP Fast Open where the OS supports it
  --tcp-fastopen-queue-len <N> JSON_RPC_TCP_FASTOPEN_QUEUE_LEN
                                                Fast Open listener queue length
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
Ports (server and client), one per transport, required for every used transport:
  --tcp-port <N>      * JSON_RPC_TCP_PORT       TCP
  --udp-port <N>      * JSON_RPC_UDP_PORT       reliable UDP
  --rtp-port <N>      * JSON_RPC_RTP_PORT       RTP/RTCP over UDP
  --rudp-port <N>     * JSON_RPC_RUDP_PORT      Reliable UDP with explicit sequencing
  --srt-port <N>      * JSON_RPC_SRT_PORT       Encrypted reliable UDP-style transport
  --udplite-port <N>  * JSON_RPC_UDPLITE_PORT   UDP-Lite with partial checksum coverage (Linux/Android only)
  --mqtt-port <N>     * JSON_RPC_MQTT_PORT      MQTT-style publish/subscribe over TCP
  --coap-port <N>     * JSON_RPC_COAP_PORT      CoAP-style constrained application transport over UDP
  --amqp-port <N>     * JSON_RPC_AMQP_PORT      AMQP-style message queuing over TCP
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
  --ws-mqtt-port <N>  * JSON_RPC_WS_MQTT_PORT   WebSocket over MQTT-style transport
  --ws-coap-port <N>  * JSON_RPC_WS_COAP_PORT   WebSocket over CoAP-style transport
  --ws-amqp-port <N>  * JSON_RPC_WS_AMQP_PORT   WebSocket over AMQP-style transport
  --ws-dccp-port <N>  * JSON_RPC_WS_DCCP_PORT   DCCP (Linux/Android only)
  --ws-sctp-port <N>  * JSON_RPC_WS_SCTP_PORT   SCTP (Linux/Android only)
  --ws-quic-port <N>  * JSON_RPC_WS_QUIC_PORT   UDP
  --ws-http1-port <N> * JSON_RPC_WS_HTTP1_PORT  TCP
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub transports: Vec<TransportKind>,
    pub bind: Vec<IpAddr>,
    pub hosts: Vec<String>,
    pub ports: [Option<u16>; TRANSPORT_SLOT_COUNT],
    pub max_missed_pongs: u32,
    pub request_timeout: Duration,
    pub reconnect: ReconnectPolicy,
    pub server_limits: ServerTransportLimits,
    pub tcp_tuning: TcpTuningConfig,
    pub udp_reliability: UdpReliabilityConfig,
    pub udplite: UdpLiteConfig,
    pub srt: Option<SrtConfig>,
    pub cert: PathBuf,
    pub key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
    pub server_name: Option<String>,
    pub scripts: ScriptProfiles,
}

impl RuntimeOptions {
    pub fn from_env() -> Result<Self, String> {
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
                send_checksum_coverage: parse_setting(
                    "JSON_RPC_UDPLITE_SEND_CSCOV",
                    parse_u16,
                )?
                .unwrap_or(8),
                recv_checksum_coverage: parse_setting(
                    "JSON_RPC_UDPLITE_RECV_CSCOV",
                    parse_u16,
                )?
                .unwrap_or(8),
            },
            srt: setting("JSON_RPC_SRT_PASSPHRASE").map(|passphrase| SrtConfig { passphrase }),
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
        validate_runtime_options(&options)?;
        Ok(options)
    }

    pub fn port(&self, kind: TransportKind) -> u16 {
        self.ports[kind.index()].expect("port checked")
    }

    pub fn check_ports(&self) -> Result<(), String> {
        let missing: Vec<String> = self
            .transports
            .iter()
            .filter(|kind| self.ports[kind.index()].is_none())
            .map(|kind| format!("{} ({})", kind.port_setting(), kind.port_option()))
            .collect();
        if !missing.is_empty() {
            return Err(format!("port not set: {}", missing.join(", ")));
        }
        for (index, first) in self.transports.iter().enumerate() {
            for second in &self.transports[index + 1..] {
                if first.socket() == second.socket() && self.port(*first) == self.port(*second) {
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
            let mut options = RuntimeOptions::from_env()?;
            options.transports = TransportKind::ALL.to_vec();
            options.check_ports()?;
            return Ok(CommandConfig::Demo(options));
        }
        Some("-h" | "--help" | "help") => return Ok(CommandConfig::Help),
        Some("server") => false,
        Some("client") => true,
        Some(other) => return Err(format!("unknown command {other}")),
    };

    let mut options = RuntimeOptions::from_env()?;
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
                options.udplite.send_checksum_coverage =
                    parse_u16(&value()?).map_err(|error| format!("--udplite-send-cscov: {error}"))?;
            }
            "--udplite-recv-cscov" => {
                options.udplite.recv_checksum_coverage =
                    parse_u16(&value()?).map_err(|error| format!("--udplite-recv-cscov: {error}"))?;
            }
            "--srt-passphrase" => {
                options.srt = Some(SrtConfig {
                    passphrase: value()?,
                });
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
    } else if options.bind.is_empty() {
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
            TransportKind::WebSocketHttp3 => "JSON_RPC_WS_HTTP3_PORT",
        }
    }

    fn port_option(self) -> String {
        format!("--{}-port", self.id())
    }
}

const PATH_SETTINGS: [&str; 3] = ["JSON_RPC_CERT", "JSON_RPC_KEY", "JSON_RPC_CA"];

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
    value.parse().map_err(|_| format!("invalid integer {value}"))
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
    if let Some(config) = &options.srt {
        config.validate()?;
    }
    Ok(())
}
