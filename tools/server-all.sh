#!/usr/bin/env bash
# Start every local transport supported by this OS.
# External MQTT/AMQP broker transports are opt-in with --brokers.
source "$(dirname "$0")/common.sh"

usage() {
    cat <<'EOF'
Usage: tools/server-all.sh [--brokers] [--dry-run] [SERVER OPTIONS...]

Starts all local transports supported by the current OS. Each port comes from
the corresponding JSON_RPC_*_PORT setting in the project-root .env file.
SERVER OPTIONS are forwarded to the server command, for example:
--bind 127.0.0.1

Pass --brokers to also enable MQTT, AMQP, WebSocket/MQTT, and WebSocket/AMQP.
Those require live external brokers and explicit JSON_RPC_MQTT_URL and
JSON_RPC_AMQP_URL settings in the environment or .env.

Pass --dry-run to print the command without building or starting the server.
EOF
}

include_brokers=0
dry_run=0
server_options=()
while (($#)); do
    case "$1" in
        --brokers)
            include_brokers=1
            shift
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        --)
            shift
            server_options+=("$@")
            break
            ;;
        *)
            server_options+=("$1")
            shift
            ;;
    esac
done

if ((include_brokers)); then
    for setting_name in JSON_RPC_MQTT_URL JSON_RPC_AMQP_URL; do
        if [[ -z "$(setting "$setting_name")" ]]; then
            echo "--brokers requires $setting_name in the environment or .env" >&2
            exit 2
        fi
    done
fi

transports=()
port_args=()
display_ports=()

add_transport() {
    local transport="$1" option="$2" setting_name="$3" port
    port="$(required "$setting_name")"
    if [[ ! "$port" =~ ^[0-9]{1,5}$ ]] || ((10#$port < 1 || 10#$port > 65535)); then
        echo "$setting_name must be a port between 1 and 65535 (got: $port)" >&2
        exit 2
    fi
    transports+=("$transport")
    port_args+=("$option" "$port")
    display_ports+=("$port")
}

add_transport tcp --tcp-port JSON_RPC_TCP_PORT
add_transport udp --udp-port JSON_RPC_UDP_PORT
add_transport rtp --rtp-port JSON_RPC_RTP_PORT
add_transport rudp --rudp-port JSON_RPC_RUDP_PORT
add_transport coap --coap-port JSON_RPC_COAP_PORT
add_transport quic --quic-port JSON_RPC_QUIC_PORT
add_transport http1 --http1-port JSON_RPC_HTTP1_PORT
add_transport http3 --http3-port JSON_RPC_HTTP3_PORT
add_transport ws-tcp --ws-tcp-port JSON_RPC_WS_TCP_PORT
add_transport ws-udp --ws-udp-port JSON_RPC_WS_UDP_PORT
add_transport ws-rtp --ws-rtp-port JSON_RPC_WS_RTP_PORT
add_transport ws-rudp --ws-rudp-port JSON_RPC_WS_RUDP_PORT
add_transport ws-coap --ws-coap-port JSON_RPC_WS_COAP_PORT
add_transport ws-quic --ws-quic-port JSON_RPC_WS_QUIC_PORT
add_transport ws-http1 --ws-http1-port JSON_RPC_WS_HTTP1_PORT
add_transport ws-http3 --ws-http3-port JSON_RPC_WS_HTTP3_PORT

if [[ -n "$(setting JSON_RPC_SRT_PASSPHRASE)" ]]; then
    add_transport srt --srt-port JSON_RPC_SRT_PORT
    add_transport ws-srt --ws-srt-port JSON_RPC_WS_SRT_PORT
else
    echo "Skipping srt and ws-srt: JSON_RPC_SRT_PASSPHRASE is not set."
fi

case "$(uname -s)" in
    Linux|Android)
        add_transport udplite --udplite-port JSON_RPC_UDPLITE_PORT
        add_transport dccp --dccp-port JSON_RPC_DCCP_PORT
        add_transport sctp --sctp-port JSON_RPC_SCTP_PORT
        add_transport ws-udplite --ws-udplite-port JSON_RPC_WS_UDPLITE_PORT
        add_transport ws-dccp --ws-dccp-port JSON_RPC_WS_DCCP_PORT
        add_transport ws-sctp --ws-sctp-port JSON_RPC_WS_SCTP_PORT
        ;;
esac

if ((include_brokers)); then
    transports+=(mqtt amqp ws-mqtt ws-amqp)
    display_ports+=(broker broker broker broker)
fi

transports_csv="$(IFS=,; echo "${transports[*]}")"
echo "Transports: $transports_csv"
for ((i = 0; i < ${#transports[@]}; i++)); do
    printf '  %-12s %s\n' "${transports[i]}" "${display_ports[i]}"
done

if ((dry_run)); then
    printf 'Command:'
    printf ' %q' bash "$ROOT/tools/build-srt.sh" "$BIN" server "$transports_csv" "${port_args[@]}" ${server_options[@]+"${server_options[@]}"}
    printf '\n'
    exit 0
fi

app server "$transports_csv" "${port_args[@]}" ${server_options[@]+"${server_options[@]}"}
