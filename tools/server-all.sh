#!/usr/bin/env bash
# Start every local transport supported by this OS and kernel.
# External MQTT/AMQP broker transports are opt-in with --brokers.
source "$(dirname "$0")/common.sh"

usage() {
    cat <<'EOF'
Usage: tools/server-all.sh [--brokers] [--dry-run] [SERVER OPTIONS...]

Starts all local transports supported by the current OS and kernel. Each port comes from
the corresponding JSON_RPC_*_PORT setting in the project-root .env file.
SERVER OPTIONS are forwarded to the server command, for example:
--bind 127.0.0.1

Pass --brokers to also enable MQTT, AMQP, WebSocket/MQTT, and WebSocket/AMQP.
Those require live external brokers and explicit JSON_RPC_MQTT_URL and
JSON_RPC_AMQP_URL settings in the environment or .env.

Pass --dry-run to print the command without building or starting the server.
On Linux/Android, Python 3 probes UDP-Lite, DCCP, and SCTP socket support for
the configured bind address families. Unsupported transports and their
WebSocket variants are skipped with a diagnostic, including in --dry-run.
Other socket errors (such as permission errors) abort the launch.
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
        if ! command -v python3 >/dev/null; then
            echo "Python 3 is required to probe kernel transport support." >&2
            exit 1
        fi
        bind="$(setting JSON_RPC_BIND)"
        for ((i = 0; i < ${#server_options[@]}; i++)); do
            if [[ "${server_options[i]}" == --bind ]]; then
                if ((i + 1 >= ${#server_options[@]})); then
                    echo "--bind needs a value" >&2
                    exit 2
                fi
                bind="${server_options[i + 1]}"
                i=$((i + 1))
            fi
        done
        supported="$(python3 "$ROOT/tools/probe-transports.py" "$bind")"
        for transport in udplite dccp sctp; do
            if [[ ",$supported," == *",$transport,"* ]]; then
                setting_prefix="${transport^^}"
                add_transport "$transport" "--$transport-port" "JSON_RPC_${setting_prefix}_PORT"
            fi
        done
        for transport in udplite dccp sctp; do
            if [[ ",$supported," == *",$transport,"* ]]; then
                setting_prefix="${transport^^}"
                add_transport "ws-$transport" "--ws-$transport-port" "JSON_RPC_WS_${setting_prefix}_PORT"
            fi
        done
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
