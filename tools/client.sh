#!/usr/bin/env bash
# Call a running server (tools/server.sh).
#   tools/client.sh                            every transport, server from .env (JSON_RPC_HOST)
#   tools/client.sh quic                       one transport: tcp|udp|dccp|sctp|quic|http1|http3|ws-tcp|ws-udp|ws-dccp|ws-sctp|ws-quic|ws-http1|ws-http3
#   tools/client.sh http3,quic --host ::1      some transports, another host
#   tools/client.sh ipv4                        JSON_RPC_HOST_IPV4
#   tools/client.sh ipv6 --each                 JSON_RPC_HOST_IPV6, separate process per transport
source "$(dirname "$0")/common.sh"

host=()
each=0
case "${1:-}" in
    ipv4)
        host=(--host "$(required JSON_RPC_HOST_IPV4)")
        shift
        ;;
    ipv6)
        host=(--host "$(required JSON_RPC_HOST_IPV6)")
        shift
        ;;
esac

if [[ "${1:-}" == "--each" ]]; then
    each=1
    shift
fi

if ((each)); then
    build
    status=0
    transports=(tcp udp quic http1 http3 ws-tcp ws-udp ws-quic ws-http1 ws-http3)
    case "$(uname -s)" in
        Linux|Android)
            transports+=(dccp sctp ws-dccp ws-sctp)
            ;;
    esac
    for transport in "${transports[@]}"; do
        echo "### $transport"
        "$BIN" client "$transport" "$@" "${host[@]}" || status=1
    done
    exit "$status"
fi

app client "$@" "${host[@]}"
