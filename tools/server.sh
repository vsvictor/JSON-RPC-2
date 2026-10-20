#!/usr/bin/env bash
# One JSON-RPC server configured by .env in the project root, until Ctrl+C.
# Profiles:
#   tools/server.sh                 default JSON_RPC_BIND
#   tools/server.sh ipv4            JSON_RPC_BIND_IPV4
#   tools/server.sh ipv6            JSON_RPC_BIND_IPV6
#   tools/server.sh lan             JSON_RPC_BIND_LAN
# Extra options are passed through, e.g.
#   tools/server.sh lan tcp,http1 --tcp-port 41000
source "$(dirname "$0")/common.sh"

bind=()
case "${1:-}" in
    ipv4)
        bind=(--bind "$(required JSON_RPC_BIND_IPV4)")
        shift
        ;;
    ipv6)
        bind=(--bind "$(required JSON_RPC_BIND_IPV6)")
        shift
        ;;
    lan)
        bind=(--bind "$(required JSON_RPC_BIND_LAN)")
        shift
        ;;
esac

app server "${bind[@]}" "$@"
