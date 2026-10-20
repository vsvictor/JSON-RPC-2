#!/usr/bin/env bash
# End to end: start the server in the background, call it over every
# transport with separate client processes and curl, then stop it with
# Ctrl+C (SIGINT). Uses the E2E settings from .env.test (loaded on top of .env,
# or environment overrides): port base, temporary self-signed certificate and
# loopback addresses, so it does not clash with tools/server.sh.
export JSON_RPC_ENV_OVERLAY="${JSON_RPC_ENV_OVERLAY:-$(cd "$(dirname "$0")/.." && pwd)/.env.test}"
CERT="$(mktemp -d)/cert.pem"
source "$(dirname "$0")/common.sh"
base="${JSON_RPC_E2E_PORT_BASE:-$(required JSON_RPC_E2E_PORT_BASE)}"
export JSON_RPC_TRANSPORT=all JSON_RPC_BIND="$(required JSON_RPC_E2E_BIND)"
export JSON_RPC_TCP_PORT=$base JSON_RPC_UDP_PORT=$((base + 1)) JSON_RPC_QUIC_PORT=$((base + 2))
export JSON_RPC_DCCP_PORT=$((base + 10))
export JSON_RPC_SCTP_PORT=$((base + 12))
export JSON_RPC_HTTP1_PORT=$((base + 3)) JSON_RPC_HTTP3_PORT=$((base + 4))
export JSON_RPC_WS_TCP_PORT=$((base + 5)) JSON_RPC_WS_UDP_PORT=$((base + 6))
export JSON_RPC_WS_DCCP_PORT=$((base + 11))
export JSON_RPC_WS_SCTP_PORT=$((base + 13))
export JSON_RPC_WS_QUIC_PORT=$((base + 7)) JSON_RPC_WS_HTTP1_PORT=$((base + 8))
export JSON_RPC_WS_HTTP3_PORT=$((base + 9))
export JSON_RPC_CERT="$CERT" JSON_RPC_KEY="$(dirname "$CERT")/key.pem" JSON_RPC_CA= JSON_RPC_SERVER_NAME=
build

"$BIN" server &
server=$!
trap 'kill -INT "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true; rm -rf "$(dirname "$CERT")"' EXIT

# Wait until the server listens (HTTP/1.1 answers and the certificate exists).
for _ in $(seq 100); do
    if [[ -f "$CERT" ]] && curl -s -o /dev/null -d '{}' "http://$(required JSON_RPC_E2E_HTTP1_HOST):$JSON_RPC_HTTP1_PORT/"; then
        break
    fi
    kill -0 "$server" 2>/dev/null || { echo "server exited" >&2; exit 1; }
    sleep 0.1
done

echo; "$BIN" client --host "$(required JSON_RPC_E2E_HOST)"
echo; "$ROOT/tools/curl.sh" "$(required JSON_RPC_E2E_HOST)"
echo
