#!/usr/bin/env bash
# Raw JSON-RPC 2.0 over HTTP/1.1 with curl against a running server:
# a call, a notification, a batch and errors, on JSON_RPC_HOST
# or on the hosts given: tools/curl.sh 192.168.0.10
source "$(dirname "$0")/common.sh"
HTTP1_PORT="$(required JSON_RPC_HTTP1_PORT)"
HOSTS="${1:-$(required JSON_RPC_HOST)}"

rpc() {
    local url="$1" body="$2" response
    # -g: do not treat [::1] as a curl URL glob. The last line is the status.
    response="$(curl -gsS -w '\n%{http_code}' -H 'Content-Type: application/json' -d "$body" "$url")"
    printf '  -> %s\n  <- %s (HTTP %s)\n' "$body" "$(sed '$d' <<<"$response")" "$(tail -n1 <<<"$response")"
}

urls=()
IFS=',' read -ra hosts <<<"$HOSTS"
for host in "${hosts[@]}"; do
    host="${host// /}"; host="${host#[}"; host="${host%]}"
    # IPv6 literals go in brackets in a URL: http://[::1]:port/
    [[ "$host" == *:* ]] && host="[$host]"
    urls+=("http://$host:$HTTP1_PORT/")
done

for url in "${urls[@]}"; do
    echo "== $url"
    rpc "$url" '{"jsonrpc":"2.0","method":"echo","params":{"hello":"world"},"id":1}'
    rpc "$url" '{"jsonrpc":"2.0","method":"echo","params":[1]}'
    rpc "$url" '[{"jsonrpc":"2.0","method":"echo","params":[1],"id":1},{"jsonrpc":"2.0","method":"echo","params":[2]},{"jsonrpc":"2.0","method":"echo","params":[3],"id":"three"}]'
    rpc "$url" '{"jsonrpc":"2.0","method":"missing","id":2}'
    rpc "$url" '{"jsonrpc":"2.0","method":'
done
