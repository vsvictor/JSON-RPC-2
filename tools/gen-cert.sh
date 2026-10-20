#!/usr/bin/env bash
# Generate a self-signed TLS certificate for the local network (LAN): the
# certificate is valid for mobilespace.local and this machine's LAN IP
# addresses, so QUIC, HTTP/3 and WebSocket/HTTP/3 clients on other machines
# can verify it.
#
#   tools/gen-cert.sh                       mobilespace.local and detected LAN addresses
#   tools/gen-cert.sh 192.168.1.50 nas.lan  plus extra IP addresses / DNS names
#   tools/gen-cert.sh --only 192.168.1.50   only the given names
#
# Options:
#   --only              use only the given names (no mobilespace.local, no detection)
#   --with-localhost    also add localhost, 127.0.0.1 and ::1
#   --days <N>          validity in days (default: 825)
#   --force             overwrite an existing certificate and key
#
# Output: JSON_RPC_CERT and JSON_RPC_KEY from .env in the project root
# (default: certs/cert.pem and certs/key.pem in the project root).
source "$(dirname "$0")/common.sh"

# DNS name of the server in the local network, the certificate's common name.
DNS_NAME=mobilespace.local

days=825
detect=1
localhost=0
force=0
names=()
while (($#)); do
    case "$1" in
        --only) detect=0 ;;
        --with-localhost) localhost=1 ;;
        --days) days="${2:?--days needs a value}"; shift ;;
        --force) force=1 ;;
        -h | --help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        -*) echo "unknown option $1" >&2; exit 2 ;;
        *) names+=("$1") ;;
    esac
    shift
done

command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }

# Paths as the application resolves them: relative to the project root.
resolve() {
    case "$1" in
        /*) printf '%s\n' "$1" ;;
        *) printf '%s\n' "$ROOT/$1" ;;
    esac
}
cert="$(setting JSON_RPC_CERT)"
key="$(setting JSON_RPC_KEY)"
cert="$(resolve "${cert:-certs/cert.pem}")"
key="$(resolve "${key:-certs/key.pem}")"

if ((!force)) && [[ -e "$cert" || -e "$key" ]]; then
    echo "$cert or $key already exists; use --force to overwrite" >&2
    exit 1
fi

# LAN addresses of this machine: loopback and link-local (fe80::, needs a
# zone id, cannot be used in a certificate) are skipped.
lan_addresses() {
    if command -v ip >/dev/null; then
        ip -o addr show up | awk '{print $4}' | cut -d/ -f1
    else
        ifconfig | awk '($1 == "inet" || $1 == "inet6") && $0 !~ / temporary/ {print $2}'
    fi | grep -vE '^(127\.|::1$|fe80:|169\.254\.)' | cut -d% -f1 | sort -u
}

if ((detect)); then
    names=("$DNS_NAME" ${names[@]+"${names[@]}"})
    while read -r address; do
        [[ -n "$address" ]] && names+=("$address")
    done < <(lan_addresses)
fi
if ((localhost)); then
    names+=(localhost 127.0.0.1 ::1)
fi

# Unique names, each as an IP or a DNS subjectAltName.
alt_names=()
seen=" "
for name in "${names[@]}"; do
    name="${name#[}"; name="${name%]}"
    lower="$(tr '[:upper:]' '[:lower:]' <<<"$name")"
    [[ "$seen" == *" $lower "* ]] && continue
    seen+="$lower "
    if [[ "$name" =~ ^[0-9]+(\.[0-9]+){3}$ || "$name" == *:* ]]; then
        alt_names+=("IP:$name")
    else
        alt_names+=("DNS:$name")
    fi
done
if ((${#alt_names[@]} == 0)); then
    echo "no LAN address found; pass IP addresses or names as arguments" >&2
    exit 1
fi
san="$(IFS=,; echo "${alt_names[*]}")"
common_name="${alt_names[0]#*:}"

# Self-signed end-entity certificate (not a CA: rustls rejects a CA
# certificate used as a server certificate), ECDSA P-256 key in PKCS#8.
mkdir -p "$(dirname "$cert")" "$(dirname "$key")"
temporary_key="$(mktemp)"
trap 'rm -f "$temporary_key"' EXIT
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$temporary_key" -out "$cert" -days "$days" -sha256 \
    -subj "/O=JSON-RPC 2.0 local network/CN=$common_name" \
    -addext "subjectAltName=$san" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature" \
    -addext "extendedKeyUsage=serverAuth" 2>/dev/null \
    -text
install -m 600 "$temporary_key" "$key"

echo "Certificate: $cert"
echo "Private key: $key (mode 600)"
echo "Valid for:   ${alt_names[*]}"
echo "Expires:     $(openssl x509 -in "$cert" -noout -enddate | cut -d= -f2)"
echo
echo "Server (this machine), listen on every interface:"
echo "  tools/server.sh lan            (or JSON_RPC_BIND=0.0.0.0,:: in .env)"
echo "Client (another machine): copy $cert there, then:"
echo "  JSON_RPC_CA=/path/to/cert.pem tools/client.sh --host ${common_name}"
