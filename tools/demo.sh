#!/usr/bin/env bash
# Server and clients in one process on every address of mobilespace.local
# (IPv4 and IPv6, plus the IPv6 examples), ports, certificate and key from .env
# (tools/gen-cert.sh).
source "$(dirname "$0")/common.sh"
app demo "$@"
