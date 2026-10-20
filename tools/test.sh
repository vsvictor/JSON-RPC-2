#!/usr/bin/env bash
# Formatting check and every test of the workspace (unit and network tests,
# all transports over IPv4 and IPv6).
source "$(dirname "$0")/common.sh"
cargo fmt --all --check
bash "$ROOT/tools/build-srt.sh" cargo test -q --workspace --all-targets "$@"
