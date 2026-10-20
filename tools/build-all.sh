#!/usr/bin/env bash
# Build every workspace crate and target with all features.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

case "${1:-}" in
    -h|--help)
        cat <<'EOF'
Usage: tools/build-all.sh [CARGO BUILD OPTIONS...]

Builds all workspace crates, binaries, libraries, examples, tests and benches
with all features supported by the target platform, including FFI and UniFFI
bindgen. Builds and caches pinned Haivision libsrt through build-srt.sh.
Tests are compiled, not executed. The separate cargo-fuzz project is excluded.

Examples:
  tools/build-all.sh
  tools/build-all.sh --release
  tools/build-all.sh --locked -j 4

Native prerequisites: see json-rpc-2-datagram/SRT.md.
EOF
        exit 0
        ;;
esac

exec bash "$ROOT/tools/build-srt.sh" \
    cargo build --workspace --all-targets --all-features "$@"
