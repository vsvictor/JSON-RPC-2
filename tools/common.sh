#!/usr/bin/env bash
# Shared helpers: sourced by the other scripts, not run directly.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Settings of the application, see .env in the project root. An optional
# overlay (for example .env.test) can be loaded via JSON_RPC_ENV_OVERLAY.
env_files() {
    local files=()
    if [[ -n "${JSON_RPC_ENV_OVERLAY:-}" ]]; then
        files+=("$JSON_RPC_ENV_OVERLAY")
    fi
    files+=("${JSON_RPC_ENV:-$ROOT/.env}")
    printf '%s\n' "${files[@]}"
}

# Value of a setting as the application sees it: the environment first,
# then the optional overlay, then .env. The application itself loads the same
# file chain; nothing is exported here.
setting() {
    local name="$1" key value env_file
    if printenv "$name" >/dev/null; then
        printenv "$name"
        return
    fi
    while IFS= read -r env_file; do
        [[ -f "$env_file" ]] || continue
        while IFS='=' read -r key value || [[ -n "$key" ]]; do
            key="${key// /}"
            [[ "$key" == "$name" ]] || continue
            value="${value%%#*}"; value="${value%"${value##*[![:space:]]}"}"
            value="${value%\"}"; value="${value#\"}"
            printf '%s\n' "$value"
            return
        done <"$env_file"
    done < <(env_files)
}

# A required setting: exit with a message if it is not set.
required() {
    local value
    value="$(setting "$1")"
    if [[ -z "$value" ]]; then
        echo "$1 is not set (see $(env_files | paste -sd ', ' -))" >&2
        exit 1
    fi
    printf '%s\n' "$value"
}
BIN="$ROOT/target/debug/JSON_RPC_2"

# Build quietly; the compiler output (warnings of the vendored h3) is shown
# only when the build fails.
build() {
    local output
    if ! output="$(cargo build -q --color always 2>&1)"; then
        printf '%s\n' "$output" >&2
        exit 1
    fi
}

# Run the demo binary with the given arguments.
app() {
    build
    "$BIN" "$@"
}
