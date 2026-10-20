#!/usr/bin/env bash
# Build pinned upstream libsrt, then optionally run a command against it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
revision=899348d8318eb9a3c5a5b6ec43c4a1114288773a
directory="$ROOT/target/native/srt/$revision"
prefix="$directory/install"
export PKG_CONFIG_PATH="$prefix/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"

for tool in git cmake pkg-config; do
    if ! command -v "$tool" >/dev/null; then
        echo "Missing build prerequisite: $tool (see json-rpc-2-datagram/SRT.md)" >&2
        exit 1
    fi
done

if [[ ! -f "$prefix/lib/pkgconfig/srt.pc" ]]; then
    pkg-config --exists openssl || {
        echo "OpenSSL development files are required (see json-rpc-2-datagram/SRT.md)." >&2
        exit 1
    }
    if [[ ! -d "$directory/source/.git" ]]; then
        git init -q "$directory/source"
    fi
    git -C "$directory/source" fetch --quiet --depth 1 \
        https://github.com/Haivision/srt.git "$revision"
    git -C "$directory/source" checkout --quiet --detach "$revision"
    [[ "$(git -C "$directory/source" rev-parse HEAD)" == "$revision" ]]
    cmake -S "$directory/source" -B "$directory/build" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$prefix" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_RPATH="$prefix/lib" \
        -DENABLE_ENCRYPTION=ON -DUSE_ENCLIB=openssl-evp \
        -DENABLE_SHARED=ON -DENABLE_STATIC=OFF \
        -DENABLE_APPS=OFF -DENABLE_TESTING=OFF
    cmake --build "$directory/build" --parallel
    cmake --install "$directory/build"
fi

if [[ "$(pkg-config --variable=prefix srt)" != "$prefix" ]] \
    || ! pkg-config --exact-version=1.5.7 srt; then
    echo "Failed to select the pinned upstream libsrt installation." >&2
    exit 1
fi

case "$(uname -s)" in
    Darwin) export DYLD_LIBRARY_PATH="$prefix/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" ;;
    *) export LD_LIBRARY_PATH="$prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ;;
esac

if (($#)); then
    exec "$@"
fi
printf 'libsrt 1.5.7: %s\n' "$prefix"
printf 'Run Cargo with: tools/build-srt.sh cargo <COMMAND>\n'
