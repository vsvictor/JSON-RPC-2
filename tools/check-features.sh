#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

checks=(
  "cargo check -q -p json-rpc-2 --no-default-features --features tcp"
  "cargo check -q -p json-rpc-2 --no-default-features --features udp"
  "cargo check -q -p json-rpc-2 --no-default-features --features rtp"
  "cargo check -q -p json-rpc-2 --no-default-features --features rudp"
  "cargo check -q -p json-rpc-2 --no-default-features --features srt"
  "cargo check -q -p json-rpc-2 --no-default-features --features mqtt"
  "cargo check -q -p json-rpc-2 --no-default-features --features coap"
  "cargo check -q -p json-rpc-2 --no-default-features --features amqp"
  "cargo check -q -p json-rpc-2 --no-default-features --features http1"
  "cargo check -q -p json-rpc-2 --no-default-features --features quic"
  "cargo check -q -p json-rpc-2 --no-default-features --features http3"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'tcp,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'udp,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'rtp,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'rudp,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'srt,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'mqtt,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'coap,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'amqp,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'http1,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'quic,websocket'"
  "cargo check -q -p json-rpc-2 --no-default-features --features 'http3,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features tcp"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features udp"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features rtp"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features rudp"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features srt"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features mqtt"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features coap"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features amqp"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features http1"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features quic"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features http3"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'tcp,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'udp,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'rtp,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'rudp,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'srt,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'mqtt,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'coap,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'amqp,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'http1,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'quic,websocket'"
  "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'http3,websocket'"
)

case "$(uname -s)" in
  Linux|Android)
    checks+=(
      "cargo check -q -p json-rpc-2 --no-default-features --features udplite"
      "cargo check -q -p json-rpc-2 --no-default-features --features dccp"
      "cargo check -q -p json-rpc-2 --no-default-features --features sctp"
      "cargo check -q -p json-rpc-2 --no-default-features --features 'udplite,websocket'"
      "cargo check -q -p json-rpc-2 --no-default-features --features 'dccp,websocket'"
      "cargo check -q -p json-rpc-2 --no-default-features --features 'sctp,websocket'"
      "cargo check -q -p json-rpc-2-ffi --no-default-features --features udplite"
      "cargo check -q -p json-rpc-2-ffi --no-default-features --features dccp"
      "cargo check -q -p json-rpc-2-ffi --no-default-features --features sctp"
      "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'udplite,websocket'"
      "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'dccp,websocket'"
      "cargo check -q -p json-rpc-2-ffi --no-default-features --features 'sctp,websocket'"
    )
    ;;
esac

for cmd in "${checks[@]}"; do
  echo ">>> $cmd"
  eval "$cmd"
done
