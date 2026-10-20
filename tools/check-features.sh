#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

transport_features=(tcp udp rtp rudp srt mqtt coap amqp http1 quic http3)
for package in json-rpc-2 json-rpc-2-ffi; do
  for feature in "${transport_features[@]}"; do
    echo ">>> cargo check -q -p $package --no-default-features --features $feature"
    cargo check -q -p "$package" --no-default-features --features "$feature"
  done
  for feature in tcp udp rtp rudp srt mqtt coap amqp http1 quic http3; do
    echo ">>> cargo check -q -p $package --no-default-features --features $feature,websocket"
    cargo check -q -p "$package" --no-default-features --features "$feature,websocket"
  done

  case "$(uname -s)" in
    Linux|Android)
      for feature in udplite dccp sctp; do
        echo ">>> cargo check -q -p $package --no-default-features --features $feature"
        cargo check -q -p "$package" --no-default-features --features "$feature"
        echo ">>> cargo check -q -p $package --no-default-features --features $feature,websocket"
        cargo check -q -p "$package" --no-default-features --features "$feature,websocket"
      done
      ;;
  esac
done
