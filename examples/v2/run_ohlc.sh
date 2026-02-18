#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${EVM_WS_URL:-}" ]]; then
  echo "EVM_WS_URL is required" >&2
  exit 1
fi

if [[ -z "${POOL_ADDRESS:-}" ]]; then
  echo "POOL_ADDRESS is required" >&2
  exit 1
fi

"$SCRIPT_DIR/handlers/ohlc-1m/build_handler.sh"

psql "${PGURL:-postgresql:///postgres}" -f "$SCRIPT_DIR/run_ohlc.sql"
