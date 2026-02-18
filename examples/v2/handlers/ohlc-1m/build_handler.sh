#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/../../../.."

(
  cd "$REPO_ROOT"
  cargo build --release -p ohlc_handler
)

cp "$REPO_ROOT/target/release/libohlc_handler.so" "$SCRIPT_DIR/handler.so"
echo "Built $SCRIPT_DIR/handler.so"
