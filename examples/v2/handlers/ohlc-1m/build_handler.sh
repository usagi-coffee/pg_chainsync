#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$SCRIPT_DIR/module-src"

(
  cd "$MODULE_DIR"
  cargo build --release
)

cp "$MODULE_DIR/target/release/libohlc_handler.so" "$SCRIPT_DIR/handler.so"
echo "Built $SCRIPT_DIR/handler.so"
