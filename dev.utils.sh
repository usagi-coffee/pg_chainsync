#!/bin/bash

shopt -s huponexit # make sure children get SIGHUP when the script exits

# Cleanup function to stop and remove containers
cleanup () {
  (podman-compose -f docker-compose.utils.yml down -t 0 || true)
}

# run cleanup on Ctrl-C (INT), kill/TERM, any error (ERR) or a normal EXIT
trap cleanup INT TERM ERR EXIT

# Get the example script from the argument
if [ -z "$1" ]; then
  echo "Usage: $0 <examples/demo.sql>"
  exit 1
fi

# Build extension
cargo pgrx package -d

# Copy the example script to the dev directory so it gets mounted
cp "$1" dev/dev.sql

podman-compose -f docker-compose.utils.yml build
sleep 1
podman-compose -f docker-compose.utils.yml up

# Ctrl+C to stop the containers, should cleanup
