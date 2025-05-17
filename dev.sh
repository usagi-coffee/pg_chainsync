#!/bin/bash

# Cleanup function to stop and remove containers
cleanup () {
  (podman-compose down -t 0 || true)
}

# Trap SIGINT to call cleanup
trap "cleanup" INT

# Get the example script from the argument
if [ -z "$1" ]; then
  echo "Usage: $0 <examples/demo.sql>"
  exit 1
fi

# Build extension
cargo pgrx package -d

# Copy the example script to the dev directory so it gets mounted
cp "$1" dev/dev.sql

podman-compose build
sleep 1
podman-compose up

# Ctrl+C to stop the containers, should cleanup
