#!/bin/bash

cleanup () {
  (podman-compose down -t 0 || true)
}

trap "cleanup" INT

cargo pgrx package -d
podman-compose build
sleep 1
podman-compose up
