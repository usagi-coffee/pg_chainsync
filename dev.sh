#!/bin/bash

cleanup () {
  (podman-compose down -t 0 || true)
}

trap "cleanup" INT

cargo pgrx package
podman-compose build
sleep 1
podman-compose up
