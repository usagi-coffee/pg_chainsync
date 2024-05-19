#!/bin/bash

cargo pgrx package
podman compose build
(podman compose down || true)
sleep 0.3
podman compose up

