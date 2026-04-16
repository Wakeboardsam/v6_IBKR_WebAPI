#!/bin/bash
if [ "$ENABLE_VNC" = "true" ]; then
  echo "Starting x11vnc..."
  exec x11vnc -display :99 -forever -nopw -quiet -listen 0.0.0.0 -xkb
else
  echo "VNC is disabled, sleeping..."
  exec sleep infinity
fi
