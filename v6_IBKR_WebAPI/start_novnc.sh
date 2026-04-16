#!/bin/bash
if [ "$ENABLE_VNC" = "true" ]; then
  echo "Starting novnc..."
  exec websockify --web /usr/share/novnc/ 6080 localhost:5900
else
  echo "noVNC is disabled, sleeping..."
  exec sleep infinity
fi
