#!/bin/bash
echo "Starting Home Assistant Add-on..."

# Home Assistant passes user configuration into /data/options.json
# We use jq to parse it and set it as an environment variable for Python
GREETING=$(jq --raw-output '.greeting_message // empty' /data/options.json)
export BOOT_GREETING=$GREETING

echo "Launching Python bot..."
python3 main.py
