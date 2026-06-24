#!/bin/bash
# hermes-mqtt-bridge.sh — Bridge Hermes hooks to MQTT publisher
# 
# Hermes calls this script with JSON payload on stdin. We forward it to
# the MQTT publisher via Unix socket.
#
# Usage:
#   echo '{"hook_event_name":"pre_llm_call",...}' | bash hermes-mqtt-bridge.sh

set -euo pipefail

SOCKET_PATH="${HERMES_MQTT_SOCKET:-/tmp/hermes-mqtt.sock}"

# Read JSON from stdin
read -r json_data

# Try to send to publisher via Unix socket
if echo "$json_data" | nc -U -w1 "$SOCKET_PATH" 2>/dev/null; then
    exit 0
fi

# Fallback: write to pickup file
PICKUP_DIR="$HOME/.hermes-agentpet/state"
mkdir -p "$PICKUP_DIR"
echo "$json_data" >> "$PICKUP_DIR/pickup.json"
exit 0
