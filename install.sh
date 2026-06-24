#!/bin/bash
# install.sh — Install Hermes MQTT Publisher/Subscriber
# Run this on the Mac Mini (Publisher) or MacBook (Subscriber)

set -euo pipefail

echo "=== Hermes MQTT Publisher/Subscriber Setup ==="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Install Python dependencies
echo -e "\n${YELLOW}Installing Python dependencies...${NC}"
pip3 install --quiet paho-mqtt pyyaml 2>/dev/null || pip3 install paho-mqtt pyyaml

# Detect machine type
HOSTNAME=$(hostname)
if [[ "$HOSTNAME" == *"iai-sti111"* ]]; then
    MACHINE="macmini"
    echo -e "${GREEN}Detected: Mac Mini${NC}"
else
    MACHINE="macbook"
    echo -e "${GREEN}Detected: MacBook${NC}"
fi

# Create directories
echo -e "\n${YELLOW}Creating directories...${NC}"
mkdir -p ~/.hermes-agentpet/{publisher,subscriber,hooks,state,logs}
mkdir -p ~/Library/LaunchAgents
mkdir -p ~/Library/Logs/hermes-agentpet

# Install Publisher
echo -e "\n${YELLOW}Installing Publisher...${NC}"
cat > ~/.hermes-agentpet/publisher/config.yaml << 'EOF'
broker:
  host: "127.0.0.1"
  port: 1883
  client_id: "hermes-publisher"
  username: ""
  password: ""
  keepalive: 60
  clean_session: false
  qos: 1
  topic_prefix: "hermes"

machine:
  hostname: "macmini"
  hermes_home: "~/.hermes"

polling:
  token_interval_seconds: 5
  cron_interval_seconds: 30
  heartbeat_interval_seconds: 60

last_will:
  topic: "hermes/status/macmini"
  payload: '{"hostname":"macmini","status":"offline","timestamp":0}'
  qos: 1
  retain: true

hooks:
  forward:
    - on_session_start
    - on_session_end
    - pre_llm_call
    - post_llm_call
    - pre_tool_call
    - post_tool_call
    - subagent_start
    - subagent_stop
    - api_request_error

commands:
  subscribe: "hermes/cmd/+/"
  response_prefix: "hermes/cmd/response"
EOF

# Install Subscriber
echo -e "\n${YELLOW}Installing Subscriber...${NC}"
cat > ~/.hermes-agentpet/subscriber/config.yaml << 'EOF'
broker:
  host: "127.0.0.1"
  port: 1883
  client_id: "hermes-subscriber"
  username: ""
  password: ""
  keepalive: 60
  qos: 1
  topic_prefix: "hermes"

display:
  menubar_enabled: true
  pet_enabled: true
  update_interval: 1
  heartbeat_timeout: 90

machine:
  hostname: "macbook"
  monitor_host: "macmini"
EOF

# Install LaunchAgents
echo -e "\n${YELLOW}Installing LaunchAgents...${NC}"

# Publisher
cat > ~/Library/LaunchAgents/com.hermes.publisher.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.hermes.publisher</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>~/.hermes-agentpet/publisher/hermes_mqtt_publisher.py</string>
        <string>--config</string>
        <string>~/.hermes-agentpet/publisher/config.yaml</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>HERMES_MQTT_SOCKET</key>
        <string>/tmp/hermes-mqtt.sock</string>
    </dict>
    <key>StandardOutPath</key>
    <string>~/Library/Logs/hermes-agentpet/publisher.log</string>
    <key>StandardErrorPath</key>
    <string>~/Library/Logs/hermes-agentpet/publisher.log</string>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
EOF

# Subscriber
cat > ~/Library/LaunchAgents/com.hermes.subscriber.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.hermes.subscriber</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>~/.hermes-agentpet/subscriber/hermes_mqtt_subscriber.py</string>
        <string>--config</string>
        <string>~/.hermes-agentpet/subscriber/config.yaml</string>
    </array>
    <key>StandardOutPath</key>
    <string>~/Library/Logs/hermes-agentpet/subscriber.log</string>
    <key>StandardErrorPath</key>
    <string>~/Library/Logs/hermes-agentpet/subscriber.log</string>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
EOF

# Install hook bridge
chmod +x ~/.hermes-agentpet/hooks/hermes-mqtt-bridge.sh

echo -e "\n${GREEN}✅ Installation complete!${NC}"
echo ""
echo "Next steps:"
echo "1. Start MQTT broker:  mosquitto -c ~/.hermes-agentpet/mosquitto.conf"
echo "2. Start Publisher:     launchctl load ~/Library/LaunchAgents/com.hermes.publisher.plist"
echo "3. Start Subscriber:    launchctl load ~/Library/LaunchAgents/com.hermes.subscriber.plist"
echo "4. Configure Hermes to call the hook bridge on events"
echo ""
echo "Or just run the scripts directly for testing:"
echo "   python3 ~/.hermes-agentpet/publisher/hermes_mqtt_publisher.py"
echo "   python3 ~/.hermes-agentpet/subscriber/hermes_mqtt_subscriber.py"
