# Hermes MQTT Agent Pet

MQTT bridge for Hermes Agent — publishes Blacky's status in real-time to a local MQTT broker, displays status in the menubar, and controls AgentPet animations.

## Components

- **Publisher** — Runs on the same machine as Hermes. Listens for hook events via Unix socket, publishes to MQTT.
- **Subscriber** — Runs on your MacBook. Subscribes to MQTT topics, updates menubar status.
- **Hook Bridge** — Bridge script that Hermes calls on events.

## Quick Start

1. Install Mosquitto: `brew install mosquitto`
2. Run installer: `bash ~/.hermes-agentpet/install.sh`
3. Start broker: `mosquitto -c ~/.hermes-agentpet/mosquitto.conf`
4. Start Publisher: `launchctl load ~/Library/LaunchAgents/com.hermes.publisher.plist`
5. Start Subscriber: `launchctl load ~/Library/LaunchAgents/com.hermes.subscriber.plist`

## MQTT Topics

| Topic | Purpose |
|---|---|
| `hermes/status/{hostname}` | Current status snapshot (retained) |
| `hermes/pet/state` | Blacky animation state (retained) |
| `hermes/tokens/summary` | Token counts (retained) |
| `hermes/meta/heartbeat` | Heartbeat every 60s (retained) |
| `hermes/session/start` | Session start event |
| `hermes/session/end` | Session end event |
| `hermes/llm/pre_call` | Pre LLM call |
| `hermes/llm/post_call` | Post LLM call |
| `hermes/tool/pre_call` | Pre tool call |
| `hermes/tool/post_call` | Post tool call |
| `hermes/subagent/start` | Subagent start |
| `hermes/subagent/stop` | Subagent stop |
| `hermes/llm/error` | API error |
| `hermes/cron/summary` | Cron job summary |
| `hermes/cmd/+` | Remote commands |

## Blacky States

| Status | Blacky | Icon |
|---|---|---|
| offline | idle | 😴 |
| idle | idle | 🐉 |
| waiting | waiting | 🐉 |
| thinking | runRight | 🔥 |
| working | runLeft | ⚒️ |
| cron_active | jumping | ⏰ |
| subagent | running | 🐲 |
| review | review | 📖 |
| error | failed | 💥 |
| done | waving | ✅ |

## Configuration

Edit `~/.hermes-agentpet/publisher/config.yaml` and `~/.hermes-agentpet/subscriber/config.yaml`.

## Testing

```bash
# Test publisher
python3 ~/.hermes-agentpet/publisher/hermes_mqtt_publisher.py --config ~/.hermes-agentpet/publisher/config.yaml

# Test subscriber
python3 ~/.hermes-agentpet/subscriber/hermes_mqtt_subscriber.py --config ~/.hermes-agentpet/subscriber/config.yaml

# Subscribe to all topics
mosquitto_sub -t "hermes/#" -v
```

## Troubleshooting

- **Publisher not connecting:** Check Mosquitto is running (`brew services list | grep mosquitto`)
- **Subscriber not receiving:** Check both Publisher and Subscriber are connected to the same broker
- **Menubar not showing:** Ensure PyObjC is installed (`pip3 install pyobjc`)
