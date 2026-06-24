#!/usr/bin/env python3
"""
hermes_mqtt_publisher.py — MQTT publisher for Hermes Agent events.

Runs on the same machine as Hermes. Listens for hook events via a Unix socket,
publishes enriched JSON to the MQTT broker, and runs background pollers for
tokens and cron jobs.

Usage:
    python3 hermes_mqtt_publisher.py --config config.yaml
"""

import argparse
import json
import logging
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
import yaml

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("hermes-publisher")

# ── Configuration ────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    """Load YAML configuration."""
    config_path = Path(path).expanduser()
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        sys.exit(1)
    with open(config_path) as f:
        return yaml.safe_load(f)

# ── State Tracker ────────────────────────────────────────────────────────────

class HermesState:
    """Tracks current Hermes agent state."""

    def __init__(self):
        self.current_status = "offline"
        self.current_blacky = "idle"
        self.session_id: str = ""
        self.model: str = ""
        self.turns: int = 0
        self.tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "total": 0}
        self.subagents = {"active": 0, "completed": 0}
        self.cron = {"running": 0, "scheduled": 0}
        self._last_update = time.time()

    def update_from_hook(self, event_name: str, payload: dict):
        """Update state based on hook event."""
        self._last_update = time.time()

        if event_name == "on_session_start":
            self.current_status = "waiting"
            self.current_blacky = "waiting"
            self.session_id = payload.get("session_id", "")
            self.model = payload.get("model", "")
            self.turns = 0
            self.tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "total": 0}
            logger.info(f"Session started: {self.session_id}")

        elif event_name == "on_session_end":
            self.current_status = "done"
            self.current_blacky = "waving"
            logger.info(f"Session ended: {self.session_id}")

        elif event_name == "pre_llm_call":
            self.current_status = "thinking"
            self.current_blacky = "runRight"
            self.model = payload.get("model", self.model)
            self.turns += 1
            logger.info(f"LLM call #{self.turns}: {self.model}")

        elif event_name == "post_llm_call":
            inp = payload.get("input_tokens", 0)
            out = payload.get("output_tokens", 0)
            cache_read = payload.get("cache_read_tokens", 0)
            cache_write = payload.get("cache_write_tokens", 0)
            self.tokens["input"] += inp
            self.tokens["output"] += out
            self.tokens["cache_read"] += cache_read
            self.tokens["cache_write"] += cache_write
            self.tokens["total"] += inp + out
            self.current_status = "idle"
            self.current_blacky = "idle"
            logger.info(f"LLM done: +{inp}in/{out}out (total: {self.tokens['total']})")

        elif event_name == "pre_tool_call":
            self.current_status = "working"
            self.current_blacky = "runLeft"
            tool_name = payload.get("tool_name", "unknown")
            logger.info(f"Tool: {tool_name}")

        elif event_name == "post_tool_call":
            self.current_status = "idle"
            self.current_blacky = "idle"

        elif event_name == "subagent_start":
            self.current_status = "subagent"
            self.current_blacky = "running"
            self.subagents["active"] += 1
            role = payload.get("child_role", "Unknown")
            logger.info(f"Subagent started: {role} (active: {self.subagents['active']})")

        elif event_name == "subagent_stop":
            self.subagents["active"] = max(0, self.subagents["active"] - 1)
            self.subagents["completed"] += 1
            if self.subagents["active"] == 0:
                self.current_status = "idle"
                self.current_blacky = "idle"
            role = payload.get("child_role", "Unknown")
            logger.info(f"Subagent done: {role} (active: {self.subagents['active']})")

        elif event_name == "api_request_error":
            self.current_status = "error"
            self.current_blacky = "failed"
            error = payload.get("error_message", "Unknown error")
            logger.warning(f"API error: {error}")

        elif event_name == "on_session_finalize":
            self.current_status = "idle"
            self.current_blacky = "idle"

    def get_snapshot(self) -> dict:
        """Get current state snapshot."""
        cache_hit = 0
        if self.tokens["input"]:
            cache_hit = round(self.tokens["cache_read"] / self.tokens["input"], 2)
        return {
            "hostname": self.hostname,
            "timestamp": int(time.time()),
            "status": self.current_status,
            "blacky_state": self.current_blacky,
            "session": {
                "id": self.session_id,
                "model": self.model,
                "turns": self.turns,
            },
            "tokens": {
                **self.tokens,
                "cache_hit_rate": cache_hit,
            },
            "subagents": self.subagents.copy(),
            "cron": self.cron.copy(),
        }

    def get_token_summary(self) -> dict:
        """Get token summary for retained topic."""
        cache_hit = 0
        if self.tokens["input"]:
            cache_hit = round(self.tokens["cache_read"] / self.tokens["input"], 2)
        return {
            "hostname": self.hostname,
            "timestamp": int(time.time()),
            "session_id": self.session_id,
            "session_tokens": {
                **self.tokens,
                "api_call_count": self.turns,
                "cache_hit_rate": cache_hit,
            },
        }

    def get_pet_state(self) -> dict:
        """Get pet state for retained topic."""
        return {
            "hostname": self.hostname,
            "timestamp": int(time.time()),
            "blacky_state": self.current_blacky,
            "hermes_status": self.current_status,
        }

# ── MQTT Publisher ───────────────────────────────────────────────────────────

class MQTTPublisher:
    """Publishes Hermes events to MQTT broker."""

    def __init__(self, config: dict, state: HermesState):
        self.config = config
        self.state = state
        self.hostname = config["machine"]["hostname"]
        broker = config["broker"]
        self.prefix = broker.get("topic_prefix", "hermes")

        self.client = mqtt.Client(
            client_id=broker["client_id"],
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.client.username_pw_set(
            username=broker.get("username", ""),
            password=broker.get("password", ""),
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish

        # Last will
        lw = config.get("last_will", {})
        if lw:
            self.client.will_set(
                topic=lw["topic"],
                payload=lw["payload"],
                qos=lw.get("qos", 1),
                retain=lw.get("retain", True),
            )

        self._connected = False
        self._heartbeat_interval = config.get("polling", {}).get("heartbeat_interval_seconds", 60)
        self._token_interval = config.get("polling", {}).get("token_interval_seconds", 5)

    def connect(self, host: str, port: int):
        """Connect to MQTT broker."""
        broker = self.config["broker"]
        self.client.connect(
            host,
            port,
            keepalive=broker.get("keepalive", 60),
        )
        self.client.loop_start()
        logger.info(f"Connecting to {host}:{port}...")

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self._connected = True
            logger.info("✅ MQTT connected")
            self.publish_status()
            self.publish_pet_state()
            self.publish_token_summary()
            # Start background threads
            threading.Thread(target=self._heartbeat_loop, daemon=True).start()
            threading.Thread(target=self._token_summary_loop, daemon=True).start()
        else:
            logger.warning(f"MQTT connect failed: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected = False
        logger.info(f"MQTT disconnected (reason: {reason_code})")

    def _on_publish(self, client, userdata, mid):
        pass  # Silent publish confirm

    def publish(self, topic: str, payload: dict, retain: bool = False):
        """Publish a message to a topic."""
        full_topic = f"{self.prefix}/{topic}"
        try:
            self.client.publish(full_topic, json.dumps(payload), qos=1, retain=retain)
        except Exception as e:
            logger.error(f"Publish failed ({topic}): {e}")

    def publish_status(self):
        """Publish current status snapshot."""
        self.publish(f"status/{self.hostname}", self.state.get_snapshot(), retain=True)

    def publish_pet_state(self):
        """Publish Blacky's current state."""
        self.publish("pet/state", self.state.get_pet_state(), retain=True)

    def publish_token_summary(self):
        """Publish token summary."""
        self.publish("tokens/summary", self.state.get_token_summary(), retain=True)

    def publish_hook_event(self, event_name: str, payload: dict):
        """Publish a hook event."""
        topic_map = {
            "on_session_start": "session/start",
            "on_session_end": "session/end",
            "pre_llm_call": "llm/pre_call",
            "post_llm_call": "llm/post_call",
            "pre_tool_call": "tool/pre_call",
            "post_tool_call": "tool/post_call",
            "subagent_start": "subagent/start",
            "subagent_stop": "subagent/stop",
            "api_request_error": "llm/error",
            "on_session_finalize": "session/finalize",
        }
        topic = topic_map.get(event_name, f"event/{event_name}")
        enriched = {
            "hostname": self.hostname,
            "timestamp": int(time.time()),
            "hook_event_name": event_name,
            **payload,
        }
        self.publish(topic, enriched)

    def _heartbeat_loop(self):
        """Send heartbeat every N seconds."""
        while True:
            time.sleep(self._heartbeat_interval)
            if self._connected:
                self.publish("meta/heartbeat", {
                    "hostname": self.hostname,
                    "timestamp": int(time.time()),
                    "status": self.state.current_status,
                    "blacky_state": self.state.current_blacky,
                    "broker_connected": True,
                }, retain=True)

    def _token_summary_loop(self):
        """Publish token summary every N seconds."""
        while True:
            time.sleep(self._token_interval)
            if self._connected:
                self.publish_token_summary()

    def disconnect(self):
        """Disconnect from broker."""
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("MQTT disconnected")

# ── Hook Handler (Unix Socket) ──────────────────────────────────────────────

class HookHandler:
    """Listens for hook events on a Unix socket."""

    def __init__(self, socket_path: str, publisher: MQTTPublisher, state: HermesState):
        self.socket_path = socket_path
        self.publisher = publisher
        self.state = state
        self._running = False

    def start(self):
        """Start listening on the Unix socket."""
        # Remove old socket
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(self.socket_path)
        self.sock.listen(5)
        self._running = True
        logger.info(f"Listening on {self.socket_path}")

        while self._running:
            self.sock.settimeout(1.0)
            try:
                conn, _ = self.sock.accept()
                data = conn.recv(4096).decode("utf-8")
                conn.close()
                if data:
                    self._handle_message(data)
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"Socket error: {e}")

        self.sock.close()

    def stop(self):
        """Stop listening."""
        self._running = False
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

    def _handle_message(self, data: str):
        """Process a hook event message."""
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            logger.warning(f"Bad JSON from hook: {data[:100]}")
            return

        event_name = payload.get("hook_event_name", "")
        if not event_name:
            logger.warning(f"No event name in: {data[:100]}")
            return

        # Update state
        self.state.update_from_hook(event_name, payload)

        # Publish to MQTT
        self.publisher.publish_hook_event(event_name, payload)
        self.publisher.publish_status()
        self.publisher.publish_pet_state()

        logger.info(f"Hook: {event_name} → {self.state.current_blacky}")

# ── Token Poller (SQLite) ───────────────────────────────────────────────────

class TokenPoller:
    """Polls state.db for token counts."""

    def __init__(self, state: HermesState, publisher: MQTTPublisher, interval: int = 5):
        self.state = state
        self.publisher = publisher
        self.interval = interval
        self._running = False
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)

    def start(self):
        """Start polling."""
        self._running = True
        self._thread.start()
        logger.info(f"Token poller started (interval: {self.interval}s)")

    def stop(self):
        """Stop polling."""
        self._running = False

    def _poll_loop(self):
        """Main polling loop."""
        while self._running:
            time.sleep(self.interval)
            try:
                self._poll_tokens()
            except Exception as e:
                logger.error(f"Token poll error: {e}")

    def _poll_tokens(self):
        """Read tokens from state.db."""
        db_path = Path(self.state.home_dir) / "state.db"
        if not db_path.exists():
            return

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT input_tokens, output_tokens, cache_read_tokens, cache_write_tokens "
                "FROM sessions WHERE ended_at IS NULL LIMIT 1"
            )
            row = cursor.fetchone()
            conn.close()

            if row:
                inp, out, cr, cw = row
                self.state.tokens["input"] = inp or 0
                self.state.tokens["output"] = out or 0
                self.state.tokens["cache_read"] = cr or 0
                self.state.tokens["cache_write"] = cw or 0
                self.state.tokens["total"] = (inp or 0) + (out or 0)
                if self.publisher._connected:
                    self.publisher.publish_token_summary()
        except Exception as e:
            logger.debug(f"SQLite poll: {e}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Hermes MQTT Publisher")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--socket", default="/tmp/hermes-mqtt.sock", help="Unix socket path")
    parser.add_argument("--broker-host", default="127.0.0.1", help="Override broker host")
    parser.add_argument("--broker-port", type=int, default=1883, help="Override broker port")
    args = parser.parse_args()

    config = load_config(args.config)
    hostname = config["machine"]["hostname"]

    # Create state
    state = HermesState()
    state.hostname = hostname
    state.home_dir = os.path.expanduser(config["machine"]["hermes_home"])

    # Create publisher
    publisher = MQTTPublisher(config, state)

    # Connect to broker
    broker_host = args.broker_host or config["broker"]["host"]
    broker_port = args.broker_port or config["broker"]["port"]
    publisher.connect(broker_host, broker_port)

    # Wait for connection
    time.sleep(2)
    if not publisher._connected:
        logger.error("❌ Could not connect to MQTT broker")
        sys.exit(1)

    # Start hook handler
    handler = HookHandler(args.socket, publisher, state)
    hook_thread = threading.Thread(target=handler.start, daemon=True)
    hook_thread.start()

    # Start token poller
    poller = TokenPoller(state, publisher, interval=config.get("polling", {}).get("token_interval_seconds", 5))
    poller.start()

    # Signal handling
    def shutdown(signum, frame):
        logger.info("Shutting down...")
        handler.stop()
        poller.stop()
        publisher.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # PID file
    pid_path = Path.home() / ".hermes-agentpet" / "state" / "publisher.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))

    logger.info(f"✅ Hermes MQTT Publisher running (PID: {os.getpid()})")
    logger.info(f"   Socket: {args.socket}")
    logger.info(f"   Broker: {broker_host}:{broker_port}")
    logger.info(f"   Hostname: {hostname}")

    # Main loop (keep alive)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(None, None)

if __name__ == "__main__":
    main()
