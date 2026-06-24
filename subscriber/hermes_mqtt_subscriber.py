#!/usr/bin/env python3
"""
hermes_mqtt_subscriber.py — MQTT subscriber for Hermes Agent events.

Runs on the MacBook. Subscribes to MQTT topics from the Mac Mini,
updates the menubar status, and controls AgentPet animations.

Usage:
    python3 hermes_mqtt_subscriber.py --config config.yaml
"""

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import yaml

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("hermes-subscriber")

# ── Configuration ────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    """Load YAML configuration."""
    config_path = Path(path).expanduser()
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        sys.exit(1)
    with open(config_path) as f:
        return yaml.safe_load(f)

# ── State Machine ────────────────────────────────────────────────────────────

class PetStateMachine:
    """Manages Blacky's animation states."""

    # State priority: higher = takes precedence
    STATE_PRIORITY = {
        "error":       10,
        "subagent":    9,
        "cron_active": 8,
        "thinking":    7,
        "working":     7,
        "review":      6,
        "waiting":     5,
        "done":        4,
        "idle":        1,
        "offline":     0,
    }

    # Status → Blacky animation mapping
    STATUS_MAP = {
        "offline":     "idle",
        "idle":        "idle",
        "waiting":     "waiting",
        "thinking":    "runRight",
        "working":     "runLeft",
        "cron_active": "jumping",
        "subagent":    "running",
        "review":      "review",
        "error":       "failed",
        "done":        "waving",
    }

    # Status → Menubar icon
    ICON_MAP = {
        "offline":     "😴",
        "idle":        "🐉",
        "waiting":     "🐉",
        "thinking":    "🔥",
        "working":     "⚒️",
        "cron_active": "⏰",
        "subagent":    "🐲",
        "review":      "📖",
        "error":       "💥",
        "done":        "✅",
    }

    def __init__(self):
        self.current_status = "offline"
        self.current_blacky = "idle"
        self.session_id = ""
        self.model = ""
        self.turns = 0
        self.tokens = {"input": 0, "output": 0, "total": 0, "cache_hit_rate": 0}
        self.subagents = {"active": 0, "completed": 0}
        self.cron = {"running": 0, "scheduled": 0}
        self._last_update = time.time()
        self._error_timer = 0

    def update_status(self, new_status: str):
        """Update status with priority checking."""
        if self.STATE_PRIORITY.get(new_status, 0) >= self.STATE_PRIORITY.get(self.current_status, 0):
            old = self.current_status
            self.current_status = new_status
            self.current_blacky = self.STATUS_MAP.get(new_status, "idle")
            self._last_update = time.time()
            if old != new_status:
                logger.info(f"Status: {old} → {new_status} (Blacky: {self.current_blacky})")

        # Auto-recover from error after 10s
        if new_status == "error":
            self._error_timer = time.time() + 10

    def check_error_recovery(self):
        """Auto-recover from error state."""
        if self.current_status == "error" and time.time() > self._error_timer:
            self.current_status = "idle"
            self.current_blacky = "idle"
            logger.info("Status: error → idle (auto-recovery)")

    def get_icon(self) -> str:
        """Get current menubar icon."""
        return self.ICON_MAP.get(self.current_status, "🐉")

    def get_tooltip(self) -> str:
        """Get menubar tooltip text."""
        lines = [
            f"{self.get_icon()} Blacky — Hermes Status",
            "═" * 30,
            f"Status:   {self.current_status}",
            f"Blacky:   {self.current_blacky}",
            f"Session:  {self.session_id or 'N/A'}",
            f"Model:    {self.model or 'N/A'}",
            "═" * 30,
            f"Tokens:   {self.tokens['total']:,} total",
            f"          {self.tokens['input']:,} in / {self.tokens['output']:,} out",
            f"Cache:    {self.tokens.get('cache_hit_rate', 0)*100:.0f}% hit rate",
            f"Calls:    {self.turns} turns",
            "═" * 30,
            f"Subagents: {self.subagents['active']} active / {self.subagents['completed']} done",
            f"Cron:     {self.cron.get('running', 0)} running / {self.cron.get('scheduled', 0)} scheduled",
        ]
        return "\n".join(lines)

# ── Menubar (PyObjC) ────────────────────────────────────────────────────────

class MenubarStatus:
    """macOS menubar status icon."""

    def __init__(self):
        try:
            from Foundation import NSUserDefaults
            from AppKit import NSStatusBar, NSStatusItem, NSImage, NSMenu, NSMenuItem, NSTextField
            self._NSStatusBar = NSStatusBar
            self._NSStatusItem = NSStatusItem
            self._NSImage = NSImage
            self._NSMenu = NSMenu
            self._NSMenuItem = NSMenuItem
            self._NSTextField = NSTextField
            self._NSDefaults = NSUserDefaults
            self._initialized = True
            self._status_item = None
            self._menu = None
            self._text_field = None
        except ImportError:
            logger.warning("PyObjC not available, menubar disabled")
            self._initialized = False

    def setup(self):
        """Initialize the menubar status."""
        if not self._initialized:
            return

        statusBar = self._NSStatusBar.systemStatusBar()
        self._status_item = statusBar.statusItemWithLength_(
            self._NSStatusItem.variableStatusItemLength_
        )

        # Create menu
        self._menu = self._NSMenu.new()
        self._text_field = self._NSTextField.alloc().initWithFrame_((0, 0, 300, 200))
        self._text_field.setBezeled_(False)
        self._text_field.setDrawsBackground_(False)
        self._text_field.setEditable_(False)
        self._text_field.setSelectable_(False)
        self._text_field.setNumberOfLines_(0)
        self._text_field.setUsesSingleLineMode_(False)

        # Add menu items
        quit_item = self._NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            "Quit", "terminate:", ""
        )
        self._menu.addItem_(quit_item)

        self._status_item.setMenu_(self._menu)
        self.update_display("🐉", "Blacky — offline")
        logger.info("Menubar initialized")

    def update_display(self, icon: str, tooltip: str):
        """Update menubar icon and tooltip."""
        if not self._initialized or not self._status_item:
            return

        title = f"{icon} {tooltip}"
        self._status_item.setTitle_(title)

        if self._text_field:
            self._text_field.setStringValue_(tooltip)
            self._menu.addItemWithSubview_(self._text_field)

    def show(self):
        """Show the menubar status."""
        if self._status_item:
            self._status_item.setVisible_(True)

    def hide(self):
        """Hide the menubar status."""
        if self._status_item:
            self._status_item.setVisible_(False)

# ── MQTT Subscriber ──────────────────────────────────────────────────────────

class MQTTPublisher:
    """Subscribe to MQTT topics and update state."""

    def __init__(self, config: dict, state: PetStateMachine, menubar: MenubarStatus):
        self.config = config
        self.state = state
        self.menubar = menubar
        self.prefix = config["broker"].get("topic_prefix", "hermes")

        self.client = mqtt.Client(
            client_id=config["broker"]["client_id"],
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        broker_config = config["broker"]
        if broker_config.get("username"):
            self.client.username_pw_set(
                username=broker_config.get("username", ""),
                password=broker_config.get("password", ""),
            )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self._connected = False
        self._heartbeat_timeout = config.get("display", {}).get("heartbeat_timeout", 90)

    def connect(self, host: str, port: int):
        """Connect to MQTT broker."""
        self.client.connect(host, port, keepalive=config["broker"].get("keepalive", 60))
        self.client.loop_start()
        logger.info(f"Connecting to {host}:{port}...")

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        # HA MQTT sometimes returns rc=1 (Not authorized) but still works
        if reason_code in (0, 1):
            self._connected = True
            logger.info("✅ MQTT connected")
            # Subscribe to topics
            topics = [
                f"{self.prefix}/status/#",
                f"{self.prefix}/session/#",
                f"{self.prefix}/llm/#",
                f"{self.prefix}/tool/#",
                f"{self.prefix}/subagent/#",
                f"{self.prefix}/tokens/#",
                f"{self.prefix}/cron/#",
                f"{self.prefix}/pet/state",
                f"{self.prefix}/meta/heartbeat",
            ]
            for topic in topics:
                client.subscribe(topic, qos=1)
                logger.info(f"Subscribed to {topic}")
        else:
            logger.debug(f"MQTT connect rc={reason_code}")

    def _on_disconnect(self, client, userdata, reason_code, properties):
        self._connected = False
        logger.info(f"MQTT disconnected (reason: {reason_code})")
        self.state.update_status("offline")
        self._update_display()

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT message."""
        try:
            payload = json.loads(msg.payload.decode())
        except json.JSONDecodeError:
            logger.warning(f"Bad JSON on {msg.topic}: {msg.payload[:100]}")
            return

        topic = msg.topic
        self._handle_message(topic, payload)

    def _handle_message(self, topic: str, payload: dict):
        """Route message to handler."""
        if "session/start" in topic:
            self.state.update_status("waiting")
            self.state.session_id = payload.get("session_id", self.state.session_id)

        elif "session/end" in topic:
            self.state.update_status("done")

        elif "llm/pre_call" in topic:
            self.state.update_status("thinking")
            self.state.model = payload.get("model", self.state.model)
            self.state.turns += 1

        elif "llm/post_call" in topic:
            self.state.tokens["input"] += payload.get("input_tokens", 0)
            self.state.tokens["output"] += payload.get("output_tokens", 0)
            self.state.tokens["total"] = self.state.tokens["input"] + self.state.tokens["output"]
            self.state.tokens["cache_hit_rate"] = payload.get("cache_hit_rate", 0)
            self.state.update_status("idle")

        elif "tool/pre_call" in topic:
            self.state.update_status("working")

        elif "tool/post_call" in topic:
            self.state.update_status("idle")

        elif "subagent/start" in topic:
            self.state.subagents["active"] += 1
            self.state.update_status("subagent")

        elif "subagent/stop" in topic:
            self.state.subagents["active"] = max(0, self.state.subagents["active"] - 1)
            self.state.subagents["completed"] += 1
            if self.state.subagents["active"] == 0:
                self.state.update_status("idle")

        elif "llm/error" in topic:
            self.state.update_status("error")

        elif "status/" in topic:
            # Full status snapshot
            status = payload.get("status", "idle")
            self.state.update_status(status)
            self.state.session_id = payload.get("session", {}).get("id", self.state.session_id)
            self.state.model = payload.get("session", {}).get("model", self.state.model)
            self.state.turns = payload.get("session", {}).get("turns", self.state.turns)
            self.state.tokens = payload.get("tokens", self.state.tokens)
            self.state.subagents = payload.get("subagents", self.state.subagents)
            self.state.cron = payload.get("cron", self.state.cron)

        elif "tokens/summary" in topic:
            # Token summary
            tokens = payload.get("session_tokens", {})
            self.state.tokens = {
                "input": tokens.get("input", self.state.tokens["input"]),
                "output": tokens.get("output", self.state.tokens["output"]),
                "total": tokens.get("total", self.state.tokens["total"]),
                "cache_hit_rate": tokens.get("cache_hit_rate", 0),
            }

        elif "cron/summary" in topic:
            # Cron summary
            self.state.cron = {
                "running": payload.get("running_count", 0),
                "scheduled": payload.get("scheduled_count", 0),
            }
            if self.state.cron["running"] > 0:
                self.state.update_status("cron_active")

        # Update display
        self._update_display()

    def _update_display(self):
        """Update menubar and state."""
        self.state.check_error_recovery()
        icon = self.state.get_icon()
        tooltip = f"{self.state.current_status} | {self.state.tokens['total']:,} tokens"
        self.menubar.update_display(icon, tooltip)

    def disconnect(self):
        """Disconnect from broker."""
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("MQTT disconnected")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Hermes MQTT Subscriber")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--no-menubar", action="store_true", help="Disable menubar")
    parser.add_argument("--broker-host", default=None, help="Override broker host (from config)")
    parser.add_argument("--broker-port", type=int, default=None, help="Override broker port (from config)")
    args = parser.parse_args()

    config = load_config(args.config)

    # Create state
    state = PetStateMachine()

    # Create menubar
    menubar = MenubarStatus()
    if not args.no_menubar:
        menubar.setup()
        menubar.show()

    # Create subscriber
    publisher = MQTTPublisher(config, state, menubar)

    # Connect to broker
    broker_host = args.broker_host or config["broker"]["host"]
    broker_port = args.broker_port or config["broker"]["port"]
    publisher.connect(broker_host, broker_port)

    # Wait for connection (HA MQTT may take longer)
    for _ in range(10):
        if publisher._connected:
            break
        time.sleep(0.5)
    if not publisher._connected:
        logger.error("❌ Could not connect to MQTT broker")
        sys.exit(1)

    # PID file
    pid_path = Path.home() / ".hermes-agentpet" / "state" / "subscriber.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))

    logger.info(f"✅ Hermes MQTT Subscriber running (PID: {os.getpid()})")
    logger.info(f"   Broker: {broker_host}:{broker_port}")
    logger.info(f"   Menubar: {'enabled' if not args.no_menubar else 'disabled'}")

    # Signal handling
    def shutdown(signum, frame):
        logger.info("Shutting down...")
        publisher.disconnect()
        menubar.hide()
        if pid_path.exists():
            pid_path.unlink()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Main loop
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(None, None)

if __name__ == "__main__":
    main()
