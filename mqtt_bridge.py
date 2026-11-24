"""MQTT bridge to Home Assistant (read-only v1) — see docs/adr-mqtt-bridge.md.

One shared client publishes printer state to the user's own broker with HA
discovery. The bridge never opens a second connection to any printer: it
fans out from the status cache (state.printers), which the existing printer
connections already populate.

Lifecycle: ensure_started() is idempotent and decides from the feature flag
and the broker URL; stop_and_clear() disconnects and empties every retained
discovery payload ("off means off" — entities vanish, not go stale). Both
are called from server startup and via on_change hooks, never at import.
"""

import asyncio
import json
import urllib.parse

try:
    import aiomqtt
except ImportError:     # bridge stays off when the optional dep is absent
    aiomqtt = None

import config
import features
import state

NUMERIC_THROTTLE_S = 5.0    # numerics republish at most this often

_client_id = "spooler-bridge"

_task: asyncio.Task = None
_running = False


# ── discovery registry ───────────────────────────────────────────────────────
# One HA device per printer; every entity reads the single retained state
# topic through a value_template, mirroring what the browser UI reads.

BRIDGE_AVAILABILITY = "spooler/bridge/availability"

TYPE_MODELS = {
    "cc1": "Centauri Carbon (CC1)",
    "cc2": "Centauri Carbon 2 (CC2)",
    "prusa": "Prusa (PrusaLink)",
    "moonraker": "Klipper (Moonraker)",
}

# (key, component, friendly name, unit, device_class, state_class, nullable, entity_category)
SENSORS = [
    ("state",            "sensor", "Status",             None, None,          None,          False, None),
    ("nozzle_temp",      "sensor", "Nozzle temperature", "°C", "temperature", "measurement", False, None),
    ("nozzle_target",    "sensor", "Nozzle target",      "°C", "temperature", "measurement", False, None),
    ("bed_temp",         "sensor", "Bed temperature",    "°C", "temperature", "measurement", False, None),
    ("bed_target",       "sensor", "Bed target",         "°C", "temperature", "measurement", False, None),
    ("chamber_temp",     "sensor", "Chamber temperature", "°C", "temperature", "measurement", False, None),
    ("chamber_target",   "sensor", "Chamber target",     "°C", "temperature", "measurement", False, None),
    ("progress",         "sensor", "Progress",           "%",  None,          "measurement", True,  None),
    ("layer",            "sensor", "Layer",              None, None,          "measurement", False, None),
    ("total_layer",      "sensor", "Total layers",       None, None,          "measurement", False, None),
    ("time_remaining",   "sensor", "Time remaining",     "s",  None,          "measurement", False, None),
    ("filament_used_mm", "sensor", "Filament used",      "mm", None,          "measurement", False, None),
    ("filament_used_g",  "sensor", "Filament used",      "g",  None,          "measurement", False, None),
    ("speed_factor",     "sensor", "Speed factor",       "%",  None,          "measurement", False, None),
    ("firmware",         "sensor", "Firmware",           None, None,          None,          True,  "diagnostic"),
]

BINARY_SENSORS = [("connected", "Connected"), ("printing", "Printing")]


def _state_topic(pid: str) -> str:
    return f"spooler/{pid}/state"


def _discovery_topic(component: str, pid: str, key: str) -> str:
    return f"homeassistant/{component}/spooler/{pid}/{key}/config"


def _template(key: str, nullable: bool) -> str:
    if nullable:
        return f"{{{{ value_json.{key} if value_json.{key} is not none else 'unknown' }}}}"
    return f"{{{{ value_json.{key} }}}}"


def _device(pid: str, d: dict) -> dict:
    t = d.get("printer_type")
    dev = {"identifiers": [pid], "name": d.get("name") or pid}
    model = TYPE_MODELS.get(t)
    if model:
        dev["model"] = model
    if t in ("cc1", "cc2"):
        dev["manufacturer"] = "Elegoo"
    fw = d.get("firmware_version")
    if fw:
        dev["sw_version"] = str(fw)
    return dev


def _entity_payload(pid: str, d: dict, component: str, key: str, name: str,
                    unit=None, device_class=None, state_class=None,
                    nullable=False, category=None, binary=False) -> dict:
    p = {
        "name": name,
        "state_topic": _state_topic(pid),
        "value_template": _template(key, nullable),
        "unique_id": f"spooler-{pid}-{key}",
        "availability_topic": BRIDGE_AVAILABILITY,
        "device": _device(pid, d),
    }
    if unit:
        p["unit_of_measurement"] = unit
    if device_class:
        p["device_class"] = device_class
    if state_class:
        p["state_class"] = state_class
    if category:
        p["entity_category"] = category
    if binary:
        p["payload_on"] = "True"
        p["payload_off"] = "False"
    return p


def _discovery_messages(pid: str, d: dict):
    """(topic, json payload) for every discovered entity of one printer."""
    for key, component, name, unit, device_class, state_class, nullable, category in SENSORS:
        payload = _entity_payload(pid, d, component, key, name, unit, device_class,
                                  state_class, nullable, category)
        yield _discovery_topic(component, pid, key), json.dumps(payload)
    for key, name in BINARY_SENSORS:
        payload = _entity_payload(pid, d, "binary_sensor", key, name, binary=True)
        yield _discovery_topic("binary_sensor", pid, key), json.dumps(payload)


# ── normalized state extraction ──────────────────────────────────────────────
# ports of the frontend's rules so HA sees exactly what the UI sees:
# app-render.js's key fallback chains, app-core.js's getProgress(), and
# classify_display_state's value already arrive via to_dict().

def _first_num(d: dict, *keys, default=None):
    for k in keys:
        v = d.get(k)
        if isinstance(v, (int, float)):
            return v
    return default


def _progress(pi: dict):
    if pi.get("TotalTicks") == 100:
        return max(0, min(100, round(pi.get("CurrentTicks") or 0)))
    if (pi.get("TotalLayer") or 0) > 0 and (pi.get("CurrentLayer") or 0) > 0:
        return min(100, round((pi["CurrentLayer"] / pi["TotalLayer"]) * 100))
    if (pi.get("TotalTicks") or 0) > 0 and pi.get("CurrentTicks") is not None:
        return min(100, round((pi["CurrentTicks"] / pi["TotalTicks"]) * 100))
    return None


def bridge_state(printer: dict) -> dict:
    """Normalize a printer's to_dict() into the bridge's flat state dict.

    Input is the shape http_handler sends browsers: status keys differ per
    protocol (CC1 SDCP vs CC2/Prusa/Moonraker), so fall back the same way
    the frontend does. progress is None when the printer reports nothing
    usable; numerics default to 0 like the UI's "!connected ? 0" rule."""
    st = printer.get("status") or {}
    pi = st.get("PrintInfo") or {}
    return {
        "state":            printer.get("state"),
        "connected":        bool(printer.get("connected")),
        "printing":         printer.get("state") == "printing",
        "nozzle_temp":      _first_num(st, "TempOfNozzle", "NozzleTemp", default=0),
        "nozzle_target":    _first_num(st, "TempTargetNozzle", "NozzleTempTarget", default=0),
        "bed_temp":         _first_num(st, "TempOfHotbed", "BedTemp", default=0),
        "bed_target":       _first_num(st, "TempTargetHotbed", "BedTempTarget", default=0),
        "chamber_temp":     _first_num(st, "TempOfBox", "ChamberTemp", default=0),
        "chamber_target":   _first_num(st, "TempTargetBox", default=0),
        "progress":         _progress(pi),
        "layer":            pi.get("CurrentLayer") or 0,
        "total_layer":      pi.get("TotalLayer") or 0,
        "time_remaining":   pi.get("RemainTime") or 0,
        "filament_used_mm": printer.get("filament_mm"),
        "filament_used_g":  printer.get("filament_g"),
        "speed_factor":     _first_num(st, "SpeedFactor", default=100),
        "firmware":         printer.get("firmware_version"),
    }


def _broker_params():
    """Split mqtt://host:port into (hostname, port, username, password)."""
    u = urllib.parse.urlparse(config.get("mqtt.url"))
    return (
        u.hostname,
        u.port or 1883,
        config.get("mqtt.username") or None,
        config.get("mqtt.password") or None,
    )


async def _run():
    global _running
    hostname, port, username, password = _broker_params()
    delay = 1.0
    while _running:
        try:
            will = aiomqtt.Will(topic=BRIDGE_AVAILABILITY, payload=b"offline", retain=True)
            async with aiomqtt.Client(
                hostname=hostname, port=port, identifier=_client_id,
                username=username, password=password, will=will,
            ) as client:
                delay = 1.0
                await _announce(client)
                # Status fan-out hook arrives with its own slice; idle-wait here.
                while _running:
                    await asyncio.sleep(3600)
        except aiomqtt.MqttError:
            if _running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)


def _printer_dict(pr):
    """Fan-out works on to_dict() — the exact shape browsers see."""
    return pr.to_dict() if hasattr(pr, "to_dict") else pr


async def _announce(client) -> None:
    """On connect: availability + per-printer discovery and retained state."""
    await client.publish(BRIDGE_AVAILABILITY, "online", retain=True)
    for pid, pr in list(state.printers.items()):
        d = _printer_dict(pr)
        for topic, payload in _discovery_messages(pid, d):
            await client.publish(topic, payload, retain=True)
        await client.publish(_state_topic(pid), json.dumps(bridge_state(d)), retain=True)


async def ensure_started():
    global _task, _running
    if aiomqtt is None:
        return
    if not features.is_enabled("mqtt_bridge") or not config.get("mqtt.url"):
        return
    if _task and not _task.done():
        return
    _running = True
    _task = asyncio.create_task(_run())


async def stop_and_clear():
    global _running, _task
    _running = False
    if _task:
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
