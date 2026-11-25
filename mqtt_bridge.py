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
import time
import urllib.parse

try:
    import aiomqtt
except ImportError:     # bridge stays off when the optional dep is absent
    aiomqtt = None

import config
import features
import state
from snapshot import grab_jpeg

NUMERIC_THROTTLE_S = 5.0    # numerics republish at most this often
CAMERA_INTERVAL_S = 10.0    # snapshot cadence while a print is running

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
    ("filament_mm",      "sensor", "Filament used",      "mm", None,          "measurement", False, None),
    ("filament_g",       "sensor", "Filament used",      "g",  None,          "measurement", False, None),
    ("speed_factor",     "sensor", "Speed factor",       "%",  None,          "measurement", False, None),
    ("firmware_version", "sensor", "Firmware",           None, None,          None,          True,  "diagnostic"),
]

BINARY_SENSORS = [("connected", "Connected"), ("printing", "Printing")]

# Tray status 0/1/2, verified live 2026-11-24: dynamic state, not static
# config — the previously active tray moves 2→1 when the active tray changes.
_TRAY_STATUS = {0: "empty", 1: "loaded", 2: "active"}


def _state_topic(pid: str) -> str:
    return f"spooler/{pid}/state"


def _camera_topic(pid: str) -> str:
    return f"spooler/{pid}/camera/frame"


def _yaml_topic(pid: str) -> str:
    return f"spooler/{pid}/camera/mjpeg_yaml"


def _camera_yaml(pid: str, name: str) -> str | None:
    """Paste-ready Home Assistant YAML for the realtime MJPEG camera — only
    when the user told us the address HA can reach and a token exists; the
    bridge never invents a hostname."""
    base = (config.get("camera.external_url") or "").rstrip("/")
    token = config.get("camera.external_token")
    if not base or not token:
        return None
    return (
        f"# Home Assistant: realtime camera of the Spooler printer {name}\n"
        "camera:\n"
        "  - platform: mjpeg\n"
        f"    name: \"{name}\"\n"
        f"    still_image_url: {base}/api/snapshot/{pid}?token={token}\n"
        f"    mjpeg_url: {base}/api/camera/{pid}?token={token}\n"
    )


async def _publish_camera_yaml(pid: str, d: dict) -> None:
    """Keep `spooler/<pid>/camera/mjpeg_yaml` in step with the config: publish
    when it changes, publish an empty retained payload when it goes away."""
    yaml = _camera_yaml(pid, d.get("name") or pid) if d.get("camera_url") else None
    if yaml == _published_yaml.get(pid):
        return
    if yaml is None and pid not in _published_yaml:
        return
    await _client.publish(_yaml_topic(pid), (yaml or "").encode(), retain=True)
    if yaml is None:
        _published_yaml.pop(pid, None)
        return
    _published_yaml[pid] = yaml
    known = _discovered.setdefault(pid, [])
    if _yaml_topic(pid) not in known:
        known.append(_yaml_topic(pid))


def _discovery_topic(component: str, pid: str, key: str) -> str:
    return f"homeassistant/{component}/spooler/{pid}/{key}/config"


def _template(key: str, nullable: bool) -> str:
    if nullable:
        return f"{{{{ value_json.{key} if value_json.{key} is not none else 'unknown' }}}}"
    return f"{{{{ value_json.{key} }}}}"


def _device(pid: str, d: dict) -> dict:
    t = d.get("printer_type")
    # "Spooler <printer name>" so the device is findable in HA by both names —
    # a device named only like the printer is invisible to a "spooler" search.
    dev = {"identifiers": [pid], "name": f"Spooler {d.get('name') or pid}"}
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
    canvas = d.get("canvas")
    if isinstance(canvas, dict) and canvas.get("trays"):
        yield from _canvas_entities(pid, d, canvas)
    if d.get("camera_url"):
        yield _discovery_topic("camera", pid, "camera"), json.dumps({
            "name": "Camera",
            "unique_id": f"spooler-{pid}-camera",
            "topic": _camera_topic(pid),
            "availability_topic": BRIDGE_AVAILABILITY,
            "device": _device(pid, d),
        })


def _canvas_entities(pid: str, d: dict, canvas: dict):
    """(topic, json payload) for one printer's Canvas entities: the active
    tray plus status/material/brand/color per tray the printer reports."""
    yield _discovery_topic("sensor", pid, "canvas_active_tray"), json.dumps(
        _entity_payload(pid, d, "sensor", "canvas_active_tray", "Active tray"))
    for t in canvas.get("trays") or []:
        tid = t.get("id")
        if not isinstance(tid, int) or tid < 0:
            continue
        for suffix in ("status", "material", "brand", "color"):
            key = f"canvas_tray{tid}_{suffix}"
            payload = _entity_payload(pid, d, "sensor", key, f"Tray {tid + 1} {suffix}")
            yield _discovery_topic("sensor", pid, key), json.dumps(payload)


# ── state projection ─────────────────────────────────────────────────────────
# All protocol normalization (key fallback chains, progress rules) lives in
# printers/base.py to_dict() — the bridge owns none. This is just a whitelist
# of what the bridge publishes, plus one derived flag.

def bridge_state(printer: dict) -> dict:
    """Thin projection of a printer's to_dict() for the state topic."""
    out = {
        "state":            printer.get("state"),
        "connected":        bool(printer.get("connected")),
        "printing":         printer.get("state") == "printing",
        "nozzle_temp":      printer.get("nozzle_temp"),
        "nozzle_target":    printer.get("nozzle_target"),
        "bed_temp":         printer.get("bed_temp"),
        "bed_target":       printer.get("bed_target"),
        "chamber_temp":     printer.get("chamber_temp"),
        "chamber_target":   printer.get("chamber_target"),
        "progress":         printer.get("progress"),
        "layer":            printer.get("layer"),
        "total_layer":      printer.get("total_layer"),
        "time_remaining":   printer.get("time_remaining"),
        "filament_mm":      printer.get("filament_mm"),
        "filament_g":       printer.get("filament_g"),
        "speed_factor":     printer.get("speed_factor"),
        "firmware_version": printer.get("firmware_version"),
    }
    # Canvas: only the canonical keys are read (the raw canvas_info blob is
    # never seen here). The -1/0/1/2 semantics verified live (§4b) are
    # rendered exactly once, in this presentation layer.
    canvas = printer.get("canvas")
    if isinstance(canvas, dict) and canvas.get("trays"):
        active = canvas.get("active_tray")
        out["canvas_active_tray"] = active if isinstance(active, int) and active >= 0 else "none"
        for t in canvas["trays"]:
            tid = t.get("id")
            if not isinstance(tid, int) or tid < 0:
                continue
            out[f"canvas_tray{tid}_status"]   = _TRAY_STATUS.get(t.get("status"), "unknown")
            out[f"canvas_tray{tid}_material"] = t.get("material")
            out[f"canvas_tray{tid}_brand"]    = t.get("brand")
            out[f"canvas_tray{tid}_color"]    = t.get("color")
    return out


def _broker_params():
    """Split mqtt://host:port into (hostname, port, username, password)."""
    u = urllib.parse.urlparse(config.get("mqtt.url"))
    return (
        u.hostname,
        u.port or 1883,
        config.get("mqtt.username") or None,
        config.get("mqtt.password") or None,
    )


async def _camera_tick(client) -> None:
    """One snapshot round for every printing/paused printer with a camera,
    roughly every CAMERA_INTERVAL_S (ADR: ~10 s while printing, nothing
    when idle/offline). Frames are not retained — a stale frame must not
    outlive the stream."""
    loop = asyncio.get_running_loop()
    for pid, pr in list(state.printers.items()):
        d = _printer_dict(pr)
        if d.get("state") not in ("printing", "paused") or not d.get("camera_url"):
            continue
        jpeg = await loop.run_in_executor(None, grab_jpeg, d["camera_url"])
        if jpeg:
            await client.publish(_camera_topic(pid), jpeg)


async def _run():
    global _running, _client
    hostname, port, username, password = _broker_params()
    delay = 1.0
    while _running:
        try:
            will = aiomqtt.Will(topic=BRIDGE_AVAILABILITY, payload=b"offline", retain=True)
            async with aiomqtt.Client(
                hostname=hostname, port=port, identifier=_client_id,
                username=username, password=password, will=will,
            ) as client:
                _client = client
                try:
                    delay = 1.0
                    await _announce(client)
                    while _running:
                        await _camera_tick(client)
                        await asyncio.sleep(CAMERA_INTERVAL_S)
                finally:
                    _client = None
        except aiomqtt.MqttError:
            if _running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)


def _printer_dict(pr):
    """Fan-out works on to_dict() — the exact shape browsers see."""
    return pr.to_dict() if hasattr(pr, "to_dict") else pr


# ── status_update subscriber ─────────────────────────────────────────────────
# Transitions (state/connected/printing) go out immediately; every other
# change is a numeric refresh and waits for NUMERIC_THROTTLE_S. A suppressed
# update is simply not sent — the next status_update recomputes from the
# live printer, so the freshest values win and nothing queues up.

_IMMEDIATE = ("state", "connected", "printing")


class _Sent:
    __slots__ = ("json", "ts")
    def __init__(self):
        self.json = None
        self.ts = 0.0


_published: dict = {}       # printer_id -> _Sent (what the topic holds right now)
_discovered: dict = {}      # printer_id -> discovery topics announced (what stop_and_clear must empty)
_published_yaml: dict = {}  # printer_id -> mjpeg YAML currently retained (or absent = nothing out)
_client = None              # the live aiomqtt client, set while connected


async def _publish_discovery(pid: str, msgs) -> None:
    """Publish discovery, remembering the topics so stop_and_clear can
    empty every retained payload later ("off means off")."""
    topics = []
    for topic, payload in msgs:
        topics.append(topic)
        await _client.publish(topic, payload, retain=True)
    if topics:
        known = _discovered.setdefault(pid, [])
        for t in topics:
            if t not in known:
                known.append(t)


async def _sync_discovery(pid: str, d: dict) -> bool:
    """Announce discovery topics the printer deserves but that were never
    published — late-arriving canvas (the 30 s poll), a camera appearing.
    True when something went out."""
    known = set(_discovered.get(pid, []))
    msgs = [(t, p) for t, p in _discovery_messages(pid, d) if t not in known]
    if not msgs:
        return False
    await _publish_discovery(pid, msgs)
    return True


async def on_status(payload: dict) -> None:
    pid = payload.get("printer_id")
    d = payload.get("printer") or {}
    if not pid or not d or _client is None:
        return
    fresh = bridge_state(d)
    await _publish_camera_yaml(pid, d)
    # A printer gained discoverable extras since the last announce (canvas
    # trays from the 30 s poll, a camera that appeared): announce what is
    # missing and publish the state right away — the new entities should
    # not sit behind the numeric throttle reading "unknown".
    if await _sync_discovery(pid, d):
        await _publish_state(pid, fresh)
        return
    sent = _published.setdefault(pid, _Sent())
    if sent.json is None:
        await _publish_state(pid, fresh)        # first sight after (re)connect
        return
    changed = {k for k in fresh if fresh[k] != sent.json.get(k)}
    if changed and (any(k in _IMMEDIATE for k in changed)
                    or time.monotonic() - sent.ts >= NUMERIC_THROTTLE_S):
        await _publish_state(pid, fresh)


async def _publish_state(pid: str, fresh: dict) -> None:
    await _client.publish(_state_topic(pid), json.dumps(fresh), retain=True)
    sent = _published.setdefault(pid, _Sent())
    sent.json = fresh
    sent.ts = time.monotonic()


async def _announce(client) -> None:
    """On connect: availability + per-printer discovery and retained state."""
    await client.publish(BRIDGE_AVAILABILITY, "online", retain=True)
    for pid, pr in list(state.printers.items()):
        d = _printer_dict(pr)
        await _publish_discovery(pid, _discovery_messages(pid, d))
        await _publish_camera_yaml(pid, d)
        fresh = bridge_state(d)
        await client.publish(_state_topic(pid), json.dumps(fresh), retain=True)
        sent = _published.setdefault(pid, _Sent())
        sent.json, sent.ts = fresh, time.monotonic()


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
    await _clear_discovery()


async def _clear_discovery():
    """Empty every retained payload we ever published — availability, state
    and discovery — so switching the feature off makes the HA entities
    vanish instead of going stale. Best effort: if the broker is unreachable
    the records survive, and the next stop_and_clear retries the clear."""
    if aiomqtt is None or not (_discovered or _published):
        return
    hostname, port, username, password = _broker_params()
    try:
        async with aiomqtt.Client(
            hostname=hostname, port=port, identifier=_client_id,
            username=username, password=password,
        ) as client:
            await client.publish(BRIDGE_AVAILABILITY, b"", retain=True)
            for pid in set(_discovered) | set(_published):
                for topic in _discovered.get(pid, []):
                    await client.publish(topic, b"", retain=True)
                await client.publish(_state_topic(pid), b"", retain=True)
            # QoS-0 publishes are handed to aiomqtt's loop task; leaving the
            # context immediately would drop whatever it hasn't written yet.
            await asyncio.sleep(0.25)
    except aiomqtt.MqttError as e:
        print(f"[Bridge] Broker unreachable, retained discovery not cleared yet "
              f"({type(e).__name__}: {e}) — the next stop clears it")
        return
    _discovered.clear()
    _published.clear()
    _published_yaml.clear()
