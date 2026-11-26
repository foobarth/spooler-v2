"""MQTT bridge to Home Assistant (read-only v1) — see docs/adr-mqtt-bridge.md.

One shared client publishes printer state to the user's own broker with HA
discovery. The bridge never opens a second connection to any printer: it
fans out from the status cache (state.printers), which the existing printer
connections already populate.

Lifecycle (v2, single owner): one task owns the connection and every
lifecycle decision. Callers never start or stop anything — `sync_lifecycle()`
just pokes the owner, which re-evaluates desired state (feature on AND broker
URL set → that broker) and reconciles: connect, move (clear old, announce
new), or tear down and empty every retained payload ("off means off" —
entities vanish, not go stale). A poke from any context wakes it; interior
naps end early on a wake but never consume it.
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
    # HA's discovery grammar is homeassistant/<component>/[<node_id>/]<object_id>/config
    # — node_id = the printer, object_id = the entity. One level more (an extra
    # namespace) and HA silently ignores the payload; that cost us the whole
    # device until the probe experiment exposed it.
    return f"homeassistant/{component}/{pid}/{key}/config"


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


async def _camera_tick(client, wanted) -> None:
    """One snapshot round for every printing/paused printer with a camera,
    roughly every CAMERA_INTERVAL_S (ADR: ~10 s while printing, nothing
    when idle/offline). Frames are not retained — a stale frame must not
    outlive the stream. A lifecycle change ends the round at printer
    granularity; the in-flight grab (executor, ~5 s) is uncancellable."""
    loop = asyncio.get_running_loop()
    for pid, pr in list(state.printers.items()):
        if _desired_params() != wanted:
            return
        d = _printer_dict(pr)
        if d.get("state") not in ("printing", "paused") or not d.get("camera_url"):
            continue
        jpeg = await loop.run_in_executor(None, grab_jpeg, d["camera_url"])
        if jpeg:
            await client.publish(_camera_topic(pid), jpeg)


def _printer_dict(pr):
    """Fan-out works on to_dict() — the exact shape browsers see."""
    return pr.to_dict() if hasattr(pr, "to_dict") else pr


# ── lifecycle: one serialized owner ──────────────────────────────────────────
# Exactly one task connects, reconnects, announces, ticks the camera, tears
# down or clears. Requests from outside are pokes, not commands: `Event.set`
# (thread-safe via call_soon_threadsafe from foreign contexts), and the owner
# re-reads committed config when it acts. A wake is consumed by the owner —
# at the top of its loop, or by a nap that ends early — and every consume is
# immediately followed by a fresh desired-state read, so a consumed poke is
# always acted on and none are lost.

_owner_task: asyncio.Task | None = None
_owner_loop: asyncio.AbstractEventLoop | None = None
_wake: asyncio.Event | None = None

_CLEAR_RETRY_S = 60.0        # parked: keep retrying a failed off-clear this often
ACK_TIMEOUT_S = 10.0        # how long turning OFF waits to report "done"

# broker the outstanding retained payloads were published to — the clear
# targets THIS, never whatever the config says now (the URL may have been
# changed or cleared in the same breath). Set at every connect.
_ledger_broker: tuple | None = None


def _ledger_outstanding() -> bool:
    return bool(_discovered or _published or _published_yaml or _announced_online)


def _desired_params():
    """The broker to hold while the bridge should run — feature on AND broker
    URL set — or None when it should not. aiomqtt absent: never runs."""
    if aiomqtt is None:
        return None
    if not features.is_enabled("mqtt_bridge") or not config.get("mqtt.url"):
        return None
    return _broker_params()


def _ensure_owner() -> None:
    """(Re-)spawn the ONE owner task on the current loop when absent, done or
    stranded on a closed loop. A live owner is kept: re-wiring channels must
    not restart the connection."""
    global _owner_task, _owner_loop, _wake
    running = asyncio.get_running_loop()
    if _owner_task is not None and not _owner_task.done() and _owner_loop is running:
        return
    _wake = asyncio.Event()
    _owner_loop = running
    _owner_task = asyncio.create_task(_owner())


def _poke() -> None:
    """Wake the owner so it re-evaluates desired state. Never a command: the
    owner reads the live config when it acts. A poke that arrives while a
    dead owner is being replaced is inherently honored — the replacement's
    first cycle reconciles exactly then."""
    _ensure_owner()
    loop, wake = _owner_loop, _wake
    if loop is not None and not loop.is_closed():
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            wake.set()
        else:
            loop.call_soon_threadsafe(wake.set)


async def sync_lifecycle() -> None:
    """The one lifecycle entry point — what channels' hooks call (via their
    scheduler) and what tests await: tell the owner to re-evaluate desired
    state. Idempotent, any context. Turning OFF is acked by the real
    condition: this returns once the retained-payload ledger is empty (every
    published topic was emptied on the broker), or reports loudly that the
    clear is still being retried in the background. Turning ON returns
    immediately — the connection appearing on the broker is the progress
    signal."""
    if aiomqtt is None:
        return
    _poke()
    if _desired_params() is not None:
        return
    deadline = time.monotonic() + ACK_TIMEOUT_S
    while _ledger_outstanding():
        if time.monotonic() > deadline:
            print("[Bridge] lifecycle: off-clear still in progress "
                  "(broker slow or down?) — the owner keeps retrying in the background")
            return
        await asyncio.sleep(0.02)


async def _owner():
    """Single serialized actor: the only task that touches the connection or
    the lifecycle state. Runs until cancelled; its own errors are contained."""
    while True:
        _wake.clear()
        # A poke counted before this clear has set the event (or queues its
        # set after it — a spurious wake at worst): the fresh desired-state
        # reads below act on it either way. Lost wakes are impossible.
        try:
            await _reconcile()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Bridge] lifecycle error ({type(e).__name__}: {e}) — retrying")
        if _desired_params() is None and _ledger_outstanding():
            # parked with retained payloads outstanding (the clear failed —
            # broker down?): keep finishing "off" instead of leaving stale
            # entities forever. A poke ends the nap early.
            await _sleep_or_wake(_CLEAR_RETRY_S)
        else:
            await _wake.wait()


async def _sleep_or_wake(timeout: float) -> None:
    """Sleep that ends early when the owner is woken — consuming the wake it
    observed. Every nap is immediately followed by a fresh desired-state
    read, so a consumed poke is always acted on; leaving the event set here
    instead would make the nap return instantly forever (an await-free hot
    loop that starves the whole event loop)."""
    try:
        await asyncio.wait_for(_wake.wait(), timeout)
    except TimeoutError:
        return
    _wake.clear()


async def _reconcile() -> None:
    """Bring reality to desire — the only place that connects or clears.
    Returns when reality matches desire, so a spurious wake costs one pass."""
    while True:
        wanted = _desired_params()
        if wanted is None:
            if _ledger_outstanding():
                await _clear_records()      # off means off (retried on failure)
            return
        if _ledger_outstanding() and _ledger_broker != wanted:
            await _clear_records()          # broker moved: empty the OLD one first
        await _hold(wanted)                 # returns when desire changed or the connection died


async def _hold(wanted) -> None:
    """Connect and serve while desire still says `wanted`; return when desire
    changed or the connection ended. Reconnects with backoff while the broker
    is down; the backoff and camera naps end early on a wake."""
    global _client, _ledger_broker
    delay = 1.0
    while _desired_params() == wanted:
        try:
            will = aiomqtt.Will(topic=BRIDGE_AVAILABILITY, payload=b"offline", retain=True)
            async with aiomqtt.Client(
                hostname=wanted[0], port=wanted[1], identifier=_client_id,
                username=wanted[2], password=wanted[3], will=will,
            ) as client:
                _client = client
                _ledger_broker = wanted
                delay = 1.0
                n = sum(1 for p in state.printers.values())
                print(f"[Bridge] Connected to {wanted[0]}:{wanted[1]} — {n} printer(s) bridged")
                if _desired_params() != wanted:
                    break
                await _announce(client)
                while _desired_params() == wanted:
                    await _camera_tick(client, wanted)
                    await _sleep_or_wake(CAMERA_INTERVAL_S)
        except aiomqtt.MqttError as e:
            if _desired_params() != wanted:
                break
            print(f"[Bridge] Broker connection failed ({type(e).__name__}: {e}) — retrying in {delay:.0f} s")
            await _sleep_or_wake(delay)
            delay = min(delay * 2, 30)
        finally:
            _client = None
    # falling out of the async with on a desire change = clean DISCONNECT, no LWT


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
_discovered: dict = {}      # printer_id -> discovery topics announced (what the off-clear must empty)
_published_yaml: dict = {}  # printer_id -> mjpeg YAML currently retained (or absent = nothing out)
_announced_online = False   # the availability "online" payload is on the broker, clear it on stop
_client = None              # the live aiomqtt client, set while connected


async def _publish_discovery(pid: str, msgs) -> None:
    """Publish discovery, remembering each topic as it goes out — a stop that
    cancels the announce mid-way must still leave an accurate record for
    stop_and_clear to empty ("off means off")."""
    known = _discovered.setdefault(pid, [])
    for topic, payload in msgs:
        if topic not in known:
            known.append(topic)
        await _client.publish(topic, payload, retain=True)


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
    global _announced_online
    await client.publish(BRIDGE_AVAILABILITY, "online", retain=True)
    _announced_online = True
    for pid, pr in list(state.printers.items()):
        d = _printer_dict(pr)
        await _publish_discovery(pid, _discovery_messages(pid, d))
        await _publish_camera_yaml(pid, d)
        fresh = bridge_state(d)
        await client.publish(_state_topic(pid), json.dumps(fresh), retain=True)
        sent = _published.setdefault(pid, _Sent())
        sent.json, sent.ts = fresh, time.monotonic()


async def _clear_records():
    """Empty every retained payload we ever published — availability, state
    and discovery — against the broker the records were published to, so
    switching the feature off makes the HA entities vanish instead of going
    stale. Best effort: if the broker is unreachable the records survive and
    the owner retries (the parked retry, and every later reconcile)."""
    global _ledger_broker, _announced_online
    if aiomqtt is None or not _ledger_outstanding():
        return
    broker = _ledger_broker or _broker_params()
    if not broker or not broker[0]:
        print("[Bridge] No broker known to clear discovery on — keeping the records")
        return
    try:
        async with aiomqtt.Client(
            hostname=broker[0], port=broker[1], identifier=_client_id,
            username=broker[2], password=broker[3],
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
              f"({type(e).__name__}: {e}) — retrying")
        return
    _discovered.clear()
    _published.clear()
    _published_yaml.clear()
    _announced_online = False
    _ledger_broker = None
