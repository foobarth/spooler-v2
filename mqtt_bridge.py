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
    aiomqtt = None      # type: ignore[assignment]

import config
import features
import state
from notifiers import webhook_payload
from snapshot import grab_jpeg

NUMERIC_THROTTLE_S = 5.0    # numerics republish at most this often
PUBLISH_TIMEOUT_S = 10.0    # every publish is bounded: a silently-dead socket (no
                            # RST — writes never complete, nothing raises) must
                            # surface as an error, not hang the publisher forever
NOTIFY_ACK_TIMEOUT_S = 25.0  # notification handoff: up to two bounded publishes
CAMERA_INTERVAL_S = 10.0    # snapshot cadence while a print is running
IDLE_CAMERA_INTERVAL_S = 60.0   # snapshot cadence while connected but idle

NOTIFY_TOPIC = "spooler/notification"
NOTIFY_IMAGE_TOPIC = "spooler/notification/image"


class NotificationPublishError(Exception):
    """The MQTT notification channel's failure mode — the notifiers layer
    translates it into its own NotifierError, so send_test reports it the
    same way it reports every other channel's failure."""

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

# (key, component, friendly name, unit, device_class, state_class, nullable, entity_category, icon)
SENSORS = [
    ("state",            "sensor", "Status",             None, "enum",        None,          False, None,        "mdi:printer-3d"),
    ("nozzle_temp",      "sensor", "Nozzle temperature", "°C", "temperature", "measurement", False, None,        None),
    ("nozzle_target",    "sensor", "Nozzle target",      "°C", "temperature", "measurement", False, None,        None),
    ("bed_temp",         "sensor", "Bed temperature",    "°C", "temperature", "measurement", False, None,        None),
    ("bed_target",       "sensor", "Bed target",         "°C", "temperature", "measurement", False, None,        None),
    ("chamber_temp",     "sensor", "Chamber temperature", "°C", "temperature", "measurement", False, None,       None),
    ("chamber_target",   "sensor", "Chamber target",     "°C", "temperature", "measurement", False, None,        None),
    ("progress",         "sensor", "Progress",           "%",  None,          "measurement", True,  None,        "mdi:percent"),
    ("layer",            "sensor", "Layer",              None, None,          "measurement", False, None,        "mdi:layers"),
    ("total_layer",      "sensor", "Total layers",       None, None,          "measurement", False, None,        "mdi:layers"),
    ("time_remaining",   "sensor", "Time remaining",     "s",  "duration",    "measurement", False, None,        "mdi:timer"),
    ("time_elapsed",     "sensor", "Time elapsed",       "s",  "duration",    "measurement", False, None,        "mdi:timer-sand"),
    ("eta",              "sensor", "ETA",                None, "timestamp",   None,          True,  None,        None),
    ("filament_mm",      "sensor", "Filament used",      "mm", None,          "total_increasing", False, None,   "mdi:tape-measure"),
    ("filament_g",       "sensor", "Filament used",      "g",  "weight",      "total_increasing", False, None,    "mdi:weight"),
    ("speed_factor",     "sensor", "Speed factor",       "%",  None,          "measurement", False, None,        "mdi:speedometer"),
    ("firmware_version", "sensor", "Firmware",           None, None,          None,          True,  "diagnostic", "mdi:chip"),
] + [
    # Fan percents ride the same projection (CC1/CC2 put them into
    # status['FanSpeed']); announced only for printers whose capabilities
    # declare them (see FAN_KEYS below), so Prusa/Moonraker printers get none.
    (key, "sensor", name, "%", None, "measurement", False, None, icon)
    for key, name, icon in (("fan_part",    "Part fan",    "mdi:fan"),
                            ("fan_aux",     "Aux fan",     "mdi:fan"),
                            ("fan_chamber", "Chamber fan", "mdi:fan"))
]
_FAN_KEYS = {"fan_part", "fan_aux", "fan_chamber"}

# (key, friendly name, extra discovery fields) — device_class gives the icon
# and the on/off wording in HA; plain icons where no class fits.
BINARY_SENSORS = [
    ("connected", "Connected", {"device_class": "connectivity"}),
    ("printing",  "Printing",  {"icon": "mdi:printer-3d"}),
]

# Tray status 0/1/2, verified live 2026-11-24: dynamic state, not static
# config — the previously active tray moves 2→1 when the active tray changes.
_TRAY_STATUS = {0: "empty", 1: "loaded", 2: "active"}

# The Status sensor's enum: exactly the display states classify_display_state()
# can produce (printers/base.py is the source of truth; a test pins the two
# lists against each other). HA renders these as the sensor's possible values.
STATE_OPTIONS = ("offline", "idle", "preparing", "printing", "pausing", "paused",
                 "stopping", "complete", "cancelled", "error", "unknown")


def _state_topic(pid: str) -> str:
    return f"spooler/{pid}/state"


def _camera_topic(pid: str) -> str:
    return f"spooler/{pid}/camera/frame"


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
    model = TYPE_MODELS.get(t or "")
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
                    nullable=False, category=None, icon=None, binary=False,
                    **extra) -> dict:
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
    if icon:
        p["icon"] = icon
    if category:
        p["entity_category"] = category
    if binary:
        p["payload_on"] = "True"
        p["payload_off"] = "False"
    p.update(extra)
    return p


def _discovery_messages(pid: str, d: dict):
    """(topic, json payload) for every discovered entity of one printer."""
    caps = set(d.get("capabilities") or ())
    for key, component, name, unit, device_class, state_class, nullable, category, icon in SENSORS:
        if key in _FAN_KEYS and key not in caps:
            continue
        payload = _entity_payload(pid, d, component, key, name, unit, device_class,
                                  state_class, nullable, category, icon)
        if key == "state":
            payload["options"] = list(STATE_OPTIONS)
        yield _discovery_topic(component, pid, key), json.dumps(payload)
    for key, name, extra in BINARY_SENSORS:
        payload = _entity_payload(pid, d, "binary_sensor", key, name, binary=True, **extra)
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


def _superset_discovery_topics(pid: str):
    """Every discovery topic this code could ever have published for one
    printer — the registry superset, not what the ledger happens to know.
    The ledger records only what THIS instance announced; a previous run
    (different printer state, different code) may have left retained
    configs the current state never re-publishes — observed live
    2026-11-27: Canvas topics published while the printer was online
    survived an off-clear that ran with the printer offline. Canvas tray
    ids are 0-based protocol truth; the superset sweeps trays 0-7 so a
    retired tray's entities clear too."""
    for key, component, *_rest in SENSORS:
        yield _discovery_topic(component, pid, key)
    for key, _name, _extra in BINARY_SENSORS:
        yield _discovery_topic("binary_sensor", pid, key)
    yield _discovery_topic("camera", pid, "camera")
    yield _discovery_topic("sensor", pid, "canvas_active_tray")
    for tid in range(8):
        for suffix in ("status", "material", "brand", "color"):
            yield _discovery_topic("sensor", pid, f"canvas_tray{tid}_{suffix}")


def _notification_sensor_config() -> dict:
    """The ready-made HA wrapper for the notification topic: state = the
    event name (enum over every event the prose registry knows), the
    payload's facts as attributes. One sensor for the whole bridge — the
    printer rides inside the payload (printer_id). Exists only while the
    notify_mqtt feature feeds the topic."""
    import notify
    return {
        "name": "Last notification",
        "unique_id": "spooler-notification",
        "state_topic": NOTIFY_TOPIC,
        "value_template": "{{ value_json.event }}",
        "json_attributes_topic": NOTIFY_TOPIC,
        "json_attributes": ["printer", "title", "body", "priority", "has_image", "timestamp", "id"],
        "device_class": "enum",
        "options": sorted(set(notify.MESSAGES) | {"test"}),
        "availability_topic": BRIDGE_AVAILABILITY,
        "icon": "mdi:bell-ring",
        "device": {"identifiers": ["spooler-notifications"], "name": "Spooler Notifications"},
    }


def _canvas_entities(pid: str, d: dict, canvas: dict):
    """(topic, json payload) for one printer's Canvas entities: the active
    tray plus status/material/brand/color per tray the printer reports.
    Tray names are 1-based like the printer's screen; tray_id stays 0-based
    in the unique ids/keys (protocol truth)."""
    yield _discovery_topic("sensor", pid, "canvas_active_tray"), json.dumps(
        _entity_payload(pid, d, "sensor", "canvas_active_tray", "Active tray",
                        icon="mdi:tray"))
    icons = {"status": "mdi:tray", "material": "mdi:texture",
             "brand": "mdi:tag", "color": "mdi:palette"}
    for t in canvas.get("trays") or []:
        tid = t.get("id")
        if not isinstance(tid, int) or tid < 0:
            continue
        for suffix in ("status", "material", "brand", "color"):
            key = f"canvas_tray{tid}_{suffix}"
            payload = _entity_payload(pid, d, "sensor", key,
                                      f"Tray {tid + 1} {suffix}", icon=icons[suffix])
            yield _discovery_topic("sensor", pid, key), json.dumps(payload)


# ── state projection ─────────────────────────────────────────────────────────
# All protocol normalization (key fallback chains, progress rules) lives in
# printers/base.py to_dict() — the bridge owns none. This is just a whitelist
# of what the bridge publishes, plus one derived flag.

def _eta_iso(remaining) -> str | None:
    """Wall-clock finish time: now + remaining on the server's clock (the
    same anchor the browser's formatFinishAt uses), as ISO 8601 with the
    local UTC offset so any receiver — HA's timestamp device class included
    — interprets it unambiguously. None when nothing remains."""
    try:
        rem = float(remaining)
    except (TypeError, ValueError):
        return None
    if rem <= 0:
        return None
    from datetime import datetime, timedelta
    return (datetime.now().astimezone() + timedelta(seconds=rem)).isoformat()


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
        "fan_part":         printer.get("fan_part"),
        "fan_aux":          printer.get("fan_aux"),
        "fan_chamber":      printer.get("fan_chamber"),
        "progress":         printer.get("progress"),
        "layer":            printer.get("layer"),
        "total_layer":      printer.get("total_layer"),
        "time_remaining":   printer.get("time_remaining"),
        "time_elapsed":     printer.get("time_elapsed"),
        # ETA: the UI's formatFinishAt anchor (now + remaining) computed on
        # the server's clock, ISO 8601 with the local UTC offset — HA's
        # timestamp device class renders it correctly in any timezone.
        "eta":              _eta_iso(printer.get("time_remaining")),
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
        # tray_id is 0-based (canvas protocol); the printer's screen and this
        # bridge's own "Tray N" entities are 1-based — HA must agree with the
        # screen, so the active tray is shown +1. -1/unknown stays "none"
        # (never invent a tray 0 — §4b).
        out["canvas_active_tray"] = active + 1 if isinstance(active, int) and active >= 0 else "none"
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


def _ha_on() -> bool:
    """Home Assistant discovery is an optional layer on top of the agnostic
    MQTT core (docs/adr-mqtt-bridge.md §Agnostic core): off, only the generic
    topics flow — no homeassistant/ config is ever published."""
    return bool(config.get("mqtt.home_assistant"))


async def _camera_tick(client, wanted) -> None:
    """One snapshot round for every connected printer with a camera: ~10 s
    while printing/paused, ~IDLE_CAMERA_INTERVAL_S otherwise, nothing when
    offline. The idle feed is not a nicety: HA's MQTT camera holds no image
    until the first frame arrives (MqttCamera._last_image starts None), so
    a print-gated feed rendered the entity broken on an idle printer —
    observed in real HA 2026-11-26. Frames are retained — every join shows
    the latest picture instantly (feature toggle, broker restart, entity
    reload); the stream-over clear below empties the retained frame so a
    stale picture never outlives the stream. A lifecycle change ends the
    round at printer granularity; the in-flight grab (executor, ~5 s) is
    uncancellable."""
    loop = asyncio.get_running_loop()
    now = time.monotonic()
    for pid, pr in list(state.printers.items()):
        if _desired_params() != wanted:
            return
        d = _printer_dict(pr)
        gate = (bool(d.get("connected")), bool(d.get("camera_url")))
        if _camera_gate_logged.get(pid) != gate:
            _camera_gate_logged[pid] = gate
            print(f"[Bridge camera] {pid}: feeder gate connected={gate[0]} "
                  f"camera_url={gate[1]} state={d.get('state')}")
        if not gate[0] or not gate[1]:
            # The stream is over: the retained frame must not outlive it.
            if pid in _camera_frame_out:
                await client.publish(_camera_topic(pid), b"", retain=True, timeout=PUBLISH_TIMEOUT_S)
                _camera_frame_out.discard(pid)
                print(f"[Bridge camera] {pid}: frame cleared (stream over)")
            _camera_last_sent.pop(pid, None)
            continue
        printing = d.get("state") in ("printing", "paused")
        interval = CAMERA_INTERVAL_S if printing else IDLE_CAMERA_INTERVAL_S
        # "never grabbed yet" means DUE NOW: time.monotonic() counts from host
        # boot, and a freshly booted print server (or an ephemeral CI VM) can
        # be younger than the idle cadence — a 0.0 sentinel would leave the
        # camera dark until host uptime exceeds the interval.
        last = _camera_last_sent.get(pid)
        if last is not None and now - last < interval:
            continue
        # A failed grab consumes the slot too: a cold or broken camera is
        # retried at the cadence, not hammered every tick.
        _camera_last_sent[pid] = now
        jpeg = await loop.run_in_executor(None, grab_jpeg, d["camera_url"])
        if jpeg:
            await client.publish(_camera_topic(pid), jpeg, retain=True, timeout=PUBLISH_TIMEOUT_S)
            _camera_frame_out.add(pid)
        else:
            print(f"[Bridge camera] {pid}: grab returned nothing")


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

# parked: keep retrying a failed off-clear this often
_CLEAR_RETRY_S = 60.0        # parked: keep retrying a failed off-clear this often
ACK_TIMEOUT_S = 10.0        # how long turning OFF waits to report "done"

# the last off-clear's superset sweep failed (broker unreachable) with an empty
# ledger — the ledger cannot represent the sweep, so this flag carries the
# retry until a clear succeeds
_off_clear_failed = False

# broker the outstanding retained payloads were published to — the clear
# targets THIS, never whatever the config says now (the URL may have been
# changed or cleared in the same breath). Set at every connect.
_ledger_broker: tuple | None = None

def _ledger_outstanding() -> bool:
    return bool(_discovered or _published or _camera_frame_out or _announced_online)


def _desired_params():
    """The desired state while the integration should run — feature on AND
    broker URL set — or None when it should not. The HA-discovery flag and
    the MQTT-notification feature are part of the tuple, so flipping either
    while connected is a real transition (clean disconnect, clear,
    re-announce), never a silent ignore. aiomqtt absent: never runs."""
    if aiomqtt is None:
        return None
    if not features.is_enabled("mqtt_integration") or not config.get("mqtt.url"):
        return None
    return (*_broker_params(), _ha_on(), features.is_enabled("notify_mqtt"))


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
    if loop is not None and wake is not None and not loop.is_closed():
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
        if _desired_params() is None and (_ledger_outstanding() or _off_clear_failed):
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
    assert _wake is not None      # _ensure_owner sets it before spawning the owner
    try:
        await asyncio.wait_for(_wake.wait(), timeout)
    except TimeoutError:
        return
    _wake.clear()


async def _reconcile() -> None:
    """Bring reality to desire — the only place that connects or clears.
    Returns when reality matches desire, so a spurious wake costs one pass."""
    global _off_clear_failed
    while True:
        wanted = _desired_params()
        if wanted is None:
            if _ledger_outstanding() or _off_clear_failed or state.printers:
                await _clear_records()      # off means off (retried on failure)
            return
        if _ledger_broker is not None and _ledger_broker != wanted \
                and (_ledger_outstanding() or _off_clear_failed):
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
        except (aiomqtt.MqttError, TimeoutError) as e:
            if _desired_params() != wanted:
                break
            print(f"[Bridge] Broker connection failed ({type(e).__name__}: {e}) — retrying in {delay:.0f} s")
            await _sleep_or_wake(delay)
            delay = min(delay * 2, 30)
        finally:
            _client = None
    # falling out of the async with on a desire change = clean DISCONNECT, no LWT


# ── MQTT notification channel (publishes through the integration's connection) ──

def publish_notification(n) -> None:
    """Thread-safe entry for the MQTT notification channel, called from
    notify's worker thread. Publishes through the ONE connection the owner
    holds (never a second client) and blocks until the broker confirms —
    bounded — so a failure surfaces as NotificationPublishError and send_test
    can report it, exactly like every other channel reports its failures."""
    if aiomqtt is None or _desired_params() is None:
        raise NotificationPublishError("The MQTT integration is switched off (or no broker is set).")
    loop = _owner_loop
    if loop is None or loop.is_closed():
        raise NotificationPublishError("The MQTT integration's connection is not running.")
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is loop:
        raise NotificationPublishError("publish_notification must not run on the owner loop's thread.")
    try:
        fut = asyncio.run_coroutine_threadsafe(_publish_notification(n), loop)
        fut.result(timeout=NOTIFY_ACK_TIMEOUT_S)
    except NotificationPublishError:
        raise
    except Exception as e:
        raise NotificationPublishError(f"The broker rejected the notification ({type(e).__name__}).") from None


async def _publish_notification(n) -> None:
    if _client is None:
        raise NotificationPublishError("The broker connection is down — try again when it returns.")
    if n.image:
        # the picture goes first: a consumer reacting to the JSON finds it already there
        await _client.publish(NOTIFY_IMAGE_TOPIC, n.image, timeout=PUBLISH_TIMEOUT_S)
    await _client.publish(NOTIFY_TOPIC, json.dumps(webhook_payload(n)), timeout=PUBLISH_TIMEOUT_S)


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
_camera_last_sent: dict = {}  # printer_id -> monotonic ts of the last snapshot round (idle cadence gate)
_camera_gate_logged: dict = {}  # printer_id -> last logged (connected, camera_url) gate state
_camera_frame_out: set = set()   # printer_ids with a retained frame on the broker — the off-clear must empty these
_discovered: dict = {}      # printer_id -> discovery topics announced (what the off-clear must empty)
_announced_online = False   # the availability "online" payload is on the broker, clear it on stop
_client: aiomqtt.Client | None = None   # the live aiomqtt client, set while connected


async def _publish_discovery(pid: str, msgs) -> None:
    """Publish discovery, remembering each topic as it goes out — a stop that
    cancels the announce mid-way must still leave an accurate record for
    stop_and_clear to empty ("off means off")."""
    assert _client is not None      # callers hold a live connection
    known = _discovered.setdefault(pid, [])
    for topic, payload in msgs:
        if topic not in known:
            known.append(topic)
        await _client.publish(topic, payload, retain=True, timeout=PUBLISH_TIMEOUT_S)


async def _sync_discovery(pid: str, d: dict) -> bool:
    """Announce discovery topics the printer deserves but that were never
    published — late-arriving canvas (the 30 s poll), a camera appearing.
    True when something went out. The HA-discovery toggle gates this entirely
    (the agnostic core keeps flowing regardless)."""
    if not _ha_on():
        return False
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
    sent = _published.setdefault(pid, _Sent())
    changed = {k for k in fresh if fresh[k] != sent.json.get(k)}
    immediate = any(k in _IMMEDIATE for k in changed)
    # A printer gained discoverable extras since the last announce (canvas
    # trays from the 30 s poll, a camera that appeared): announce what is
    # missing and publish the state right away — the new entities should
    # not sit behind the numeric throttle reading "unknown".
    try:
        if await _sync_discovery(pid, d):
            await _publish_state(pid, fresh)
            if immediate:
                _schedule_frame_now(pid, d)     # the event frame rides every path
            return
        if sent.json is None:
            await _publish_state(pid, fresh)        # first sight after (re)connect
            return
        if changed and (immediate
                        or time.monotonic() - sent.ts >= NUMERIC_THROTTLE_S):
            await _publish_state(pid, fresh)
            if immediate:
                _schedule_frame_now(pid, d)
    except (aiomqtt.MqttError, TimeoutError) as e:
        # a dead socket times out here instead of hanging forever; the owner's
        # own publish timeout cycles the connection, the next event retries
        print(f"[Bridge] Publish failed ({type(e).__name__}: {e}) — dropping this update")


_frame_tasks: set = set()   # strong refs: a GC'd task is a silently dropped frame


def _schedule_frame_now(pid: str, d: dict) -> None:
    """Fire-and-forget the grab: on_status runs in the bus dispatcher, and an
    executor grab (~5 s worst case) must not delay the other subscribers."""
    if _client is None or not d.get("connected") or not d.get("camera_url"):
        return
    t = asyncio.create_task(_push_frame_now(pid, d))
    _frame_tasks.add(t)
    t.add_done_callback(_frame_tasks.discard)


async def _push_frame_now(pid: str, d: dict) -> None:
    jpeg = await asyncio.get_running_loop().run_in_executor(
        None, grab_jpeg, d["camera_url"])
    if jpeg and _client is not None:
        try:
            await _client.publish(_camera_topic(pid), jpeg, retain=True,
                                  timeout=PUBLISH_TIMEOUT_S)
            _camera_frame_out.add(pid)
            print(f"[Bridge camera] {pid}: published {len(jpeg)} B (state event)")
        except (aiomqtt.MqttError, TimeoutError) as e:
            # runs as a bus task: a dead socket must not kill it — the owner's
            # own publish timeout cycles the connection, the next event retries
            print(f"[Bridge camera] {pid}: state-event frame failed ({type(e).__name__}: {e})")
    _camera_last_sent[pid] = time.monotonic()   # the cadence restarts from this frame


async def _publish_state(pid: str, fresh: dict) -> None:
    assert _client is not None      # callers checked or hold a live connection
    await _client.publish(_state_topic(pid), json.dumps(fresh), retain=True,
                          timeout=PUBLISH_TIMEOUT_S)
    sent = _published.setdefault(pid, _Sent())
    sent.json = fresh
    sent.ts = time.monotonic()


async def _announce(client) -> None:
    """On connect: availability + per-printer discovery and retained state."""
    global _announced_online
    await client.publish(BRIDGE_AVAILABILITY, "online", retain=True, timeout=PUBLISH_TIMEOUT_S)
    _announced_online = True
    if _ha_on():
        for pid, pr in list(state.printers.items()):
            d = _printer_dict(pr)
            await _publish_discovery(pid, _discovery_messages(pid, d))
        if features.is_enabled("notify_mqtt"):
            await _publish_discovery("spooler", [
                (_discovery_topic("sensor", "spooler", "notification"),
                 json.dumps(_notification_sensor_config()))])
    for pid, pr in list(state.printers.items()):
        d = _printer_dict(pr)
        fresh = bridge_state(d)
        await client.publish(_state_topic(pid), json.dumps(fresh), retain=True)
        sent = _published.setdefault(pid, _Sent())
        sent.json, sent.ts = fresh, time.monotonic()


async def _clear_records():
    """Empty every retained payload we ever published — availability, state,
    discovery and camera frames — against the broker the records were
    published to, so switching the feature off makes the HA entities vanish
    instead of going stale. The discovery sweep is the registry SUPERSET for
    every printer the instance knows (state, ledger), so topics a previous
    instance published and this one never re-announced are emptied too.
    Best effort: if the broker is unreachable the records survive and the
    owner retries (the parked retry, and every later reconcile)."""
    global _ledger_broker, _announced_online, _off_clear_failed
    if aiomqtt is None:
        return
    _off_clear_failed = False
    broker = _ledger_broker or _broker_params()
    if not broker or not broker[0]:
        print("[Bridge] No broker known to clear discovery on — keeping the records")
        return
    try:
        async with aiomqtt.Client(
            hostname=broker[0], port=broker[1], identifier=_client_id,
            username=broker[2], password=broker[3],
        ) as client:
            # QoS-1: publish() then awaits the broker's PUBACK, so a completed
            # sweep is broker-confirmed — the ledger-empty ack below is exact.
            # QoS-0 empties handed to aiomqtt's writer task made the ack
            # aspirational (a grace-sleep guess that raced under load — the
            # discovery-toggle flake).
            await client.publish(BRIDGE_AVAILABILITY, b"", qos=1, retain=True, timeout=PUBLISH_TIMEOUT_S)
            # the notification sensor's config: its node ("spooler") is not a
            # printer pid, so the superset sweep below never touches it
            await client.publish(_discovery_topic("sensor", "spooler", "notification"),
                                 b"", qos=1, retain=True, timeout=PUBLISH_TIMEOUT_S)
            # the registry superset for every printer we know of — the ledger's
            # exact topics are a subset, so this also sweeps what a previous
            # instance published and this one never re-announced
            for pid in set(state.printers) | set(_discovered) | set(_published):
                for topic in _superset_discovery_topics(pid):
                    await client.publish(topic, b"", qos=1, retain=True, timeout=PUBLISH_TIMEOUT_S)
                await client.publish(_state_topic(pid), b"", qos=1, retain=True, timeout=PUBLISH_TIMEOUT_S)
                await client.publish(_camera_topic(pid), b"", qos=1, retain=True, timeout=PUBLISH_TIMEOUT_S)
    except aiomqtt.MqttError as e:
        print(f"[Bridge] Broker unreachable, retained discovery not cleared yet "
              f"({type(e).__name__}: {e}) — retrying")
        _off_clear_failed = True
        return
    _discovered.clear()
    _camera_last_sent.clear()
    _camera_gate_logged.clear()
    _camera_frame_out.clear()
    _published.clear()
    _announced_online = False
    _ledger_broker = None
