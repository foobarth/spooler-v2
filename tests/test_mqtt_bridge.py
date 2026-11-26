"""mqtt_bridge.py — Home Assistant MQTT bridge (read-only v1).

Tests run against tests/fake_mqtt.py, an in-process broker. The bridge is
never tested through its internals: tests read the broker's recorded
connect/publish log. See docs/adr-mqtt-bridge.md.
"""


import asyncio
import json

import pytest

import fake_mqtt
import mqtt_bridge
import state
from config import set as config_set
from printers.base import PrinterConnection


@pytest.fixture
async def broker(monkeypatch):
    b = fake_mqtt.FakeBroker()
    await b.start()
    config_set("mqtt.url", f"mqtt://127.0.0.1:{b.port}")
    monkeypatch.setattr(mqtt_bridge, "_published", {})
    monkeypatch.setattr(mqtt_bridge, "_discovered", {})
    monkeypatch.setattr(mqtt_bridge, "_announced_online", False)
    monkeypatch.setattr(mqtt_bridge, "_ledger_broker", None)
    monkeypatch.setattr(mqtt_bridge, "_client", None)
    yield b
    config_set("mqtt.url", "")                  # off means off, also for the owner
    await mqtt_bridge.sync_lifecycle()
    t = mqtt_bridge._owner_task
    if t is not None:
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
    await b.stop()


async def _wait_for_publish_after(broker, topic, n, timeout=2.0):
    """Wait until the broker has seen MORE than `n` publishes of `topic`."""
    async def _poll():
        while len(broker.payloads(topic)) <= n:
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), timeout)


async def test_sync_lifecycle_connects_to_broker(broker):
    await mqtt_bridge.sync_lifecycle()
    conn = await broker.wait_for_connect()
    assert conn.client_id.startswith("spooler")


# ── bridge_state: thin projection of to_dict() ──────────────────────────────

PROJECTION_KEYS = {"state", "connected", "printing", "nozzle_temp", "nozzle_target",
                   "bed_temp", "bed_target", "chamber_temp", "chamber_target",
                   "progress", "layer", "total_layer", "time_remaining",
                   "filament_mm", "filament_g", "speed_factor", "firmware_version"}


# ── discovery on connect ─────────────────────────────────────────────────────

def test_discovery_topics_match_has_grammar():
    """HA silently ignores configs published outside its discovery-topic
    grammar (homeassistant/<component>/[<node_id>/]<object_id>/config —
    5 or 6 segments total). This was a real bug: an extra namespace level
    made every real config invalid while the tests asserted our own strings."""
    import mqtt_bridge as mb
    for component in ("sensor", "binary_sensor", "camera"):
        topic = mb._discovery_topic(component, "7ff68cf82d3140e49dac6c423c0eb2b5", "nozzle_temp")
        parts = topic.split("/")
        assert parts[0] == "homeassistant" and parts[1] == component
        assert parts[2] == "7ff68cf82d3140e49dac6c423c0eb2b5"   # node_id = the printer
        assert parts[3] == "nozzle_temp" and parts[4] == "config"
        assert len(parts) == 5, f"{topic}: HA accepts 5 segments, got {len(parts)}"

def _register_printer(monkeypatch, **status):
    import state
    from printers.base import PrinterConnection
    pr = PrinterConnection("p1", "192.0.2.5", "Workshop")
    pr.attrs = {"FirmwareVersion": "1.2.3"}
    pr.status = {"PrintInfo": {"CurrentLayer": 12, "TotalLayer": 100, "RemainTime": 300}, **status}
    monkeypatch.setattr(state, "printers", {"p1": pr})
    return pr


async def test_connect_publishes_retained_discovery_with_device_info(broker, monkeypatch):
    _register_printer(monkeypatch, TempOfNozzle=210, TempTargetNozzle=215, TempOfHotbed=60)
    await mqtt_bridge.sync_lifecycle()

    cfg = await broker.wait_for_publish("homeassistant/sensor/p1/nozzle_temp/config")
    assert cfg.retain
    payload = json.loads(cfg.payload)
    assert payload["name"] == "Nozzle temperature"
    assert payload["state_topic"] == "spooler/p1/state"
    assert "value_json.nozzle_temp" in payload["value_template"]
    assert payload["unique_id"] == "spooler-p1-nozzle_temp"
    assert payload["unit_of_measurement"] == "°C"
    assert payload["device_class"] == "temperature"
    assert payload["device"]["identifiers"] == ["p1"]
    assert payload["device"]["name"] == "Spooler Workshop"
    assert payload["device"]["sw_version"] == "1.2.3"
    assert payload["availability_topic"] == "spooler/bridge/availability"


async def test_connect_announces_the_sensor_set_per_printer(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")   # last thing _announce publishes

    topics = {p.topic for p in broker.topics("homeassistant")}
    for key in ("state", "nozzle_temp", "nozzle_target", "bed_temp", "bed_target",
                "chamber_temp", "progress", "layer", "total_layer", "time_remaining",
                "filament_mm", "filament_g", "speed_factor", "firmware_version"):
        assert f"homeassistant/sensor/p1/{key}/config" in topics, key
    for key in ("connected", "printing"):
        assert f"homeassistant/binary_sensor/p1/{key}/config" in topics, key


async def test_state_topic_publishes_the_canonical_projection(broker, monkeypatch):
    pr = _register_printer(monkeypatch, TempOfNozzle=210, TempTargetNozzle=215, TempOfHotbed=60)
    pr.connected = True   # offline printers zero their temps (history, not a reading)
    await mqtt_bridge.sync_lifecycle()

    msg = await broker.wait_for_publish("spooler/p1/state")
    assert msg.retain
    s = json.loads(msg.payload)
    assert set(s) == PROJECTION_KEYS
    assert s["nozzle_temp"] == 210
    assert s["progress"] == 12          # from layers 12/100, computed by to_dict()
    assert s["printing"] is False       # printer is not printing → "idle"
    assert s["firmware_version"] == "1.2.3"


async def test_state_topic_zeroes_temps_for_an_offline_printer(broker, monkeypatch):
    # An offline printer's last readings are history, not a measurement —
    # the projection must not hand HA a stale 210 °C.
    _register_printer(monkeypatch, TempOfNozzle=210, TempTargetNozzle=215, TempOfHotbed=60)
    await mqtt_bridge.sync_lifecycle()

    msg = await broker.wait_for_publish("spooler/p1/state")
    s = json.loads(msg.payload)
    assert (s["nozzle_temp"], s["nozzle_target"], s["bed_temp"]) == (0, 0, 0)


# ── status_update fan-out: transitions immediate, numerics throttled ────────

async def test_status_update_republishes_on_a_state_transition(broker, monkeypatch):
    pr = _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    first = await broker.wait_for_publish("spooler/p1/state")
    assert json.loads(first.payload)["state"] == "offline"    # announce's initial state

    pr.connected = True                                        # → "idle": a transition
    n = len(broker.payloads("spooler/p1/state"))
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await _wait_for_publish_after(broker, "spooler/p1/state", n)
    assert json.loads(broker.payloads("spooler/p1/state")[-1])["state"] == "idle"


async def test_numeric_changes_are_throttled_to_the_window(broker, monkeypatch):
    monkeypatch.setattr(mqtt_bridge, "NUMERIC_THROTTLE_S", 0.2)
    pr = _register_printer(monkeypatch, TempOfNozzle=210)
    pr.connected = True
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")          # announce
    n = len(broker.payloads("spooler/p1/state"))

    pr.status = {"PrintInfo": {"CurrentLayer": 12, "TotalLayer": 100, "RemainTime": 300},
                 "TempOfNozzle": 215}                          # numeric-only change
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await asyncio.sleep(0.05)
    assert len(broker.payloads("spooler/p1/state")) == n       # suppressed inside the window

    await asyncio.sleep(0.25)                                  # past the window: the next
    pr.status = {"PrintInfo": {"CurrentLayer": 12, "TotalLayer": 100, "RemainTime": 300},
                 "TempOfNozzle": 218}                          # update flushes the latest
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await _wait_for_publish_after(broker, "spooler/p1/state", n)
    assert json.loads(broker.payloads("spooler/p1/state")[-1])["nozzle_temp"] == 218


async def test_a_transition_is_never_throttled_even_inside_the_window(broker, monkeypatch):
    monkeypatch.setattr(mqtt_bridge, "NUMERIC_THROTTLE_S", 60)
    pr = _register_printer(monkeypatch)
    pr.connected = True
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    n = len(broker.payloads("spooler/p1/state"))

    pr.status = {"PrintInfo": {"Status": 2, "Filename": "a.gcode",
                               "CurrentLayer": 1, "TotalLayer": 100}}   # → printing
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await _wait_for_publish_after(broker, "spooler/p1/state", n)
    assert json.loads(broker.payloads("spooler/p1/state")[-1])["printing"] is True


# ── Canvas: canonical to_dict["canvas"] projected + discovered ──────────────
# The sensors read only the canonical keys (docs/adr-mqtt-bridge.md); raw
# -1/0/1/2 semantics are rendered here, at the presentation layer.

CANVAS_INFO = {
    "active_canvas_id": 0,
    "active_tray_id": 2,
    "auto_refill": False,
    "canvas_list": [{
        "canvas_id": 0, "connected": 1,
        "tray_list": [
            {"tray_id": 0, "status": 1, "brand": "ELEGOO", "filament_code": "0x0001",
             "filament_color": "#FFFFFF", "filament_name": "PLA+", "filament_type": "PLA",
             "min_nozzle_temp": 190, "max_nozzle_temp": 230},
            {"tray_id": 2, "status": 2, "brand": "ELEGOO", "filament_code": "0x0006",
             "filament_color": "#FFFFFF", "filament_name": "PLA Matte", "filament_type": "PLA",
             "min_nozzle_temp": 190, "max_nozzle_temp": 230},
            {"tray_id": 3, "status": 0, "brand": "ELEGOO", "filament_code": "0x0100",
             "filament_color": "#BCBCBC", "filament_name": "PETG", "filament_type": "PETG",
             "min_nozzle_temp": 230, "max_nozzle_temp": 260},
        ],
    }],
}


def test_bridge_state_projects_canvas_canonical_keys():
    pr = PrinterConnection("p1", "192.0.2.5", "Workshop")
    pr.status = {"canvas_info": CANVAS_INFO}
    out = mqtt_bridge.bridge_state(pr.to_dict())
    assert out["canvas_active_tray"] == 2          # numbers stay numbers
    assert out["canvas_tray2_status"] == "active"  # 0/1/2 rendered once, here
    assert out["canvas_tray2_material"] == "PLA"
    assert out["canvas_tray2_brand"] == "ELEGOO"
    assert out["canvas_tray2_color"] == "#FFFFFF"
    assert out["canvas_tray3_status"] == "empty"
    assert out["canvas_tray0_status"] == "loaded"


def test_bridge_state_renders_negative_active_tray_as_none():
    ci = {**CANVAS_INFO, "active_tray_id": -1}
    pr = PrinterConnection("p1", "192.0.2.5", "Workshop")
    pr.status = {"canvas_info": ci}
    out = mqtt_bridge.bridge_state(pr.to_dict())
    assert out["canvas_active_tray"] == "none"     # never invent a tray 0


def test_bridge_state_without_canvas_has_no_canvas_keys():
    pr = PrinterConnection("p1", "192.0.2.5", "Workshop")
    pr.status = {}
    out = mqtt_bridge.bridge_state(pr.to_dict())
    assert not [k for k in out if k.startswith("canvas_")]


async def test_connect_discovers_canvas_entities(broker, monkeypatch):
    _register_printer(monkeypatch, canvas_info=CANVAS_INFO)
    await mqtt_bridge.sync_lifecycle()

    cfg = await broker.wait_for_publish("homeassistant/sensor/p1/canvas_tray2_status/config")
    assert cfg.retain
    payload = json.loads(cfg.payload)
    assert payload["name"] == "Tray 3 status"      # tray_id + 1, like the browser
    assert "value_json.canvas_tray2_status" in payload["value_template"]
    assert payload["unique_id"] == "spooler-p1-canvas_tray2_status"

    topics = {p.topic for p in broker.topics("homeassistant")}
    for key in ("canvas_active_tray", "canvas_tray0_status", "canvas_tray0_material",
                "canvas_tray0_brand", "canvas_tray0_color",
                "canvas_tray2_status", "canvas_tray2_material",
                "canvas_tray3_status", "canvas_tray3_color"):
        assert f"homeassistant/sensor/p1/{key}/config" in topics, key


async def test_no_canvas_no_canvas_discovery(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    assert not [p for p in broker.topics("homeassistant") if "canvas" in p.topic]


async def test_canvas_arriving_later_still_gets_discovered(broker, monkeypatch):
    pr = _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    assert not [p for p in broker.topics("homeassistant") if "canvas" in p.topic]

    pr.status["canvas_info"] = CANVAS_INFO          # the 30 s poll lands
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())

    cfg = await broker.wait_for_publish("homeassistant/sensor/p1/canvas_tray2_status/config")
    payload = json.loads(cfg.payload)
    assert payload["name"] == "Tray 3 status"
    # and the fresh state (with the canvas keys) is published right away, not
    # left behind the numeric throttle for the new entities to read as unknown
    async def _state_has_canvas():
        while "canvas_active_tray" not in json.loads(broker.payloads("spooler/p1/state")[-1]):
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_state_has_canvas(), 2.0)
    last = json.loads(broker.payloads("spooler/p1/state")[-1])
    assert last["canvas_active_tray"] == 2


# ── lifecycle: the bridge must start through the REAL startup path ──────────
# Not via tests driving the bridge directly (which is why a missing production
# wiring passed 25 tests): channels.wire() is what production runs.

async def test_the_bridge_starts_through_the_real_startup_path(broker, monkeypatch):
    import channels
    import features
    config_set("mqtt.url", "")                      # no broker configured yet
    channels.wire()                                 # what server startup runs
    _register_printer(monkeypatch)
    await asyncio.sleep(0)                          # the startup sync task
    assert broker.connects == []                    # nothing to connect to

    config_set("mqtt.url", f"mqtt://127.0.0.1:{broker.port}")   # config change
    conn = await broker.wait_for_connect()
    assert conn.client_id.startswith("spooler")

    # and the feature flag is a live switch, not a restart requirement:
    # off means off (everything cleared), back on reconnects
    features.set_enabled("mqtt_bridge", False)
    await _wait_for_payload(broker, "spooler/bridge/availability", b"")
    features.set_enabled("mqtt_bridge", True)
    async def _reconnected():
        while len(broker.connects) < 2:
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_reconnected(), 2.0)


async def _wait_for_payload(broker, topic, payload):
    async def _poll():
        while payload not in broker.payloads(topic):
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), 2.0)


# ── availability LWT: the bridge dying must not leave a stale "online" ───────

async def test_lwt_declared_and_offline_fires_when_the_connection_dies(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    conn = await broker.wait_for_connect()
    assert conn.will_topic == "spooler/bridge/availability"
    assert conn.will_payload == b"offline"

    online = await broker.wait_for_publish("spooler/bridge/availability")
    assert online.payload == b"online" and online.retain

    await broker.drop_client()          # bridge dies without a DISCONNECT
    async def _offline_fires():
        while b"offline" not in broker.payloads("spooler/bridge/availability"):
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_offline_fires(), 2.0)
    offline = [p for p in broker.publishes
               if p.topic == "spooler/bridge/availability" and p.payload == b"offline"][-1]
    assert offline.retain


# ── turning off: "off means off" — entities vanish, not go stale ────────────

async def test_turning_off_empties_every_retained_topic(broker, monkeypatch):
    _register_printer(monkeypatch, canvas_info=CANVAS_INFO)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    announced = sorted({p.topic for p in broker.publishes if p.retain})

    config_set("mqtt.url", "")
    await mqtt_bridge.sync_lifecycle()

    cleared = {p.topic: p for p in broker.publishes if p.payload == b""}
    assert "spooler/bridge/availability" in cleared
    for topic in announced:
        last = cleared.get(topic)
        assert last is not None, f"{topic} was never cleared"
        assert last.retain, f"{topic} was cleared non-retained"


async def test_turning_off_without_a_prior_run_is_a_no_op(broker):
    n = len(broker.publishes)
    config_set("mqtt.url", "")
    await mqtt_bridge.sync_lifecycle()
    assert len(broker.publishes) == n


# ── lifecycle v2 (consolidation): latent holes, red on the current pile ──────
# All three drive the production entry point (sync_lifecycle, what channels'
# hooks call) and assert the broker log — they survive the redesign unchanged.

async def _rebind_broker(b):
    """Stop the fake broker and listen again on the SAME port, so a URL that
    named it keeps naming it (FakeBroker.start picks a fresh port each time)."""
    await b.stop()
    b._server = await asyncio.start_server(b._handle, "127.0.0.1", b.port)


async def test_changing_the_broker_url_reconnects_and_empties_the_old_broker(broker, monkeypatch):
    """Today the running bridge ignores a broker-URL change entirely (the run
    task keeps the hostname it started with). The bridge must treat the URL
    as part of desired state: clear the old broker, announce on the new."""
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_connect()
    await broker.wait_for_publish("spooler/p1/state")

    b2 = fake_mqtt.FakeBroker()
    await b2.start()
    try:
        config_set("mqtt.url", f"mqtt://127.0.0.1:{b2.port}")
        await mqtt_bridge.sync_lifecycle()
        await b2.wait_for_connect()
        await b2.wait_for_publish("spooler/p1/state")
        # the old broker's retained payloads are emptied, not left stale
        await _wait_for_payload(broker, "spooler/p1/state", b"")
        await _wait_for_payload(broker, "spooler/bridge/availability", b"")
    finally:
        config_set("mqtt.url", "")
        await mqtt_bridge.sync_lifecycle()
        await b2.stop()


async def test_turning_off_after_a_url_change_empties_the_broker_the_payloads_live_on(broker, monkeypatch):
    """URL moved to a second broker and the flag turned off in the same
    moment: the clear must target the broker the payloads were published to
    (the old one), never the freshly configured address."""
    import features
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")

    b2 = fake_mqtt.FakeBroker()
    await b2.start()
    try:
        config_set("mqtt.url", f"mqtt://127.0.0.1:{b2.port}")
        features.set_enabled("mqtt_bridge", False)
        await mqtt_bridge.sync_lifecycle()
        await _wait_for_payload(broker, "spooler/p1/state", b"")
        await _wait_for_payload(broker, "spooler/bridge/availability", b"")
    finally:
        features.set_enabled("mqtt_bridge", True)
        await mqtt_bridge.sync_lifecycle()
        await b2.stop()


async def test_off_while_the_broker_is_down_clears_when_the_broker_returns(broker, monkeypatch):
    """'Off while the broker is down' keeps the records and must retry the
    clear by itself — without another poke or another stop."""
    monkeypatch.setattr(mqtt_bridge, "_CLEAR_RETRY_S", 0.2, raising=False)
    monkeypatch.setattr(mqtt_bridge, "ACK_TIMEOUT_S", 0.5, raising=False)
    import features
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")

    await broker.stop()                     # broker unreachable at "off" time
    config_set("mqtt.url", "")
    features.set_enabled("mqtt_bridge", False)
    await mqtt_bridge.sync_lifecycle()
    await _rebind_broker(broker)            # back up — nobody pokes again
    await _wait_for_payload(broker, "spooler/p1/state", b"")
    await _wait_for_payload(broker, "spooler/bridge/availability", b"")


async def test_poking_without_a_desire_change_keeps_the_loop_responsive(broker, monkeypatch):
    """A poke that changes nothing (the hooks fire schedule() on every
    unrelated feature/config write too) must not starve the event loop: this
    exact shape — nap ends early on an already-set wake without yielding and
    the wake was never consumed — spun the owner hot at 100 % CPU inside the
    serve loop, freezing every other task and timer with it."""
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_connect()
    for _ in range(5):
        await mqtt_bridge.sync_lifecycle()      # desire unchanged: pure pokes
    await asyncio.wait_for(asyncio.sleep(0.5), 3.0)   # the loop must breathe


# ── camera: discovered MQTT camera fed by snapshots while printing ───────────

def _printing_printer(monkeypatch, **status):
    pr = _register_printer(monkeypatch, **status)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    pr.status["PrintInfo"]["Status"] = 2
    return pr


async def test_connect_discovers_the_camera_when_the_printer_has_one(broker, monkeypatch):
    _register_printer(monkeypatch)
    pr = state.printers["p1"]
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()

    cfg = await broker.wait_for_publish("homeassistant/camera/p1/camera/config")
    assert cfg.retain
    payload = json.loads(cfg.payload)
    assert payload["topic"] == "spooler/p1/camera/frame"
    assert payload["unique_id"] == "spooler-p1-camera"
    assert payload["availability_topic"] == "spooler/bridge/availability"


async def test_no_camera_url_no_camera_discovery(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    assert not [p for p in broker.topics("homeassistant") if p.topic.startswith("homeassistant/camera/")]


async def test_camera_frames_flow_while_printing(broker, monkeypatch):
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg-" + url.encode())
    _printing_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()

    frame = await broker.wait_for_publish("spooler/p1/camera/frame")
    assert frame.payload == b"jpeg-http://192.0.2.5:8080/mjpeg"
    assert not frame.retain             # a stale frame must not outlive the stream


async def test_camera_feeder_rests_while_not_printing(broker, monkeypatch):
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    grabs = []
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: grabs.append(url) or b"jpeg")
    _register_printer(monkeypatch)          # connected, but idle — not printing
    state.printers["p1"].connected = True
    state.printers["p1"].camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await asyncio.sleep(0.15)
    assert not grabs, "an idle printer gets no frames"

