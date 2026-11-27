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
    monkeypatch.setattr(mqtt_bridge, "_camera_last_sent", {})
    monkeypatch.setattr(mqtt_bridge, "_camera_gate_logged", {})
    mqtt_bridge._camera_frame_out.clear()   # the real object: a setattr would mask type bugs
    monkeypatch.setattr(mqtt_bridge, "_announced_online", False)
    monkeypatch.setattr(mqtt_bridge, "_ledger_broker", None)
    monkeypatch.setattr(mqtt_bridge, "_client", None)
    monkeypatch.setattr(mqtt_bridge, "_off_clear_failed", False)
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


async def test_state_transition_pushes_a_fresh_camera_frame(broker, monkeypatch):
    # A state event is exactly when the picture changes (print start, pause,
    # finish): the frame must arrive with the transition, not one cadence behind.
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 60.0)      # cadence silent
    monkeypatch.setattr(mb, "IDLE_CAMERA_INTERVAL_S", 60.0)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    pr = _register_printer(monkeypatch)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    # The first tick's frame — wait for it to arrive, however slow the runner.
    await asyncio.wait_for(_first_camera_frame(broker), 10.0)
    n = len(broker.payloads("spooler/p1/camera/frame"))
    assert n >= 1

    pr.status["PrintInfo"]["Status"] = 2                   # idle → printing
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await _wait_for_publish_after(broker, "spooler/p1/camera/frame", n, timeout=10.0)
    assert len(broker.payloads("spooler/p1/camera/frame")) == n + 1


async def _first_camera_frame(broker):
    while not broker.payloads("spooler/p1/camera/frame"):
        await asyncio.sleep(0.01)


async def test_state_transition_with_discovery_republish_still_frames(broker, monkeypatch):
    # The event frame must not depend on which publish path the transition
    # takes: when _sync_discovery republishes (announce gap), the early
    # return used to skip the frame entirely.
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 60.0)
    monkeypatch.setattr(mb, "IDLE_CAMERA_INTERVAL_S", 60.0)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    monkeypatch.setattr(mb, "_sync_discovery", lambda pid, d: _true())
    pr = _register_printer(monkeypatch)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    await asyncio.wait_for(_first_camera_frame(broker), 10.0)   # the first tick's frame
    n = len(broker.payloads("spooler/p1/camera/frame"))

    pr.status["PrintInfo"]["Status"] = 2                   # idle → printing
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await _wait_for_publish_after(broker, "spooler/p1/camera/frame", n, timeout=10.0)
    assert len(broker.payloads("spooler/p1/camera/frame")) == n + 1   # via the event path


async def _true():
    return True


async def test_numeric_update_pushes_no_camera_frame(broker, monkeypatch):
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 60.0)
    monkeypatch.setattr(mb, "IDLE_CAMERA_INTERVAL_S", 60.0)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    pr = _register_printer(monkeypatch, TempOfNozzle=210)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    pr.status["PrintInfo"]["Status"] = 2                   # printing
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    await asyncio.wait_for(_first_camera_frame(broker), 10.0)
    n = len(broker.payloads("spooler/p1/camera/frame"))
    assert n >= 1

    pr.status["TempOfNozzle"] = 215                        # numeric-only change
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await asyncio.sleep(0.1)
    assert len(broker.payloads("spooler/p1/camera/frame")) == n, "no transition, no frame"


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
    # tray_id 2 is "Tray 3" (0-based protocol, 1-based screen/entities): the
    # sensor shows what the printer's screen shows.
    assert out["canvas_active_tray"] == 3
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
    assert last["canvas_active_tray"] == 3         # tray_id 2 = "Tray 3" (1-based, like the screen)


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
    features.set_enabled("mqtt_integration", False)
    await _wait_for_payload(broker, "spooler/bridge/availability", b"")
    features.set_enabled("mqtt_integration", True)
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


async def test_off_clear_empties_topics_a_previous_instance_published(broker, monkeypatch):
    """The ledger only knows what THIS instance published. A previous run
    (Elli online with Canvas, different code, different state) may have left
    discovery topics on the broker that this one never announces itself —
    observed live 2026-11-27: flipping HA discovery off cleared everything
    the running instance knew about and left every canvas topic retained.
    Off-means-off must empty the registry superset for every printer the
    instance knows, not just the ledger's topics."""
    import aiomqtt

    # a previous instance's leftovers: canvas configs, retained on the broker.
    # NOTE: the printer must be registered BEFORE the first await — conftest
    # wires the real channels, so the fixture's config_set schedules a
    # lifecycle sync that runs at this test's first await and announces
    # whatever state.printers holds at that moment.
    _register_printer(monkeypatch)      # Elli offline, NO canvas in its state
    async with aiomqtt.Client(hostname="127.0.0.1", port=broker.port,
                              identifier="previous-instance") as seed:
        for key in ("canvas_tray0_status", "canvas_tray3_color", "canvas_active_tray"):
            await seed.publish(f"homeassistant/sensor/p1/{key}/config",
                               b'{"name": "stale"}', retain=True, timeout=5.0)
        await asyncio.sleep(0.2)    # let the fake broker record the QoS-0 writes

    await mqtt_bridge.sync_lifecycle()  # connects, announces (no canvas topics)
    await broker.wait_for_publish("spooler/p1/state")

    config_set("mqtt.url", "")
    await mqtt_bridge.sync_lifecycle()  # off: the superset must sweep the stale topics

    for key in ("canvas_tray0_status", "canvas_tray3_color", "canvas_active_tray"):
        topic = f"homeassistant/sensor/p1/{key}/config"
        assert broker.payloads(topic)[-1] == b"", f"{topic} (stale from a previous run) survived the off-clear"


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
        features.set_enabled("mqtt_integration", False)
        await mqtt_bridge.sync_lifecycle()
        await _wait_for_payload(broker, "spooler/p1/state", b"")
        await _wait_for_payload(broker, "spooler/bridge/availability", b"")
    finally:
        features.set_enabled("mqtt_integration", True)
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
    features.set_enabled("mqtt_integration", False)
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


# ── agnostic core: the HA-discovery toggle gates only the discovery topics ───

async def test_discovery_toggle_off_still_publishes_the_agnostic_core(broker, monkeypatch):
    """With "Publish Home Assistant discovery" off, no homeassistant/ topics
    may exist — but the generic feeds (availability, state, camera frames) are
    the integration's core and keep flowing for any MQTT consumer."""
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    config_set("mqtt.home_assistant", False)
    pr = _register_printer(monkeypatch, canvas_info=CANVAS_INFO)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    try:
        await mqtt_bridge.sync_lifecycle()
        await broker.wait_for_publish("spooler/p1/state")
        await asyncio.wait_for(_first_camera_frame(broker), 10.0)

        assert [p for p in broker.topics("homeassistant")] == [], \
            "the HA toggle must gate every homeassistant/ topic"
        assert broker.payloads("spooler/bridge/availability"), "availability still flows"
        assert broker.payloads("spooler/p1/state"), "state still flows"
        assert broker.payloads("spooler/p1/camera/frame"), "camera frames still flow"
    finally:
        config_set("mqtt.home_assistant", True)


async def test_flipping_the_discovery_toggle_is_a_real_transition(broker, monkeypatch):
    """Off→on of "Publish Home Assistant discovery" while connected rides the
    lifecycle machinery: discovery configs are cleared (off-means-off — HA
    entities vanish rather than linger unmanaged), the generic core keeps
    flowing, and flipping back on re-announces everything."""
    _register_printer(monkeypatch, canvas_info=CANVAS_INFO)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/state")
    assert broker.topics("homeassistant")

    config_set("mqtt.home_assistant", False)
    await mqtt_bridge.sync_lifecycle()
    await _wait_for_payload(broker, "homeassistant/sensor/p1/nozzle_temp/config", b"")
    ha = {p.topic for p in broker.publishes if p.topic.startswith("homeassistant/")}
    for topic in ha:
        assert broker.payloads(topic)[-1] == b"", f"{topic} was not cleared"
    assert broker.payloads("spooler/p1/state"), "the agnostic core keeps flowing"

    config_set("mqtt.home_assistant", True)
    await mqtt_bridge.sync_lifecycle()
    cfg = await broker.wait_for_publish("homeassistant/sensor/p1/nozzle_temp/config")
    assert cfg.payload != b"" and cfg.retain


async def test_camera_frames_flow_while_printing(broker, monkeypatch):
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg-" + url.encode())
    _printing_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()

    frame = await broker.wait_for_publish("spooler/p1/camera/frame")
    assert frame.payload == b"jpeg-http://192.0.2.5:8080/mjpeg"
    assert frame.retain            # any subscriber join (feature toggle, HA restart) gets the frame


async def test_offline_printer_clears_its_camera_frame(broker, monkeypatch):
    # A retained frame's lifetime is exactly the stream's: when the printer
    # drops, the frame topic is emptied — HA's tile goes dark like the card,
    # never showing a zombie view.
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    pr = _register_printer(monkeypatch)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/camera/frame")

    pr.connected = False
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())
    await _wait_for_payload(broker, "spooler/p1/camera/frame", b"")


async def test_off_clear_empties_the_camera_frame_topic(broker, monkeypatch):
    import mqtt_bridge as mb
    import features
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    pr = _register_printer(monkeypatch)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_publish("spooler/p1/camera/frame")

    features.set_enabled("mqtt_integration", False)
    try:
        await mqtt_bridge.sync_lifecycle()
        await _wait_for_payload(broker, "spooler/p1/camera/frame", b"")
    finally:
        features.set_enabled("mqtt_integration", True)


async def test_camera_feeds_slowly_while_idle(broker, monkeypatch):
    # HA's MQTT camera holds no image until the first frame arrives — a
    # print-gated feed left the entity broken on an idle printer (observed
    # in real HA 2026-11-26). Idle printers feed at the slow idle cadence.
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "IDLE_CAMERA_INTERVAL_S", 0.3)
    grabs = []
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: grabs.append(url) or b"jpeg")
    _register_printer(monkeypatch)          # connected, but idle — not printing
    state.printers["p1"].connected = True
    state.printers["p1"].camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()

    await broker.wait_for_publish("spooler/p1/camera/frame")   # first frame promptly
    await asyncio.sleep(0.15)
    assert len(grabs) == 1, "inside the idle window the camera rests"
    await asyncio.sleep(0.3)
    assert len(grabs) == 2, "past the idle window the camera feeds again"


async def test_camera_feeds_nothing_while_offline(broker, monkeypatch):
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "IDLE_CAMERA_INTERVAL_S", 0.05)
    grabs = []
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: grabs.append(url) or b"jpeg")
    _register_printer(monkeypatch)          # not connected
    state.printers["p1"].camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await asyncio.sleep(0.15)
    assert not grabs, "an offline printer gets no frames"



async def test_a_dead_socket_self_heals(broker, monkeypatch):
    # A silently-dead broker connection (no RST — writes never complete, no
    # error surfaces) must not hang the bridge forever: bounded publish
    # timeouts kick the owner into its reconnect cycle, and after the
    # reconnect the flow resumes on the fresh connection.
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "IDLE_CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "PUBLISH_TIMEOUT_S", 0.3, raising=False)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    pr = _register_printer(monkeypatch)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await asyncio.wait_for(_first_camera_frame(broker), 10.0)
    n = len(broker.payloads("spooler/p1/camera/frame"))

    dead_client = mb._client
    import paho.mqtt.client as paho_mqtt
    real_paho_publish = paho_mqtt.Client.publish

    import itertools
    stuck_mids = itertools.count(99)

    class StuckInfo:                    # the packet is queued, never flushed
        rc = 0
        def __init__(self):
            self.mid = next(stuck_mids)
        def is_published(self):
            return False

    def hang_paho_publish(self, *a, **kw):
        if self is dead_client._client:  # the dead socket: below aiomqtt's timeout layer
            return StuckInfo()
        return real_paho_publish(self, *a, **kw)

    monkeypatch.setattr(paho_mqtt.Client, "publish", hang_paho_publish)
    await _wait_for_publish_after(broker, "spooler/p1/camera/frame", n, timeout=15.0)


# ── MQTT notification channel: publish through the integration's connection ──

async def test_publish_notification_sends_the_webhook_schema_json(broker, monkeypatch):
    """Same schema_version-1 field set as the webhook channel — one receiver
    contract. Non-retained: a notification is a transient event, not state."""
    from notify import Notification
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_connect()

    n = Notification("print_complete", "p1", "Bench — Print complete",
                     "benchy.gcode is done.", printer_name="Bench", priority="low")
    await asyncio.to_thread(mqtt_bridge.publish_notification, n)
    msg = await broker.wait_for_publish("spooler/notification")
    assert msg.retain is False
    body = json.loads(msg.payload)
    assert body["schema_version"] == 1
    assert body["event"] == "print_complete" and body["printer"] == "Bench"
    assert body["title"] == "Bench — Print complete" and body["body"] == "benchy.gcode is done."
    assert body["priority"] == "low" and body["has_image"] is False
    assert body["id"] and body["timestamp"]


async def test_notification_image_is_published_before_the_json(broker, monkeypatch):
    """Same-client publish order: when a consumer reacts to the JSON, the
    picture is already on its topic."""
    from notify import Notification
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_connect()

    n = Notification("print_complete", "p1", "T", "B", image=b"jpeg-bytes")
    await asyncio.to_thread(mqtt_bridge.publish_notification, n)
    await broker.wait_for_publish("spooler/notification")
    pubs = [p for p in broker.publishes
            if p.topic in ("spooler/notification", "spooler/notification/image")]
    order = [p.topic for p in pubs]
    assert order.index("spooler/notification/image") < order.index("spooler/notification")
    img = [p for p in pubs if p.topic == "spooler/notification/image"][-1]
    assert img.payload == b"jpeg-bytes" and img.retain is False


async def test_publish_notification_fails_plainly_when_the_connection_is_down(broker, monkeypatch):
    """The channel must never fire-and-forget: a dead broker surfaces as a
    readable error the same way every other channel reports failures."""
    from notify import Notification
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_connect()
    await broker.stop()                    # broker gone: the owner is in backoff

    n = Notification("print_complete", "p1", "T", "B")
    with pytest.raises(mqtt_bridge.NotificationPublishError):
        await asyncio.to_thread(mqtt_bridge.publish_notification, n)


async def test_the_notification_channel_reaches_the_broker_through_notifiers(broker, monkeypatch):
    """Assembly: the real channel wiring (notifiers.CHANNELS -> _mqtt_send ->
    the bridge's connection), not a direct call to the internal helper."""
    import notifiers
    from notify import Notification
    _register_printer(monkeypatch)
    await mqtt_bridge.sync_lifecycle()
    await broker.wait_for_connect()

    n = Notification("test", "p1", "Spooler — test notification", "If you can read this, this channel works.")
    await asyncio.to_thread(notifiers.send, "mqtt", n)
    msg = await broker.wait_for_publish("spooler/notification")
    assert json.loads(msg.payload)["event"] == "test"


async def test_the_notification_channel_reports_failure_when_the_integration_is_off(broker, monkeypatch):
    import notifiers
    from notify import Notification
    config_set("mqtt.url", "")             # integration off: desired None
    with pytest.raises(notifiers.NotifierError):
        notifiers.send("mqtt", Notification("test", "p1", "T", "B"))


def test_discovery_config_is_ha_quality():
    """Units, device classes, state classes and icons: HA should render every
    entity as a first-class citizen — duration switchable in more-info,
    weight with proper class, themed icons."""
    from printers.base import PrinterConnection
    pr = PrinterConnection("p1", "192.0.2.5", "Workshop")
    pr.connected = True
    pr.attrs = {"FirmwareVersion": "1.2.3"}
    pr.status = {"PrintInfo": {"CurrentLayer": 12, "TotalLayer": 100, "RemainTime": 300}}
    d = pr.to_dict()
    payloads = {t.split("/")[1] + "/" + t.split("/")[3]:
                json.loads(p) for t, p in mqtt_bridge._discovery_messages("p1", d)}
    tr = payloads["sensor/time_remaining"]
    assert tr["device_class"] == "duration" and tr["unit_of_measurement"] == "s"
    fg = payloads["sensor/filament_g"]
    assert fg["device_class"] == "weight" and fg["state_class"] == "total_increasing"
    fm = payloads["sensor/filament_mm"]
    assert fm["state_class"] == "total_increasing" and fm["icon"] == "mdi:tape-measure"
    assert payloads["sensor/time_remaining"]["icon"] == "mdi:timer"
    assert payloads["sensor/speed_factor"]["icon"] == "mdi:speedometer"
    assert payloads["sensor/firmware_version"]["icon"] == "mdi:chip"
    assert payloads["binary_sensor/connected"]["device_class"] == "connectivity"
    assert payloads["binary_sensor/printing"]["icon"] == "mdi:printer-3d"


async def test_camera_first_grab_is_due_on_a_fresh_boot(broker, monkeypatch):
    # time.monotonic() counts from HOST boot: a freshly booted print server
    # can be younger than the idle cadence, and the first grab must still be
    # due (CI runners are ephemeral VMs — this flaked exactly there).
    import mqtt_bridge as mb
    import types
    monkeypatch.setattr(mb, "time", types.SimpleNamespace(monotonic=lambda: 30.0))
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 60.0)
    monkeypatch.setattr(mb, "IDLE_CAMERA_INTERVAL_S", 60.0)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg")
    pr = _register_printer(monkeypatch)
    pr.connected = True
    pr.camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.sync_lifecycle()
    await asyncio.wait_for(_first_camera_frame(broker), 10.0)
