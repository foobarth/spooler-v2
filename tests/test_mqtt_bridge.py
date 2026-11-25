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
    monkeypatch.setattr(mqtt_bridge, "_published_yaml", {})
    monkeypatch.setattr(mqtt_bridge, "_client", None)
    yield b
    await mqtt_bridge.stop_and_clear()
    await b.stop()


async def _wait_for_publish_after(broker, topic, n, timeout=2.0):
    """Wait until the broker has seen MORE than `n` publishes of `topic`."""
    async def _poll():
        while len(broker.payloads(topic)) <= n:
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), timeout)


async def test_ensure_started_connects_to_broker(broker):
    await mqtt_bridge.ensure_started()
    conn = await broker.wait_for_connect()
    assert conn.client_id.startswith("spooler")


# ── bridge_state: thin projection of to_dict() ──────────────────────────────

PROJECTION_KEYS = {"state", "connected", "printing", "nozzle_temp", "nozzle_target",
                   "bed_temp", "bed_target", "chamber_temp", "chamber_target",
                   "progress", "layer", "total_layer", "time_remaining",
                   "filament_mm", "filament_g", "speed_factor", "firmware_version"}


# ── discovery on connect ─────────────────────────────────────────────────────

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
    await mqtt_bridge.ensure_started()

    cfg = await broker.wait_for_publish("homeassistant/sensor/spooler/p1/nozzle_temp/config")
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
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/state")   # last thing _announce publishes

    topics = {p.topic for p in broker.topics("homeassistant")}
    for key in ("state", "nozzle_temp", "nozzle_target", "bed_temp", "bed_target",
                "chamber_temp", "progress", "layer", "total_layer", "time_remaining",
                "filament_mm", "filament_g", "speed_factor", "firmware_version"):
        assert f"homeassistant/sensor/spooler/p1/{key}/config" in topics, key
    for key in ("connected", "printing"):
        assert f"homeassistant/binary_sensor/spooler/p1/{key}/config" in topics, key


async def test_state_topic_publishes_the_canonical_projection(broker, monkeypatch):
    pr = _register_printer(monkeypatch, TempOfNozzle=210, TempTargetNozzle=215, TempOfHotbed=60)
    pr.connected = True   # offline printers zero their temps (history, not a reading)
    await mqtt_bridge.ensure_started()

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
    await mqtt_bridge.ensure_started()

    msg = await broker.wait_for_publish("spooler/p1/state")
    s = json.loads(msg.payload)
    assert (s["nozzle_temp"], s["nozzle_target"], s["bed_temp"]) == (0, 0, 0)


# ── status_update fan-out: transitions immediate, numerics throttled ────────

async def test_status_update_republishes_on_a_state_transition(broker, monkeypatch):
    pr = _register_printer(monkeypatch)
    await mqtt_bridge.ensure_started()
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
    await mqtt_bridge.ensure_started()
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
    await mqtt_bridge.ensure_started()
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
    await mqtt_bridge.ensure_started()

    cfg = await broker.wait_for_publish("homeassistant/sensor/spooler/p1/canvas_tray2_status/config")
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
        assert f"homeassistant/sensor/spooler/p1/{key}/config" in topics, key


async def test_no_canvas_no_canvas_discovery(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/state")
    assert not [p for p in broker.topics("homeassistant") if "canvas" in p.topic]


async def test_canvas_arriving_later_still_gets_discovered(broker, monkeypatch):
    pr = _register_printer(monkeypatch)
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/state")
    assert not [p for p in broker.topics("homeassistant") if "canvas" in p.topic]

    pr.status["canvas_info"] = CANVAS_INFO          # the 30 s poll lands
    state.publish("status_update", printer_id="p1", printer=pr.to_dict())

    cfg = await broker.wait_for_publish("homeassistant/sensor/spooler/p1/canvas_tray2_status/config")
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


# ── availability LWT: the bridge dying must not leave a stale "online" ───────

async def test_lwt_declared_and_offline_fires_when_the_connection_dies(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.ensure_started()
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


# ── stop_and_clear: "off means off" — entities vanish, not go stale ─────────

async def test_stop_and_clear_empties_every_retained_topic(broker, monkeypatch):
    _register_printer(monkeypatch, canvas_info=CANVAS_INFO)
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/state")
    announced = sorted({p.topic for p in broker.publishes if p.retain})

    await mqtt_bridge.stop_and_clear()

    cleared = {p.topic: p for p in broker.publishes if p.payload == b""}
    assert "spooler/bridge/availability" in cleared
    for topic in announced:
        last = cleared.get(topic)
        assert last is not None, f"{topic} was never cleared"
        assert last.retain, f"{topic} was cleared non-retained"
    assert mqtt_bridge._discovered == {} and mqtt_bridge._published == {}


async def test_stop_and_clear_without_a_prior_run_is_a_no_op(broker):
    n = len(broker.publishes)
    await mqtt_bridge.stop_and_clear()
    assert len(broker.publishes) == n


async def test_stop_and_clear_keeps_the_record_when_the_broker_is_gone(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/state")
    assert mqtt_bridge._discovered

    await broker.stop()                 # broker unreachable at clear time
    await mqtt_bridge.stop_and_clear()
    assert mqtt_bridge._discovered, "the record must survive so the next clear can retry"


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
    await mqtt_bridge.ensure_started()

    cfg = await broker.wait_for_publish("homeassistant/camera/spooler/p1/camera/config")
    assert cfg.retain
    payload = json.loads(cfg.payload)
    assert payload["topic"] == "spooler/p1/camera/frame"
    assert payload["unique_id"] == "spooler-p1-camera"
    assert payload["availability_topic"] == "spooler/bridge/availability"


async def test_no_camera_url_no_camera_discovery(broker, monkeypatch):
    _register_printer(monkeypatch)
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/state")
    assert not [p for p in broker.topics("homeassistant") if p.topic.startswith("homeassistant/camera/")]


async def test_camera_frames_flow_while_printing(broker, monkeypatch):
    import mqtt_bridge as mb
    monkeypatch.setattr(mb, "CAMERA_INTERVAL_S", 0.05)
    monkeypatch.setattr(mb, "grab_jpeg", lambda url, timeout=5.0: b"jpeg-" + url.encode())
    _printing_printer(monkeypatch)
    await mqtt_bridge.ensure_started()

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
    await mqtt_bridge.ensure_started()
    await asyncio.sleep(0.15)
    assert not grabs, "an idle printer gets no frames"


# ── realtime camera (flavor b): paste-ready HA camera.mjpeg YAML ─────────────

async def test_camera_yaml_published_when_base_and_token_are_configured(broker, monkeypatch):
    _register_printer(monkeypatch)
    state.printers["p1"].camera_url = "http://192.0.2.5:8080/mjpeg"
    config_set("camera.external_url", "http://spooler.lan:8080")
    config_set("camera.external_token", "TOK-123")
    await mqtt_bridge.ensure_started()

    msg = await broker.wait_for_publish("spooler/p1/camera/mjpeg_yaml")
    assert msg.retain
    text = msg.payload.decode()
    assert "platform: mjpeg" in text
    assert "http://spooler.lan:8080/api/camera/p1?token=TOK-123" in text
    assert "http://spooler.lan:8080/api/snapshot/p1?token=TOK-123" in text


async def test_camera_yaml_needs_the_config(broker, monkeypatch):
    _register_printer(monkeypatch)
    state.printers["p1"].camera_url = "http://192.0.2.5:8080/mjpeg"
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/state")
    assert broker.payloads("spooler/p1/camera/mjpeg_yaml") == []
    config_set("camera.external_url", "")            # token alone is not enough either
    config_set("camera.external_token", "TOK-123")
    state.publish("status_update", printer_id="p1", printer=state.printers["p1"].to_dict())
    await asyncio.sleep(0.05)
    assert broker.payloads("spooler/p1/camera/mjpeg_yaml") == []


async def test_camera_yaml_republished_when_the_token_rotates(broker, monkeypatch):
    _register_printer(monkeypatch)
    state.printers["p1"].camera_url = "http://192.0.2.5:8080/mjpeg"
    config_set("camera.external_url", "http://spooler.lan:8080")
    config_set("camera.external_token", "TOK-1")
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/camera/mjpeg_yaml")

    config_set("camera.external_token", "TOK-2")
    state.publish("status_update", printer_id="p1", printer=state.printers["p1"].to_dict())
    async def _has_v2():
        while b"TOK-2" not in (broker.payloads("spooler/p1/camera/mjpeg_yaml") or [b""])[-1]:
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_has_v2(), 2.0)


async def test_camera_yaml_cleared_when_the_config_goes_away(broker, monkeypatch):
    _register_printer(monkeypatch)
    state.printers["p1"].camera_url = "http://192.0.2.5:8080/mjpeg"
    config_set("camera.external_url", "http://spooler.lan:8080")
    config_set("camera.external_token", "TOK-123")
    await mqtt_bridge.ensure_started()
    await broker.wait_for_publish("spooler/p1/camera/mjpeg_yaml")

    config_set("camera.external_token", "")
    state.publish("status_update", printer_id="p1", printer=state.printers["p1"].to_dict())
    async def _cleared():
        while b"" not in broker.payloads("spooler/p1/camera/mjpeg_yaml"):
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_cleared(), 2.0)
