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


@pytest.fixture
async def broker(monkeypatch):
    b = fake_mqtt.FakeBroker()
    await b.start()
    config_set("mqtt.url", f"mqtt://127.0.0.1:{b.port}")
    monkeypatch.setattr(mqtt_bridge, "_published", {})
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
    assert payload["device"]["name"] == "Workshop"
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

