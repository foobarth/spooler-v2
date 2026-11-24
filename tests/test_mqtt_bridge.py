"""mqtt_bridge.py — Home Assistant MQTT bridge (read-only v1).

Tests run against tests/fake_mqtt.py, an in-process broker. The bridge is
never tested through its internals: tests read the broker's recorded
connect/publish log. See docs/adr-mqtt-bridge.md.
"""


import json

import pytest

import fake_mqtt
import mqtt_bridge
from config import set as config_set


@pytest.fixture
async def broker(monkeypatch):
    b = fake_mqtt.FakeBroker()
    await b.start()
    config_set("mqtt.url", f"mqtt://127.0.0.1:{b.port}")
    yield b
    await mqtt_bridge.stop_and_clear()
    await b.stop()


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
