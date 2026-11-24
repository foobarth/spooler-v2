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


# ── bridge_state: normalized extraction from to_dict() ──────────────────────

CC1_STATUS = {"TempOfNozzle": 210, "TempTargetNozzle": 215, "TempOfHotbed": 60,
              "TempTargetHotbed": 65, "TempOfBox": 30, "SpeedFactor": 95,
              "PrintInfo": {"CurrentLayer": 12, "TotalLayer": 100,
                            "PrintTime": 120, "RemainTime": 300}}


def _d(status, **over):
    d = {"id": "p1", "name": "Workshop", "printer_type": "cc1", "connected": True,
         "state": "printing", "status": status, "filament_mm": 1234.5, "filament_g": 3.7,
         "firmware_version": "1.2.3"}
    d.update(over)
    return d


def test_bridge_state_reads_cc1_key_names():
    s = mqtt_bridge.bridge_state(_d(CC1_STATUS))
    assert s["nozzle_temp"] == 210 and s["nozzle_target"] == 215
    assert s["bed_temp"] == 60 and s["bed_target"] == 65
    assert s["chamber_temp"] == 30
    assert s["layer"] == 12 and s["total_layer"] == 100
    assert s["time_remaining"] == 300
    assert s["speed_factor"] == 95


def test_bridge_state_reads_cc2_key_names():
    s = mqtt_bridge.bridge_state(_d({"NozzleTemp": 1, "NozzleTempTarget": 2, "BedTemp": 3,
                                     "BedTempTarget": 4, "ChamberTemp": 5}))
    assert (s["nozzle_temp"], s["nozzle_target"], s["bed_temp"],
            s["bed_target"], s["chamber_temp"]) == (1, 2, 3, 4, 5)


def test_bridge_state_progress_prefers_printer_percentage():
    # TotalTicks == 100 means CurrentTicks IS the percentage (CC2/Moonraker/PrusaLink);
    # it beats layers, which understate progress (issue #56).
    s = mqtt_bridge.bridge_state(_d({"PrintInfo": {"TotalTicks": 100, "CurrentTicks": 42,
                                                   "TotalLayer": 100, "CurrentLayer": 99}}))
    assert s["progress"] == 42


def test_bridge_state_progress_from_layers_when_no_percentage():
    s = mqtt_bridge.bridge_state(_d({"PrintInfo": {"TotalTicks": 700, "CurrentTicks": 40,
                                                   "TotalLayer": 100, "CurrentLayer": 25}}))
    assert s["progress"] == 25


def test_bridge_state_progress_null_when_unknown():
    s = mqtt_bridge.bridge_state(_d({}))
    assert s["progress"] is None


def test_bridge_state_carries_identity_and_derived_fields():
    s = mqtt_bridge.bridge_state(_d(CC1_STATUS, state="paused", connected=False))
    assert s["state"] == "paused"
    assert s["printing"] is False
    assert s["connected"] is False
    assert s["filament_used_mm"] == 1234.5 and s["filament_used_g"] == 3.7
    assert s["firmware"] == "1.2.3"


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
                "filament_used_mm", "filament_used_g", "speed_factor", "firmware"):
        assert f"homeassistant/sensor/spooler/p1/{key}/config" in topics, key
    for key in ("connected", "printing"):
        assert f"homeassistant/binary_sensor/spooler/p1/{key}/config" in topics, key
