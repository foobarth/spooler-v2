"""mqtt_bridge.py — Home Assistant MQTT bridge (read-only v1).

Tests run against tests/fake_mqtt.py, an in-process broker. The bridge is
never tested through its internals: tests read the broker's recorded
connect/publish log. See docs/adr-mqtt-bridge.md.
"""


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
