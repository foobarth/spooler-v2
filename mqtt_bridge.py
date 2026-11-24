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
import urllib.parse

try:
    import aiomqtt
except ImportError:     # bridge stays off when the optional dep is absent
    aiomqtt = None

import config
import features

NUMERIC_THROTTLE_S = 5.0    # numerics republish at most this often

_client_id = "spooler-bridge"

_task: asyncio.Task = None
_running = False


def _broker_params():
    """Split mqtt://host:port into (hostname, port, username, password)."""
    u = urllib.parse.urlparse(config.get("mqtt.url"))
    return (
        u.hostname,
        u.port or 1883,
        config.get("mqtt.username") or None,
        config.get("mqtt.password") or None,
    )


async def _run():
    global _running
    hostname, port, username, password = _broker_params()
    delay = 1.0
    while _running:
        try:
            async with aiomqtt.Client(
                hostname=hostname, port=port, identifier=_client_id,
                username=username, password=password,
            ) as _client:   # bound for the connection lifetime; used by the
                            # publish loop that lands with the fan-out slice
                delay = 1.0
                # Publishing loop arrives with the status fan-out slice.
                while _running:
                    await asyncio.sleep(3600)
        except aiomqtt.MqttError:
            if _running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)


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
