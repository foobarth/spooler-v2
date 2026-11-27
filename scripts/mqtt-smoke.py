#!/usr/bin/env python3
"""mqtt-smoke.py — the real-broker contracts the in-process fake broker
structurally cannot verify (docs/adr-mqtt-bridge.md §Patterns).

Offline by default. Run against a REAL broker (~15 s):

    SPOOLER_MQTT_SMOKE_URL=mqtt://mosquitto.casa.barth:1883 python scripts/mqtt-smoke.py

What it pins, against production code (mqtt_bridge, not a re-implementation):
  1. retention: an empty retained publish clears a topic on the real broker
  2. discovery flip: ON announces; OFF leaves ZERO homeassistant/…/config
     retained topics — including strays a "previous instance" seeded
     (canvas tray 7, beyond any real tray set — the superset sweep's contract)
  3. notification ordering: the JPEG lands on the image topic BEFORE the JSON,
     both non-retained
  4. LWT: a killed bridge process leaves availability retained "offline"
  5. off-means-off: switching off empties every topic the run produced

The run is fully namespaced under spooler-smoke/… (its own client id,
availability topic and notification topics), so it can run against a broker
that already hosts a live Spooler without kicking it or touching its topics.
Exits non-zero on the first failing contract.
"""

import asyncio
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path

URL = os.getenv("SPOOLER_MQTT_SMOKE_URL", "")
PID = "smoke1"
CLIENT = "smoke-observer"
AVAIL = "spooler-smoke/availability"
NOTIFY = "spooler-smoke/notification"
NOTIFY_IMAGE = "spooler-smoke/notification/image"
REPO = Path(__file__).resolve().parent.parent


def fail(msg):
    print(f"  FAIL: {msg}")
    sys.exit(1)


def pass_(name):
    print(f"  PASS: {name}")


async def wipe_namespace(url):
    """Remove retained garbage from PREVIOUS smoke runs (a failing run exits
    before its off-clear): subscribe the smoke namespace for a moment, empty
    every topic that still holds a retained payload."""
    import aiomqtt
    leftovers = []

    async def collect():
        async with aiomqtt.Client(hostname=url.hostname, port=url.port or 1883, identifier="smoke-wiper") as c:
            await c.subscribe("spooler-smoke/#")
            await c.subscribe("spooler/smoke1/#")
            await c.subscribe("homeassistant/+/smoke1/#")
            async for m in c.messages:
                if m.payload:
                    leftovers.append(str(m.topic))

    try:
        await asyncio.wait_for(collect(), 3)
    except TimeoutError:
        pass
    if leftovers:
        print(f"  wipe: emptying {len(leftovers)} retained topic(s) from earlier smoke runs")
        async with aiomqtt.Client(hostname=url.hostname, port=url.port or 1883, identifier="smoke-wiper") as c:
            for topic in leftovers:
                await c.publish(topic, b"", retain=True, timeout=5.0)
            await asyncio.sleep(0.3)


class Recorder:
    """A plain MQTT observer. Live subscribers get a retained publish with
    retain=False (mosquitto's 3.1.1 behavior), so the last value per topic is
    tracked for ALL messages, not just retain-flagged ones."""

    def __init__(self):
        self.seen = []               # (topic, payload, retain) in arrival order
        self.last = {}               # topic -> last payload seen
        self._client = None
        self._task = None

    async def start(self, url):
        import aiomqtt
        self._client = aiomqtt.Client(hostname=url.hostname, port=url.port or 1883,
                                      identifier=CLIENT,
                                      username=url.username or None, password=url.password or None)
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        async with self._client as client:
            await client.subscribe("spooler-smoke/#")
            await client.subscribe("spooler/#")          # the pid-scoped state topic
            # discovery topics are homeassistant/<component>/<pid>/… — the pid
            # sits behind the component level, hence the + wildcard
            await client.subscribe(f"homeassistant/+/{PID}/#")
            async for m in client.messages:
                topic, payload = str(m.topic), m.payload
                self.seen.append((topic, payload, m.retain))
                self.last[topic] = payload

    def topics_with_content(self, prefix, exclude=()):
        return [t for t, p in self.last.items()
                if t.startswith(prefix) and p and p != b"" and not any(t.startswith(e) for e in exclude)]


async def wait_for(pred, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.05)
    return False


async def raw_publish(url, topic, payload, retain):
    import aiomqtt
    async with aiomqtt.Client(hostname=url.hostname, port=url.port or 1883, identifier="smoke-seeder") as c:
        await c.publish(topic, payload, retain=retain, timeout=5.0)
        await asyncio.sleep(0.3)     # let the QoS-0 write flush before the client exits


def setup_env():
    """Isolated data dir BEFORE any repo import binds DATA_DIR."""
    os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="mqtt-smoke-")
    sys.path.insert(0, str(REPO))


def namespace(mqtt_bridge):
    """Run the bridge fully namespaced under spooler-smoke/: its own client
    id, availability topic and notification topics, so a smoke run never
    kicks a live Spooler's connection (same client id = broker takeover) nor
    touches its topics. Called by parent AND child before any use."""
    mqtt_bridge._client_id = "spooler-smoke"
    mqtt_bridge.BRIDGE_AVAILABILITY = AVAIL
    mqtt_bridge.NOTIFY_TOPIC = NOTIFY
    mqtt_bridge.NOTIFY_IMAGE_TOPIC = NOTIFY_IMAGE


async def child_bridge(url_str):
    """--child: connect the bridge and idle until the parent kills us hard."""
    setup_env()
    import config
    import mqtt_bridge
    import state
    from printers.base import PrinterConnection

    namespace(mqtt_bridge)
    config.set("mqtt.url", url_str)
    p = PrinterConnection(PID, "192.0.2.5", "Smoke")
    state.printers[PID] = p
    await mqtt_bridge.sync_lifecycle()
    await asyncio.sleep(600)          # the parent kills us mid-connection


async def run_contracts():
    import config
    import features
    import mqtt_bridge
    import state
    from notify import Notification
    from printers.base import PrinterConnection
    import urllib.parse

    namespace(mqtt_bridge)          # BEFORE any bridge use: no takeover of a live Spooler

    url = urllib.parse.urlparse(URL)
    config.set("mqtt.url", URL)
    features.set_enabled("mqtt_integration", True)

    rec = Recorder()
    await rec.start(url)

    # ── 1: an empty retained publish clears the topic ────────────────────────
    probe = f"spooler-smoke/probe/{int(time.time())}"
    await raw_publish(url, probe, b"something", retain=True)
    if not await wait_for(lambda: rec.last.get(probe) == b"something", 5, "retained seed"):
        fail("the seeded retained payload never arrived")
    await raw_publish(url, probe, b"", retain=True)
    if not await wait_for(lambda: rec.last.get(probe) in (b"", None), 5, "retained clear"):
        fail("an empty retained publish did not clear the topic on the real broker")
    pass_("retention: empty retained publish clears the topic")

    # ── 2: discovery flip + the superset sweep over seeded strays ────────────
    stale = f"homeassistant/sensor/{PID}/canvas_tray7_status/config"
    await raw_publish(url, stale, b'{"name": "stale from a previous instance"}', retain=True)
    config.set("mqtt.home_assistant", True)
    state.printers[PID] = PrinterConnection(PID, "192.0.2.5", "Smoke")
    await mqtt_bridge.sync_lifecycle()
    if not await wait_for(lambda: rec.last.get(f"homeassistant/sensor/{PID}/nozzle_temp/config"), 10, "discovery announce"):
        fail("the bridge never announced discovery on the real broker")
    config.set("mqtt.home_assistant", False)
    await mqtt_bridge.sync_lifecycle()
    # the flip is a real transition: availability is emptied, then re-announced
    # on reconnect — wait for BOTH phases so later contracts see a live connection
    if not await wait_for(lambda: rec.last.get(AVAIL) == b"", 5, "flip clear"):
        fail("the discovery flip did not empty the availability topic")
    if not await wait_for(lambda: rec.last.get(AVAIL) == b"online", 5, "flip re-announce"):
        fail("the bridge did not re-announce after the discovery flip")
    if not await wait_for(lambda: not rec.topics_with_content(f"homeassistant/{PID}/"), 10, "discovery sweep"):
        fail(f"discovery-off left retained configs behind: {rec.topics_with_content(f'homeassistant/{PID}/')[:3]}")
    if not await wait_for(lambda: rec.last.get(f"spooler/{PID}/state"), 5, "state republished"):
        fail("the agnostic state topic was not republished after the discovery flip")
    pass_("discovery flip: zero homeassistant/ topics left, state keeps flowing (incl. seeded tray-7 stray)")

    # ── 3: notification image before JSON, both non-retained ─────────────────
    n = Notification("print_complete", PID, "Smoke — Print complete",
                     "smoke test body", image=b"smoke-jpeg-bytes", printer_name="Smoke")
    await asyncio.to_thread(mqtt_bridge.publish_notification, n)
    order = [(t, p, r) for t, p, r in rec.seen if t in (NOTIFY, NOTIFY_IMAGE)]
    if [t for t, p, r in order].count(NOTIFY_IMAGE) < 1 or [t for t, p, r in order].count(NOTIFY) < 1:
        fail(f"notification publish incomplete: {[t for t, p, r in order]}")
    img_idx = next(i for i, (t, p, r) in enumerate(order) if t == NOTIFY_IMAGE)
    json_idx = next(i for i, (t, p, r) in enumerate(order) if t == NOTIFY)
    if img_idx > json_idx:
        fail("the JSON landed before the image; consumers would miss the picture")
    pass_("notification: image lands before the JSON")

    # ── 4: LWT — a killed bridge leaves availability offline ─────────────────
    child_marker = len(rec.seen)
    child = subprocess.Popen([sys.executable, __file__, "--child"], cwd=REPO,
                             env={**os.environ, "SPOOLER_MQTT_SMOKE_URL": URL},
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        def child_online():
            return any(t == AVAIL and p == b"online" for t, p, r in rec.seen[child_marker:])
        if not await wait_for(child_online, 10, "child bridge online"):
            fail("the child bridge never came online on the real broker")
        offline_marker = len(rec.seen)
        child.send_signal(signal.SIGKILL)

        def child_offline():
            return any(t == AVAIL and p == b"offline" for t, p, r in rec.seen[offline_marker:])
        if not await wait_for(child_offline, 5, "LWT offline"):
            fail("SIGKILL did not fire the LWT — availability stays stale-online in HA")
        pass_("LWT: a killed bridge leaves availability retained offline")
    finally:
        if child.poll() is None:
            child.kill()

    # ── 5: off-means-off — everything the run produced is emptied ────────────
    config.set("mqtt.url", "")
    await mqtt_bridge.sync_lifecycle()
    watch = ("spooler-smoke/", f"homeassistant/{PID}/", f"spooler/{PID}/")
    exclude = (NOTIFY, NOTIFY_IMAGE)          # non-retained by design (contract 3)
    if not await wait_for(lambda: not rec.topics_with_content(watch[0], exclude)
                          and not rec.topics_with_content(watch[1])
                          and not rec.topics_with_content(watch[2]), 10, "off-clear"):
        fail(f"off-means-off left retained payloads: "
             f"{(rec.topics_with_content(watch[0], exclude) + rec.topics_with_content(watch[1]))[:3]}")
    pass_("off-means-off: every retained payload of this run is gone")


async def main():
    if not URL:
        print("Set SPOOLER_MQTT_SMOKE_URL=mqtt://host:port to run the real-broker smoke. "
              "The run is namespaced under spooler-smoke/ and never touches a live Spooler's topics.")
        return 0
    setup_env()
    if "--child" in sys.argv:
        await child_bridge(URL)
        return 0
    print(f"Real-broker smoke against {URL} (namespaced under spooler-smoke/)")
    url = urllib.parse.urlparse(URL)
    await wipe_namespace(url)
    t0 = time.monotonic()
    await run_contracts()
    print(f"All real-broker contracts passed in {time.monotonic() - t0:.1f} s.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
