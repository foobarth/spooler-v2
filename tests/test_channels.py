"""channels.py — the event-bus wiring.

Each channel attachment is tested through publish → observed side effect,
never by calling the channel's internals. See docs/adr-event-bus.md.
"""

import asyncio
import json

import pytest

import channels
import dataset
import features
import state


@pytest.fixture(autouse=True)
def wired():
    state.reset_subscriptions()
    channels.wire()
    yield
    state.reset_subscriptions()


async def _wait_for(cond, timeout=2.0):
    async def _poll():
        while not cond():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), timeout)


class DatasetPrinter:
    """Minimal stand-in registered in state.printers. The dataset channel
    needs a camera picture and a to_dict() carrying the decoded PrintInfo —
    the canonical shape it now reads from the event envelope."""

    id = "p1"
    name = "My Printer/1"
    printer_type = "cc2"

    def __init__(self):
        self.jpeg = b"jpeg"
        self.info = {"Status": 2, "Filename": "benchy.gcode", "Progress": 42,
                     "CurrentLayer": 7, "TotalLayer": 100}

    async def _camera_picture(self, max_age_s=20.0, force=False):
        return self.jpeg

    def to_dict(self):
        return {"id": "p1", "name": self.name, "status": {"PrintInfo": self.info}}


# ── dataset ──────────────────────────────────────────────────────────────────

async def test_print_started_triggers_the_before_print_picture(tmp_path, monkeypatch):
    monkeypatch.setattr(dataset, "dataset_dir", lambda: tmp_path)
    monkeypatch.setattr(dataset, "_jobs", {})
    features.set_enabled("dataset_collection", True)
    features.set_enabled("camera", True)
    p = DatasetPrinter()
    monkeypatch.setattr(state, "printers", {"p1": p})

    state.publish("print_started", printer_id="p1", printer=p.to_dict(),
                  data={"filename": "benchy.gcode"})
    await _wait_for(lambda: dataset._jobs.get("p1") is not None)

    (folder,) = [d for d in tmp_path.glob("*/*") if d.is_dir()]
    lines = [json.loads(x) for x in (folder / "pictures.jsonl").read_text().splitlines()]
    assert [x["label"] for x in lines] == ["before"]
    assert json.loads((folder / "job.json").read_text())["file"] == "benchy.gcode"
