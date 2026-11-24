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


# ── job_queue ────────────────────────────────────────────────────────────────

class QueuePrinter:
    """The shape job_queue needs from a printer (tests/test_job_queue.py
    keeps the full fake; only what print_started/ended touch lives here)."""

    def __init__(self, printer_id="p1"):
        self.id = printer_id
        self.name = f"Printer {printer_id}"
        self.printer_type = "cc2"
        self.connected = True
        self._state = "idle"

    def to_dict(self):
        return {"state": self._state}

    def _tray_spool(self):
        return None

    async def upload_file(self, local_path, remote_name, start_after=False):
        return True

    async def start_print_file(self, filename, print_opts=None):
        return True


def _queued_printing_job(tmp_path, printer_id="p1"):
    """A job confirmed onto a printer and printing — the sequence
    test_job_queue.py's helper uses."""
    from job_queue import add_job, assign_ready_jobs, confirm_plate_cleared
    path = tmp_path / "a.gcode"
    path.write_bytes(b"x" * 100)
    state.printers[printer_id] = QueuePrinter(printer_id)
    add_job(path, "a.gcode", {"quantity": 1})
    assign_ready_jobs()
    return confirm_plate_cleared(printer_id)      # awaited by the caller


async def test_print_started_marks_the_queued_job_as_printing(tmp_path):
    import job_queue
    features.set_enabled("job_queue", True)
    features.set_enabled("file_upload", True)
    await _queued_printing_job(tmp_path)
    state.publish("print_started", printer_id="p1", data={"filename": "a.gcode"})
    await _wait_for(lambda: job_queue.list_jobs()[0].get("print_seen"))
    assert job_queue.list_jobs()[0]["status"] == "printing"


@pytest.mark.parametrize("end_state,status", [("complete", "done"), ("error", "error"), ("cancelled", "stopped")])
async def test_print_ended_closes_the_queued_job_with_its_end_state(tmp_path, end_state, status):
    import job_queue
    features.set_enabled("job_queue", True)
    features.set_enabled("file_upload", True)
    await _queued_printing_job(tmp_path)
    state.publish("print_started", printer_id="p1", data={"filename": "a.gcode"})
    await state.publish_now("print_ended", printer_id="p1",
                            data={"end_state": end_state, "error": "Nozzle clog" if end_state == "error" else None})
    job = job_queue.list_jobs()[0]
    assert job["status"] == status and job["finished_at"]
    assert job["error"] == ("Nozzle clog" if status == "error" else None)


# ── auto-light ───────────────────────────────────────────────────────────────
# Only the light-ON side migrates: light-off is sequenced with the
# end-of-print picture inside the machine (it must stay on for the shot),
# so it stays inline per ADR-0002's principle.

async def test_print_started_turns_the_auto_light_on(monkeypatch):
    from printers.base import PrinterConnection
    p = PrinterConnection("p1", "192.0.2.5", "Light")
    p.auto_light, p.supports_light, p.connected = True, True, True
    light_calls = []

    async def set_light(on):
        light_calls.append(on)
    p.set_light = set_light                     # observe without a real printer
    monkeypatch.setattr(state, "printers", {"p1": p})

    state.publish("print_started", printer_id="p1")
    await _wait_for(lambda: light_calls == [True])


async def test_print_started_leaves_the_light_alone_when_auto_light_is_off(monkeypatch):
    from printers.base import PrinterConnection
    p = PrinterConnection("p1", "192.0.2.5", "Light")
    p.auto_light, p.supports_light, p.connected = False, True, True
    light_calls = []

    async def set_light(on):
        light_calls.append(on)
    p.set_light = set_light
    monkeypatch.setattr(state, "printers", {"p1": p})

    state.publish("print_started", printer_id="p1")
    await asyncio.sleep(0.05)
    assert light_calls == []
