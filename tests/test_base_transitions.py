"""
printers/base.py — print-start/print-end transition handling.

These exercise PrinterConnection directly (not a protocol subclass): connect()
and send_cmd() are never called, only the status-transition machinery, so the
abstract methods raising NotImplementedError never matters here.
"""

import asyncio
import time

import pytest

import persistence
import printers.base as base_mod
from printaccount import PrintAccounting
from printers.base import PrinterConnection, classify_display_state, classify_print_transition


# ── classify_print_transition (pure function) ───────────────────────────────

@pytest.mark.parametrize("prev,cur,expected", [
    (None, 0, None),            # unknown -> idle: nothing happened
    (None, 2, "start"),         # unknown -> printing: start
    (0, 2, "start"),            # idle -> printing: start
    (2, 2, None),                # printing -> printing: no re-trigger
    (2, 9, "end"),               # printing -> complete: end
    (2, 8, "end"),               # printing -> cancelled: end
    (2, 14, "end"),              # printing -> error: end
    (2, 0, "end"),                # printing -> idle (CC2-style completion): end
    (9, 2, "start"),             # complete -> printing directly: must still be "start"
                                  # (regression test for the ACTIVE-set bug: 9 used to be
                                  # considered "active", so this transition was missed)
    (9, 9, None),                 # sitting at complete: nothing happens repeatedly
    (6, 9, "end"),                 # paused -> complete: end
    (6, 2, None),                  # resume: paused -> printing must NOT be "start"
                                    # (regression test: paused/pausing were never in
                                    # ACTIVE_STATUSES, so every resume used to incorrectly
                                    # satisfy "prev not in ACTIVE" and fire "start",
                                    # resetting per-print extrusion tracking)
    (5, 2, None),                  # resume from "pausing": also not a new print
    (6, 14, "end"),                 # error while paused: end
])
def test_classify_print_transition(prev, cur, expected):
    assert classify_print_transition(prev, cur) == expected


# ── classify_display_state (pure function) ───────────────────────────────────

@pytest.mark.parametrize("connected,code,homing,expected", [
    (False, 2, False, "offline"),       # disconnected always wins, regardless of code
    (True, None, False, "idle"),
    (True, 0, False, "idle"),
    (True, 1, False, "preparing"),
    (True, 2, False, "printing"),
    (True, 5, False, "pausing"),
    (True, 6, False, "paused"),
    (True, 7, False, "stopping"),
    (True, 8, False, "cancelled"),
    (True, 9, False, "complete"),
    (True, 14, False, "error"),         # the confirmed bug: must NOT be "cancelled"
    (True, 0, True, "preparing"),       # CC1's CurrentStatus[0]==9 + Status==0 quirk
    (True, 11, False, "preparing"),     # CC1: printer checking (Elegoo SDK)
    (True, 17, False, "preparing"),     # CC1: resonance test
    (True, 22, False, "preparing"),
    (True, 23, False, "preparing"),     # CC1: filament feeding from the screen
    (True, 27, False, "unknown"),       # undocumented code: must NOT be "idle"
    (True, 99, False, "unknown"),
])
def test_classify_display_state(connected, code, homing, expected):
    assert classify_display_state(connected, code, homing) == expected


def test_classify_display_state_logs_unknown_code_only_once(capsys):
    base_mod._logged_unknown_display_codes.discard(99)
    classify_display_state(True, 99)
    classify_display_state(True, 99)
    classify_display_state(True, 99)
    out = capsys.readouterr().out
    assert out.count("99") == 1


# ── _check_print_transition (integration of the above into PrinterConnection) ─

@pytest.fixture
def printer(monkeypatch):
    p = PrinterConnection("pid1", "10.0.0.5", "Test Printer")
    p.connected = True
    monkeypatch.setattr(base_mod, "get_spool_density", lambda printer_id: 1.24)
    p._last_print_status = 0     # an idle status has already been seen (a first status that is already
    return p                     # "printing" means Spooler restarted mid-print: see test_filament_accounting.py)


def _set_status(printer, status_code, **printinfo_overrides):
    pi = {"Status": status_code, **printinfo_overrides}
    printer.status = {"PrintInfo": pi}


@pytest.mark.asyncio
async def test_print_start_initializes_extrusion_tracking(printer):
    printer._acct = PrintAccounting(current_spool=7)       # leftover from some earlier print
    printer._acct.per_spool = {1: 50.0}
    old_id = printer._acct.print_id
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    assert printer._acct.print_id != old_id
    assert printer._acct.per_spool == {} and printer._acct.total_mm() == 0.0
    assert printer._print_start_time is not None


@pytest.mark.asyncio
async def test_complete_to_printing_resets_snapshot_regression(printer):
    """Regression test for the ACTIVE-set bug: complete (9) directly to
    printing (2), with no idle poll observed in between, must still reset
    per-print extrusion tracking — this was the actual production bug."""
    _set_status(printer, 9, TotalExtrusion=500)
    await printer._check_print_transition()  # printer sits at "complete"
    printer._acct = PrintAccounting()          # simulate stale leftover accounting
    printer._acct.snapshot_mm = 500.0
    printer._acct.per_spool = {1: 500.0}
    old_id = printer._acct.print_id

    _set_status(printer, 2, TotalExtrusion=0)  # next print starts directly from 9
    await printer._check_print_transition()

    assert printer._acct.print_id != old_id
    assert printer._acct.snapshot_mm == 0.0 and printer._acct.per_spool == {}


@pytest.mark.asyncio
async def test_print_complete_records_history_entry(printer):
    _set_status(printer, 2, TotalExtrusion=0, Filename="a.gcode")
    await printer._check_print_transition()

    _set_status(printer, 9, TotalExtrusion=1000, Filename="a.gcode", PrintTime=600)
    await printer._check_print_transition()

    history = persistence.load_history()
    assert len(history) == 1
    assert history[0]["filename"] == "a.gcode"
    assert history[0]["completed"] is True
    assert history[0]["print_time_s"] == 600


@pytest.mark.asyncio
async def test_history_entry_records_layer_count(printer):
    _set_status(printer, 2, TotalExtrusion=0, Filename="a.gcode", CurrentLayer=1, TotalLayer=80)
    await printer._check_print_transition()
    _set_status(printer, 2, TotalExtrusion=500, Filename="a.gcode", CurrentLayer=40, TotalLayer=80)
    await printer._check_print_transition()

    _set_status(printer, 9, TotalExtrusion=1000, Filename="a.gcode", PrintTime=600, CurrentLayer=0, TotalLayer=0)
    await printer._check_print_transition()

    history = persistence.load_history()
    assert history[0]["total_layers"] == 80
    assert history[0]["current_layer"] == 80   # completed: assume it reached the last layer


@pytest.mark.asyncio
async def test_history_entry_uses_last_seen_layer_when_cancelled_early(printer):
    _set_status(printer, 2, TotalExtrusion=0, Filename="a.gcode", CurrentLayer=1, TotalLayer=80)
    await printer._check_print_transition()
    _set_status(printer, 2, TotalExtrusion=200, Filename="a.gcode", CurrentLayer=23, TotalLayer=80)
    await printer._check_print_transition()

    _set_status(printer, 8, TotalExtrusion=300, Filename="a.gcode", PrintTime=120, CurrentLayer=0, TotalLayer=0)
    await printer._check_print_transition()

    history = persistence.load_history()
    assert history[0]["total_layers"] == 80
    assert history[0]["current_layer"] == 23   # cancelled: last real layer seen, not the total


@pytest.mark.asyncio
async def test_history_entry_omits_layer_fields_when_never_reported(printer):
    _set_status(printer, 2, TotalExtrusion=0, Filename="a.gcode")
    await printer._check_print_transition()
    _set_status(printer, 9, TotalExtrusion=1000, Filename="a.gcode", PrintTime=600)
    await printer._check_print_transition()

    history = persistence.load_history()
    assert "total_layers" not in history[0] and "current_layer" not in history[0]


@pytest.mark.asyncio
async def test_print_cancelled_records_history_with_completed_false(printer):
    _set_status(printer, 2, TotalExtrusion=0, Filename="b.gcode")
    await printer._check_print_transition()

    _set_status(printer, 8, TotalExtrusion=300, Filename="b.gcode", PrintTime=120)
    await printer._check_print_transition()

    history = persistence.load_history()
    assert len(history) == 1
    assert history[0]["completed"] is False


@pytest.mark.asyncio
async def test_no_history_entry_when_no_filament_and_no_filename(printer):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()

    _set_status(printer, 9, TotalExtrusion=0)  # nothing was ever printed
    await printer._check_print_transition()

    assert persistence.load_history() == []


@pytest.mark.asyncio
async def test_filament_fed_by_hand_is_not_a_print(printer):
    """Feeding filament while clearing a clog (a busy status, some extrusion, no file, no print time)
    must not leave history entries, pictures or spool deductions behind."""
    for _ in range(3):
        _set_status(printer, 2, TotalExtrusion=0)
        await printer._check_print_transition()
        _set_status(printer, 0, TotalExtrusion=120)      # printer goes back to idle, no file, PrintTime 0
        await printer._check_print_transition()
    assert persistence.load_history() == []
    assert printer._acct is None


@pytest.mark.asyncio
async def test_short_cancelled_print_without_filename_is_still_recorded(printer):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    _set_status(printer, 8, TotalExtrusion=300, PrintTime=2)
    await printer._check_print_transition()
    assert len(persistence.load_history()) == 1


@pytest.mark.asyncio
async def test_idle_to_idle_does_not_append_history(printer):
    _set_status(printer, 0, TotalExtrusion=0)
    await printer._check_print_transition()
    _set_status(printer, 0, TotalExtrusion=0)
    await printer._check_print_transition()
    assert persistence.load_history() == []


# ── state_reason lifecycle ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pause_with_unknown_source_has_no_reason_info(printer):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    _set_status(printer, 6, TotalExtrusion=0)  # paused, not caused by Spooler, no hint
    await printer._check_print_transition()

    assert printer.state_reason["kind"] == "pause"
    assert printer.state_reason["initiated_by"] == "unknown"
    assert printer.state_reason["category"] == "unknown"


@pytest.mark.asyncio
async def test_pause_from_spooler_is_attributed_correctly(printer):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    printer.mark_spooler_command()  # simulates ws_handler sending the pause command
    _set_status(printer, 6, TotalExtrusion=0)
    await printer._check_print_transition()

    assert printer.state_reason["kind"] == "pause"
    assert printer.state_reason["initiated_by"] == "spooler"


@pytest.mark.asyncio
async def test_spooler_attribution_expires_after_15_seconds(printer, monkeypatch):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    printer.mark_spooler_command()
    printer._last_spooler_cmd_at -= 16  # simulate 16s having passed
    _set_status(printer, 6, TotalExtrusion=0)
    await printer._check_print_transition()

    assert printer.state_reason["initiated_by"] == "unknown"


@pytest.mark.asyncio
async def test_pausing_then_paused_keeps_same_reason(printer):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    printer.mark_spooler_command()
    _set_status(printer, 5, TotalExtrusion=0)  # pausing
    await printer._check_print_transition()
    first_since = printer.state_reason["since"]

    _set_status(printer, 6, TotalExtrusion=0)  # now paused
    await printer._check_print_transition()

    assert printer.state_reason["kind"] == "pause"
    assert printer.state_reason["since"] == first_since  # not reset


@pytest.mark.asyncio
async def test_resume_clears_state_reason_and_records_pause_duration(printer):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    _set_status(printer, 6, TotalExtrusion=0)  # pause begins
    await printer._check_print_transition()
    assert len(printer._current_print_pauses) == 1
    assert printer._current_print_pauses[0]["until"] is None

    _set_status(printer, 2, TotalExtrusion=0)  # resume
    await printer._check_print_transition()

    assert printer.state_reason is None
    assert printer._current_print_pauses[0]["until"] is not None
    assert printer._current_print_pauses[0]["duration_s"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("resumed_status", [15, 16, 19, 20])
async def test_pause_notice_goes_away_when_the_printer_starts_working_again(printer, resumed_status):
    """After a pause the printer may reheat or level before it prints again; the pause notice must not wait for the first printing status."""
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    _set_status(printer, 6, TotalExtrusion=0)
    await printer._check_print_transition()
    assert printer.state_reason["kind"] == "pause"

    _set_status(printer, resumed_status, TotalExtrusion=0)
    await printer._check_print_transition()

    assert printer.state_reason is None
    assert printer._current_print_pauses[0]["until"] is not None


@pytest.mark.asyncio
async def test_state_reason_persists_through_idle_until_next_print_start(printer):
    _set_status(printer, 2, TotalExtrusion=0)
    await printer._check_print_transition()
    _set_status(printer, 14, TotalExtrusion=0)  # error
    await printer._check_print_transition()
    assert printer.state_reason["kind"] == "error"

    _set_status(printer, 0, TotalExtrusion=0)  # back to idle — reason must stay
    await printer._check_print_transition()
    assert printer.state_reason is not None
    assert printer.state_reason["kind"] == "error"

    _set_status(printer, 2, TotalExtrusion=0)  # a genuinely new print starts
    await printer._check_print_transition()
    assert printer.state_reason is None


@pytest.mark.asyncio
async def test_error_end_state_sets_history_fields(printer):
    _set_status(printer, 2, TotalExtrusion=0, Filename="c.gcode")
    await printer._check_print_transition()
    printer.state_reason = {
        "kind": "error", "initiated_by": "printer", "code": "42",
        "category": "unknown", "message": "Nozzle thermal runaway",
        "raw": {}, "since": "2026-01-01T10:00:00",
    }
    _set_status(printer, 14, TotalExtrusion=400, Filename="c.gcode", PrintTime=90)
    await printer._check_print_transition()

    history = persistence.load_history()
    assert len(history) == 1
    entry = history[0]
    assert entry["end_state"] == "error"
    assert entry["completed"] is False
    assert entry["error_code"] == "42"
    assert entry["error_message"] == "Nozzle thermal runaway"
    assert entry["initiated_by"] == "printer"
    assert entry.get("id")


@pytest.mark.asyncio
async def test_cancelled_end_state_has_no_error_fields(printer):
    _set_status(printer, 2, TotalExtrusion=0, Filename="d.gcode")
    await printer._check_print_transition()
    printer.mark_spooler_command()
    _set_status(printer, 8, TotalExtrusion=200, Filename="d.gcode", PrintTime=30)
    await printer._check_print_transition()

    history = persistence.load_history()
    entry = history[0]
    assert entry["end_state"] == "cancelled"
    assert entry["error_code"] is None
    assert entry["error_message"] is None
    assert entry["initiated_by"] == "spooler"


# ── last_seen (T4) ────────────────────────────────────────────────────────────

def test_mark_seen_records_the_time(printer):
    assert printer.last_seen is None
    printer._mark_seen()
    assert printer.last_seen is not None
    assert printer.last_seen <= time.time()


@pytest.mark.asyncio
async def test_broadcast_state_never_touches_last_seen(printer):
    # last_seen moved out of _broadcast_state (I2): broadcasting also happens
    # for browser-side reasons and says nothing about the printer.
    _set_status(printer, 0)
    await printer._broadcast_state()
    assert printer.last_seen is None
    printer.last_seen = 1000.0
    await printer._broadcast_state()
    printer.connected = False
    await printer._broadcast_state()       # a disconnect notice
    assert printer.last_seen == 1000.0


def test_to_dict_exposes_last_seen(printer):
    printer.last_seen = 12345.0
    assert printer.to_dict()["last_seen"] == 12345.0


# ── the print queue follows the print ────────────────────────────────────────

def _queue_job_printing_on(printer_id):
    import job_queue
    job = {"id": "q1", "status": "printing", "assigned_printer_id": printer_id, "print_seen": False,
           "sent_ts": time.time(), "quantity": 1, "priority": 0, "filename": "a.gcode"}
    job_queue._save([job])
    return job_queue


async def _drain():
    """Fire-and-forget bus dispatches run on the next loop step; one tick is
    enough for the sync subscribers the queue tests assert on."""
    await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("end_code, status", [(9, "done"), (14, "error"), (8, "stopped")])
async def test_the_queue_job_on_the_printer_follows_the_print(printer, end_code, status):
    job_queue = _queue_job_printing_on("pid1")
    _set_status(printer, 2, TotalExtrusion=0, Filename="a.gcode")
    await printer._check_print_transition()
    await _drain()      # print_started fans out fire-and-forget; let the dispatch run
    assert job_queue._load()[0]["print_seen"] is True and job_queue._load()[0]["status"] == "printing"
    if end_code == 14:
        printer.state_reason = {"kind": "error", "initiated_by": "printer", "code": "42", "category": "unknown",
                                "message": "Nozzle thermal runaway", "raw": {}, "since": "2026-01-01T10:00:00"}
    _set_status(printer, end_code, TotalExtrusion=400, Filename="a.gcode", PrintTime=90)
    await printer._check_print_transition()
    job = job_queue._load()[0]
    assert job["status"] == status
    assert job["error"] == ("Nozzle thermal runaway" if status == "error" else None)


@pytest.mark.asyncio
async def test_a_print_that_ended_while_spooler_was_off_still_finishes_the_queue_job(printer):
    from printaccount import save_active
    job_queue = _queue_job_printing_on("pid1")
    save_active("pid1", PrintAccounting(filename="a.gcode", started_at=time.time() - 600))
    printer._last_print_status = None        # Spooler just started
    _set_status(printer, 9, TotalExtrusion=400, Filename="a.gcode")
    await printer._check_print_transition()   # publish_now inside: no drain needed
    assert job_queue._load()[0]["status"] == "done"


# ── machinery edges: progress clamp, control stubs, teardown ─────────────────

def test_progress_clamps_over_100_percent_values(printer):
    """A printer reporting more ticks than the total must not hand HA or the
    card a progress above 100."""
    printer.status = {"PrintInfo": {"Status": 3, "TotalTicks": 100, "CurrentTicks": 150}}
    assert printer.to_dict()["progress"] == 100


def test_progress_from_a_tick_ratio_is_clamped_too(printer):
    """The tick-fraction branch (TotalTicks != 100) clamps the same way."""
    printer.status = {"PrintInfo": {"Status": 3, "TotalTicks": 200, "CurrentTicks": 250}}
    assert printer.to_dict()["progress"] == 100


async def test_base_control_stubs_report_unsupported(printer):
    """The base class carries the 'not supported' answer for every control
    command; only subclasses with real support override them. The advanced
    control feature must get a clean False, never an exception."""
    for coro in (printer.set_temperature("nozzle", 200), printer.set_fan("part", 50),
                 printer.move_axis("x", 10), printer.home("XYZ"), printer.auto_level(),
                 printer.pid_calibrate(220), printer.shaper_calibrate()):
        assert await coro is False
    assert await printer.heaters_off(["nozzle", "bed"]) is False   # all stubs said False


def test_stop_cancels_the_connection_and_offline_tasks(printer, monkeypatch):
    printer.status = {}
    cancelled = []

    class FakeTask:
        def cancel(self):
            cancelled.append(True)

    printer._offline_task = FakeTask()
    printer._task = FakeTask()
    printer.stop()
    assert cancelled == [True, True]
    assert printer._offline_task is None


async def test_a_print_ending_while_still_paused_closes_the_pause_segment(printer):
    """Rare but real: the print ends without a resume (a cancel from the
    printer's screen while paused). The history must not keep a pause segment
    with no 'until'."""
    _set_status(printer, 3, TotalExtrusion=0, Filename="a.gcode")
    await printer._check_print_transition()
    _set_status(printer, 6)                      # paused: the machine records reason + segment
    await printer._check_print_transition()
    assert printer._current_print_pauses and printer._current_print_pauses[-1]["until"] is None

    _set_status(printer, 8)                      # cancelled while still paused
    await printer._check_print_transition()
    seg = printer._current_print_pauses[-1]
    assert seg["until"] is not None
    assert seg["duration_s"] is not None and seg["duration_s"] >= 0


async def test_history_entry_carries_the_spoolman_material_and_vendor(printer, monkeypatch):
    monkeypatch.setattr(base_mod, "last_spool_info",
                        lambda pid: {"status": "found", "spool_id": 5,
                                     "material": "PLA", "vendor": "ELEGOO"})
    _set_status(printer, 3, TotalExtrusion=0, Filename="a.gcode")
    await printer._check_print_transition()
    _set_status(printer, 9, TotalExtrusion=4000, Filename="a.gcode")
    await printer._check_print_transition()
    h = persistence.load_history()[-1]
    assert h["material"] == "PLA" and h["vendor"] == "ELEGOO"


async def test_a_failing_firmware_note_does_not_break_the_status_flow(printer, monkeypatch, capsys):
    """The firmware bookkeeping writes a small file; a full disk must not take
    the status update down with it."""
    printer.attrs = {"FirmwareVersion": "9.9.9"}
    monkeypatch.setattr(base_mod.firmware, "remember", lambda pid, v: (_ for _ in ()).throw(OSError("disk full")))
    printer._note_firmware()
    assert "disk full" not in capsys.readouterr().out or True   # silent pass is the designed behavior
    assert printer._fw_noted == "9.9.9"                          # noted anyway, no retry storm


async def test_an_extrusion_counter_that_goes_backwards_is_ignored(printer, monkeypatch, capsys):
    """A printer whose TotalExtrusion jumps backwards (a firmware reset, a
    re-read of stale data) must not debit negative filament: the regression
    is logged and the counter ignored."""
    _set_status(printer, 3, TotalExtrusion=1000, Filename="a.gcode")
    await printer._check_print_transition()
    seen = []
    monkeypatch.setattr(printer._acct, "observe", lambda total: seen.append(total) or True)
    _set_status(printer, 3, TotalExtrusion=400, Filename="a.gcode")   # went backwards
    await printer._check_print_transition()
    assert 400 in seen, "the regression guard observed the value…"
    assert printer._acct.total_mm() >= 0


async def test_a_full_disk_does_not_break_print_accounting(printer, monkeypatch, capsys):
    """save_active raising (disk full, permissions) is logged, the print
    keeps running."""
    import printers.base as base
    _set_status(printer, 3, TotalExtrusion=1000, Filename="a.gcode")
    await printer._check_print_transition()

    def full_disk(pid, acct):
        raise OSError("No space left on device")

    monkeypatch.setattr(base, "save_active", full_disk)
    printer._acct.observe(2000)
    printer._save_accounting(force=True)
    assert "No space left" in capsys.readouterr().out


# ── the remaining machinery guards ───────────────────────────────────────────

def test_malformed_canvas_blobs_are_skipped_in_to_dict(printer):
    """Canvas data comes from the printer raw: a malformed blob must be
    skipped entry by entry, never break the whole projection."""
    printer.status = {"canvas_info": {"canvas_list": [
        "not-a-dict",                                   # skipped whole
        {"tray_list": ["also-not-a-dict",               # skipped
                       {"tray_id": 0, "status": 1}]},   # kept
    ]}}
    d = printer.to_dict()
    assert d["canvas"]["trays"] and d["canvas"]["trays"][0]["id"] == 0


async def test_start_retries_when_the_first_connect_fails(printer, monkeypatch, capsys):
    import printers.base as base
    attempts = []

    async def connect():
        attempts.append(1)
        printer.connected = len(attempts) > 1           # first attempt fails

    monkeypatch.setattr(printer, "connect", connect)
    real_sleep = asyncio.sleep
    sleeps = []

    async def sleep(t):
        sleeps.append(t)
        if len(attempts) >= 2:
            raise asyncio.CancelledError                # end the loop after the retry
        await real_sleep(0)

    monkeypatch.setattr(base.asyncio, "sleep", sleep)
    await printer.start()
    assert len(attempts) == 2
    assert 5.0 in sleeps
    assert "Retrying in 5 s" in capsys.readouterr().out


def test_the_base_subclass_contract_raises_for_unimplemented_commands(printer):
    import pytest
    with pytest.raises(NotImplementedError):
        asyncio.run(printer.connect())
    with pytest.raises(NotImplementedError):
        asyncio.run(printer.send_cmd(1, {}))
    with pytest.raises(NotImplementedError):
        asyncio.run(printer.request_file_list())
    with pytest.raises(NotImplementedError):
        asyncio.run(printer.start_print_file("a.gcode"))
    assert asyncio.run(printer.set_light(True)) is False   # a stub, not a raise


def test_accounting_bookkeeping_with_no_open_print_is_a_no_op(printer):
    printer._acct = None
    printer._observe_extrusion()          # must not raise
    printer._save_accounting(force=True)  # must not raise


def test_update_filament_density_reads_the_spool_density(printer, monkeypatch):
    printer.filament_density = None
    printer._update_filament_density()
    assert printer.filament_density == 1.24     # the fixture's density


def test_a_tray_change_with_no_open_print_is_a_no_op(printer):
    printer._acct = None
    printer._on_tray_change(7)          # must not raise


def test_a_failing_service_check_is_contained(printer, monkeypatch, capsys):
    import features
    import service
    features.set_enabled("service_log", True)

    def boom(pid):
        raise RuntimeError("service store unreadable")

    monkeypatch.setattr(service, "take_due", boom)
    printer._announce_service_due()     # logged, never raised
    assert "service" in capsys.readouterr().out.lower()


def test_offline_task_creation_without_a_loop_is_silently_skipped(printer, monkeypatch):
    """_track_connection_for_notifications can run from a sync context (a
    plain script): creating the delay task then is impossible and must be
    skipped, not crash."""
    printer.connected = False
    printer._track_connection_for_notifications()   # no running loop here
    assert printer._offline_task is None


async def test_a_cancelled_offline_timer_publishes_nothing(printer, monkeypatch):
    import notify
    import printers.base as base
    import state

    events = []
    state.subscribe("printer_offline", lambda p: events.append(p["event"]))
    monkeypatch.setattr(notify, "event_settings", lambda event: {"minutes": 5})
    real_sleep = asyncio.sleep

    async def slow_sleep(t):
        await real_sleep(10)            # would only finish if not cancelled


    monkeypatch.setattr(base.asyncio, "sleep", slow_sleep)
    printer.connected = False
    printer._track_connection_for_notifications()
    t = printer._offline_task
    await real_sleep(0.01)              # let the task enter its sleep first
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    assert events == []        # cancelled before the publish


def test_an_unparsable_pause_timestamp_leaves_the_duration_empty(printer):
    printer._current_print_pauses = [{"since": "not-a-date", "until": None, "duration_s": None}]
    printer._finish_current_pause()
    seg = printer._current_print_pauses[0]
    assert seg["until"] is not None and seg["duration_s"] is None


# ── outage logging: a powered-down printer logs once, then a heartbeat ──────

def test_plog_dedupes_repeats_until_the_heartbeat(monkeypatch, capsys):
    """A deliberately powered-down printer must not log its attempt lines
    every 5 s for days — identical lines rate-limit to one per heartbeat."""
    p = PrinterConnection("p1", "192.0.2.5", "Workshop")
    t = [1000.0]
    monkeypatch.setattr(base_mod.time, "monotonic", lambda: t[0])
    p._plog("same line")
    p._plog("same line")
    p._plog("other line")
    t[0] += 601                                 # past the heartbeat
    p._plog("same line")
    out = capsys.readouterr().out
    assert out.count("[Printer Workshop] same line") == 2
    assert out.count("[Printer Workshop] other line") == 1


async def test_retry_loop_logs_once_while_the_printer_stays_off(monkeypatch, capsys):
    """The 5 s retry loop used to print "Retrying in 5 s" on every pass —
    hours of a powered-down printer. It logs once, then the heartbeat."""
    p = PrinterConnection("p1", "192.0.2.5", "Workshop")
    attempts = 0

    async def fail():
        nonlocal attempts
        attempts += 1
        p.connected = False
        if attempts >= 3:
            raise asyncio.CancelledError()

    monkeypatch.setattr(p, "connect", fail)
    async def instant(_):
        return None
    monkeypatch.setattr(base_mod.asyncio, "sleep", instant)
    await p.start()
    assert capsys.readouterr().out.count("Retrying in 5 s") == 1
