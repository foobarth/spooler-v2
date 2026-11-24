"""state.py — internal event bus.

Printers publish; channels subscribe. See docs/adr-event-bus.md for the
catalog and semantics: fire-and-forget, per-subscriber error isolation,
no ordering guarantee between subscribers, registration explicit.
"""

import asyncio
import time

import pytest

import state


@pytest.fixture(autouse=True)
def clean_bus():
    state.reset_subscriptions()
    yield
    state.reset_subscriptions()


async def _wait_for(flag, timeout=2.0):
    async def _poll():
        while not flag():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), timeout)


# ── delivery ─────────────────────────────────────────────────────────────────

async def test_subscriber_receives_the_envelope():
    got = []
    state.subscribe("print_started", lambda p: got.append(p))
    state.publish("print_started", printer_id="p1", data={"filename": "benchy.gcode"})
    await _wait_for(lambda: bool(got))
    assert got == [{
        "printer_id": "p1",
        "event": "print_started",
        "printer": None,
        "data": {"filename": "benchy.gcode"},
    }]


async def test_only_matching_subscribers_get_the_event():
    got = []
    state.subscribe("print_started", lambda p: got.append("started"))
    state.subscribe("print_ended", lambda p: got.append("ended"))
    state.publish("print_ended", printer_id="p1")
    await _wait_for(lambda: bool(got))
    assert got == ["ended"]


async def test_multiple_subscribers_all_get_the_event():
    got = []
    state.subscribe("print_started", lambda p: got.append("a"))
    state.subscribe("print_started", lambda p: got.append("b"))
    state.publish("print_started", printer_id="p1")
    await _wait_for(lambda: len(got) == 2)
    assert sorted(got) == ["a", "b"]


# ── isolation ────────────────────────────────────────────────────────────────

async def test_exception_in_one_subscriber_never_reaches_others_or_the_publisher(capsys):
    got = []

    def bad(payload):
        raise RuntimeError("channel exploded")

    state.subscribe("print_started", bad)
    state.subscribe("print_started", lambda p: got.append("survivor"))
    state.publish("print_started", printer_id="p1")   # must not raise
    await _wait_for(lambda: bool(got))
    assert got == ["survivor"]
    assert "channel exploded" in capsys.readouterr().out    # logged, not swallowed


# ── fire-and-forget ──────────────────────────────────────────────────────────

async def test_publish_returns_before_slow_subscribers_finish():
    done = []

    async def slow(payload):
        await asyncio.sleep(0.2)
        done.append("finished")

    state.subscribe("print_started", slow)
    t0 = time.monotonic()
    state.publish("print_started", printer_id="p1")   # sync call, must not await
    assert time.monotonic() - t0 < 0.1                # returned immediately
    await _wait_for(lambda: bool(done))


async def test_async_and_sync_subscribers_both_run():
    got = []
    state.subscribe("print_started", lambda p: got.append("sync"))
    state.subscribe("print_started", lambda p: got.append("async") or asyncio.sleep(0))
    state.publish("print_started", printer_id="p1")
    await _wait_for(lambda: len(got) == 2)
    assert sorted(got) == ["async", "sync"]


# ── thread safety ────────────────────────────────────────────────────────────

async def test_publish_from_a_thread_uses_the_remembered_loop():
    got = []
    state.subscribe("print_started", lambda p: got.append("from-thread"))
    state.remember_loop()
    results = []
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: results.append(state.publish("print_started", printer_id="p1")))
    await _wait_for(lambda: bool(got))
    assert got == ["from-thread"] and results == [None]


def test_publish_without_a_loop_and_without_a_remembered_loop_logs_and_survives(capsys):
    # no asyncio loop running, remember_loop() never called: the event is
    # dropped with a loud log line, publish itself never raises
    state.subscribe("print_started", lambda p: pytest.fail("should not be called"))
    state.publish("print_started", printer_id="p1")
    assert "dropped" in capsys.readouterr().out


# ── awaited dispatch (machine-coupled consumers) ─────────────────────────────

async def test_publish_now_awaits_every_subscriber_before_returning():
    got = []

    async def slow(payload):
        await asyncio.sleep(0.05)
        got.append("done")

    state.subscribe("print_ended", slow)
    t0 = time.monotonic()
    await state.publish_now("print_ended", printer_id="p1", data={"end_state": "complete"})
    assert got == ["done"]
    assert time.monotonic() - t0 >= 0.05


async def test_publish_now_runs_subscribers_sequentially_in_subscription_order():
    order = []

    async def first(payload):
        order.append("first-start")
        await asyncio.sleep(0.02)
        order.append("first-end")

    def second(payload):
        order.append("second")

    state.subscribe("print_ended", first)
    state.subscribe("print_ended", second)
    await state.publish_now("print_ended", printer_id="p1")
    assert order == ["first-start", "first-end", "second"]


async def test_publish_now_isolates_errors_but_runs_the_remaining_subscribers(capsys):
    got = []

    def bad(payload):
        raise RuntimeError("exploded")

    state.subscribe("print_ended", bad)
    state.subscribe("print_ended", lambda p: got.append("survivor"))
    await state.publish_now("print_ended", printer_id="p1")     # must not raise
    assert got == ["survivor"]
    assert "exploded" in capsys.readouterr().out
