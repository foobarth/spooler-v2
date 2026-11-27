"""state.py's event bus — the machinery contract (ADR-0002).

Printers publish; channels subscribe. publish() is fire-and-forget with
per-subscriber error isolation; publish_now() is awaited, sequential and
error-isolated (the machine-coupled mode the queue-end uses). These tests pin
both modes' promises through the public seam.

conftest resets the subscriptions and wires the real channels for every test,
so custom event names here are hermetic, and the one integration test below
runs against the real production wiring.
"""


import asyncio
import json
import time

import state


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


# ── publish: fire-and-forget ─────────────────────────────────────────────────

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


async def test_publish_does_not_run_subscribers_before_the_first_yield():
    ran = []

    async def slow(payload):
        await asyncio.sleep(0.05)
        ran.append("slow")

    state.subscribe("evt", slow)
    state.publish("evt")
    assert ran == [], "fire-and-forget: publish returns before subscribers run"
    await asyncio.sleep(0.1)
    assert ran == ["slow"], "but every subscriber is delivered"


async def test_publish_snapshots_its_subscribers_at_call_time():
    # a subscriber added AFTER the publish misses that event
    ran = []

    async def cb(payload):
        ran.append(payload["data"]["n"])

    state.subscribe("evt", cb)
    state.publish("evt", data={"n": 1})
    await asyncio.sleep(0.02)
    assert ran == [1]


async def test_publish_without_subscribers_is_a_no_op():
    # an event nobody subscribed to: no dispatch, no error
    state.publish("nobody_listens", printer_id="p1")
    await asyncio.sleep(0.01)          # would surface as a task error otherwise


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


def test_publish_without_any_loop_falls_back_to_inline_dispatch():
    # sync context, no remembered loop: subscribers still get the event,
    # run to completion on a throwaway loop (the old _emit fallback's
    # delivery guarantee)
    got = []
    state.subscribe("print_started", lambda p: got.append(p["event"]))
    state.publish("print_started", printer_id="p1")
    assert got == ["print_started"]


# ── publish_now: awaited, sequential, error-isolated ─────────────────────────

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


async def test_publish_now_runs_subscribers_in_subscription_order():
    order = []

    async def first(payload):
        order.append("first")

    async def second(payload):
        order.append("second")

    state.subscribe("evt", first)
    state.subscribe("evt", second)
    await state.publish_now("evt")
    assert order == ["first", "second"]


async def test_publish_now_isolates_a_failing_subscriber(capsys):
    ran = []

    async def bad(payload):
        raise RuntimeError("boom")

    async def good(payload):
        ran.append("good")

    state.subscribe("evt", bad)
    state.subscribe("evt", good)
    await state.publish_now("evt")          # must not raise
    assert ran == ["good"], "the failure must not stop the remaining subscribers"
    assert "boom" in capsys.readouterr().out


async def test_publish_now_works_with_sync_subscribers():
    ran = []

    def sync_cb(payload):
        ran.append("sync")

    state.subscribe("evt", sync_cb)
    await state.publish_now("evt")
    assert ran == ["sync"]


# ── the awaited mode through the REAL wiring (why it exists) ─────────────────

async def test_queue_end_runs_to_completion_before_a_later_subscriber(tmp_path, monkeypatch):
    """The awaited mode's reason to exist: on print_ended the queue must see
    the closed job before the print-lifecycle machinery continues (the history
    entry). A subscriber added after the startup wiring therefore observes the
    queue ALREADY closed when it runs — publish_now awaited it."""
    from test_job_queue import FakePrinter, _gcode
    import features
    import job_queue

    monkeypatch.setattr(state, "printers", {})
    features.set_enabled("job_queue", True)
    state.printers["p1"] = FakePrinter("p1")
    job_queue.add_job(_gcode(tmp_path), "a.gcode", {})
    job_queue.assign_ready_jobs()
    await job_queue.confirm_plate_cleared("p1")
    job_queue.print_started("p1")
    assert job_queue.list_jobs()[0]["status"] == "printing"

    seen = []

    async def late_subscriber(payload):
        # wired AFTER the queue: the awaited mode guarantees the queue
        # finished handling this exact event before this line runs
        seen.append(job_queue.list_jobs()[0]["status"])

    state.subscribe("print_ended", late_subscriber)
    await state.publish_now("print_ended", printer_id="p1",
                            data={"end_state": "complete", "error": None})
    assert seen == ["done"]


# ── browser visibility (_client_can_see) ─────────────────────────────────────

class _WSClient:
    """Stand-in for a ws_handler browser client: the user is stashed on the
    socket at connect time ('' = login off / single admin)."""

    def __init__(self, user):
        self._spooler_user = user


def test_a_client_without_a_user_stash_sees_everything():
    class NoUser:
        pass

    assert state._client_can_see(NoUser(), "p1") is True


def test_an_empty_user_sees_every_printer():
    assert state._client_can_see(_WSClient(""), "p1") is True


def test_a_user_only_sees_printers_the_permission_says(monkeypatch):
    import users

    monkeypatch.setattr(users, "can_see_printer", lambda user, pid, all_ids=None: pid == "p1")
    assert state._client_can_see(_WSClient("ralf"), "p1") is True
    assert state._client_can_see(_WSClient("ralf"), "p2") is False


def test_a_permission_check_failure_fails_open(monkeypatch):
    import users

    def boom(*a, **kw):
        raise RuntimeError("permission store unreadable")

    monkeypatch.setattr(users, "can_see_printer", boom)
    assert state._client_can_see(_WSClient("ralf"), "p1") is True


# ── broadcast_to_browsers: visibility filter + dead-client pruning ───────────

class _Browser:
    """Stand-in for a ws_handler browser client socket."""

    def __init__(self, user="", fail=False):
        self._spooler_user = user
        self.fail = fail
        self.sent = []

    async def send(self, data):
        if self.fail:
            raise RuntimeError("socket closed")
        self.sent.append(data)


async def test_broadcast_to_browsers_with_no_clients_is_a_no_op():
    state.browser_clients.clear()
    await state.broadcast_to_browsers({"type": "x"})          # must not raise
    assert state.browser_clients == set()


async def test_broadcast_reaches_every_client_and_extracts_the_printer_id():
    a, b = _Browser(), _Browser()
    state.browser_clients.update({a, b})
    # the printer id may ride inside the printer dict rather than the message
    await state.broadcast_to_browsers({"type": "status", "printer": {"id": "p1", "name": "Elli"}})
    assert len(a.sent) == 1 and len(b.sent) == 1
    assert json.loads(a.sent[0])["printer"]["id"] == "p1"


async def test_broadcast_skips_clients_that_may_not_see_the_printer(monkeypatch):
    import users

    monkeypatch.setattr(users, "can_see_printer", lambda user, pid, all_ids=None: pid == "p1")
    admin, restricted = _Browser(user=""), _Browser(user="ralf")   # '' sees everything
    state.browser_clients.update({admin, restricted})
    await state.broadcast_to_browsers({"type": "status", "printer_id": "p1"})
    await state.broadcast_to_browsers({"type": "status", "printer_id": "p2"})
    assert len(admin.sent) == 2                                # sees both
    assert [json.loads(m)["printer_id"] for m in restricted.sent] == ["p1"]


async def test_broadcast_prunes_dead_clients_but_serves_the_rest():
    dead, alive = _Browser(fail=True), _Browser()
    state.browser_clients.update({dead, alive})
    await state.broadcast_to_browsers({"type": "status", "printer_id": "p1"})
    assert dead not in state.browser_clients, "a dead socket must be pruned"
    assert len(alive.sent) == 1
