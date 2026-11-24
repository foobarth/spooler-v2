"""
Shared runtime state — imported by every other module.

Keeping this separate from the rest of the codebase breaks all potential
circular imports: nothing here imports from within this project.
"""

import asyncio
import json
import os
import time

# Set DEBUG=1 in the environment to enable verbose MQTT/protocol logging.
DEBUG: bool = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")

# Set once, the first time this module is imported — which happens very early
# in process startup — so (time.time() - START_TIME) is the server's uptime.
START_TIME: float = time.time()

# { printer_id: PrinterConnection }
printers: dict = {}

# Connected browser WebSocket clients
browser_clients: set = set()

# { printer_id: { str(tray_id): spoolman_spool_id } }
tray_map: dict = {}


# ── event bus ────────────────────────────────────────────────────────────────
# Printers publish; channels subscribe (docs/adr-event-bus.md). Fire-and-
# forget: every subscriber runs in its own task, a subscriber's exception is
# logged and never reaches the publisher or other subscribers, there is no
# ordering guarantee between subscribers, and nothing is retained. Channels
# register explicitly (channels.wire() at startup), never at import time.

_subscriptions: dict = {}   # event name -> [callback]
_dispatched: set = set()    # strong refs to in-flight dispatch tasks
_main_loop: asyncio.AbstractEventLoop | None = None


def subscribe(event: str, callback) -> None:
    """Register a channel callback for an event. Call from wire(), never
    at import time, so tests reset cleanly."""
    _subscriptions.setdefault(event, []).append(callback)


def reset_subscriptions() -> None:
    """Test isolation and re-wiring: forget every subscription."""
    _subscriptions.clear()


def remember_loop() -> None:
    """Record the main loop so publish() works from non-loop threads too.
    Called once from server startup, inside the running loop."""
    global _main_loop
    _main_loop = asyncio.get_running_loop()


def publish(event: str, printer_id: str = "", printer: dict | None = None,
            data: dict | None = None) -> None:
    """Fire-and-forget dispatch of one event to its subscribers. Sync-safe:
    called from sync code inside the event loop it returns immediately, and
    from other threads it hands the dispatch to the remembered loop."""
    payload = {"printer_id": printer_id, "event": event, "printer": printer,
               "data": data or {}}
    subs = list(_subscriptions.get(event, []))
    if not subs:
        return
    try:
        loop = asyncio.get_running_loop()
        for cb in subs:
            task = loop.create_task(_dispatch(cb, payload))
            _dispatched.add(task)
            task.add_done_callback(_dispatched.discard)
    except RuntimeError:
        if _main_loop is not None and not _main_loop.is_closed():
            for cb in subs:
                asyncio.run_coroutine_threadsafe(_dispatch(cb, payload), _main_loop)
            return
        # No loop anywhere (sync context: a plain script, a thread before the
        # server started, a sync test). Run subscribers to completion inline
        # on a throwaway loop — the delivery guarantee the old _emit fallback
        # gave. Never dropped silently.
        loop = asyncio.new_event_loop()
        try:
            for cb in subs:
                loop.run_until_complete(_dispatch(cb, payload))
        finally:
            loop.close()


async def publish_now(event: str, printer_id: str = "", printer: dict | None = None,
                      data: dict | None = None) -> None:
    """Awaited dispatch: subscribers run sequentially (subscription order),
    each error-isolated, and publish_now returns only when all have run.

    For machine-coupled consumers — where the state machine's next
    observable step depends on the subscriber having completed. Everything
    else uses publish() (ADR-0002: two dispatch modes)."""
    payload = {"printer_id": printer_id, "event": event, "printer": printer,
               "data": data or {}}
    for cb in list(_subscriptions.get(event, [])):
        await _dispatch(cb, payload)


async def _dispatch(cb, payload: dict) -> None:
    try:
        result = cb(payload)
        if asyncio.iscoroutine(result):
            await result
    except Exception as e:
        print(f"[Events] subscriber {getattr(cb, '__name__', repr(cb))} failed "
              f"on {payload['event']}: {type(e).__name__}: {e}")


def _client_can_see(client, printer_id: str) -> bool:
    """A browser WS client may only receive updates for printers its user can see.
    The user (or '' for login-off / single-admin) is stashed on the socket at
    connect time; '' sees everything."""
    user = getattr(client, "_spooler_user", "")
    if not user:
        return True
    try:
        import users
        return users.can_see_printer(user, printer_id)
    except Exception:
        return True


async def broadcast_to_browsers(msg: dict) -> None:
    if not browser_clients:
        return
    # Printer-specific messages are filtered per client by visibility.
    pid = msg.get("printer_id")
    if pid is None and isinstance(msg.get("printer"), dict):
        pid = msg["printer"].get("id")
    data = json.dumps(msg)
    dead = set()
    for client in list(browser_clients):
        try:
            if pid is not None and not _client_can_see(client, pid):
                continue
            await client.send(data)
        except Exception:
            dead.add(client)
    browser_clients.difference_update(dead)
