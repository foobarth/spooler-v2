"""channels.py — enumerates the event-bus channels and wires them at startup.

The inverse of the old base.py call-out list: the core no longer names
channels, this module does. Registration happens only here, never at import
time, so tests reset cleanly. Each subscriber translates the event envelope
(docs/adr-event-bus.md) into its channel's own call; channels that need live
printer objects look them up via state.printers.
"""

import config
import asyncio
import dataset
import features
import job_queue
import mqtt_bridge
import state


def _wire_notify() -> None:
    import notify
    notify.wire()


def wire() -> None:
    """(Re)build the channel subscriptions from scratch. Safe to call again:
    the subscription set is reset first."""
    state.reset_subscriptions()
    _wire_dataset()
    _wire_job_queue()
    _wire_auto_light()
    _wire_notify()
    _wire_mqtt_bridge()


_bridge_hook_removers: list = []     # this channel's listeners, taken back on re-wire


def _wire_mqtt_bridge() -> None:
    """The MQTT bridge is a channel like the others: it subscribes instead of
    being called from the printer core (ADR-0001 + ADR-0002). Its lifecycle
    is the bridge's own single owner (docs/adr-mqtt-bridge.md "Lifecycle v2"):
    startup and every feature-flag or broker-URL change just poke it via
    sync_lifecycle(); the owner re-evaluates desired state and starts, moves
    or stops-and-clears by itself. Off means off."""
    async def status(payload: dict) -> None:
        await mqtt_bridge.on_status(payload)

    state.subscribe("status_update", status)

    _lifecycle_tasks: set = set()    # strong refs, like state's _dispatched

    def schedule() -> None:
        """Run one lifecycle sync wherever we are: on the running loop, on the
        remembered main loop (sync or thread contexts), or inline on a
        throwaway loop when no loop exists — same fallback chain as
        state.publish(). Never dropped silently."""
        coro = mqtt_bridge.sync_lifecycle()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None:
            task = loop.create_task(coro)
            _lifecycle_tasks.add(task)
            task.add_done_callback(_lifecycle_tasks.discard)
            return
        ml = state.main_loop()
        if ml is not None:
            asyncio.run_coroutine_threadsafe(coro, ml)
            return
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    for r in _bridge_hook_removers:
        r()
    _bridge_hook_removers.clear()
    _bridge_hook_removers.append(features.on_change("mqtt_integration", lambda enabled: schedule()))
    _bridge_hook_removers.append(config.on_change("mqtt.", lambda key, value: schedule()))
    schedule()      # startup


def _wire_dataset() -> None:
    async def before_print(payload: dict) -> None:
        pr = state.printers.get(payload["printer_id"])
        if not pr:
            return
        pi = ((payload.get("printer") or {}).get("status") or {}).get("PrintInfo") or {}
        await dataset.capture_before_print(pr, pi)

    state.subscribe("print_started", before_print)


def _wire_job_queue() -> None:
    async def started(payload: dict) -> None:
        job_queue.print_started(payload["printer_id"])

    async def ended(payload: dict) -> None:
        data = payload["data"]
        job_queue.print_ended(payload["printer_id"], data["end_state"], data.get("error"))

    state.subscribe("print_started", started)
    state.subscribe("print_ended", ended)


def _wire_auto_light() -> None:
    # Light-ON only. The OFF side stays inline in the machine: it is
    # sequenced with the end-of-print picture (the light must still be on
    # for the shot) and with orphaned-print recovery (ADR-0002, Principle).
    async def light_on(payload: dict) -> None:
        pr = state.printers.get(payload["printer_id"])
        if pr:
            await pr._auto_light(True)

    state.subscribe("print_started", light_on)
