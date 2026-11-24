"""channels.py — enumerates the event-bus channels and wires them at startup.

The inverse of the old base.py call-out list: the core no longer names
channels, this module does. Registration happens only here, never at import
time, so tests reset cleanly. Each subscriber translates the event envelope
(docs/adr-event-bus.md) into its channel's own call; channels that need live
printer objects look them up via state.printers.
"""

import dataset
import job_queue
import state


def wire() -> None:
    """(Re)build the channel subscriptions from scratch. Safe to call again:
    the subscription set is reset first."""
    state.reset_subscriptions()
    _wire_dataset()
    _wire_job_queue()
    _wire_auto_light()


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
