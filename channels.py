"""channels.py — enumerates the event-bus channels and wires them at startup.

The inverse of the old base.py call-out list: the core no longer names
channels, this module does. Registration happens only here, never at import
time, so tests reset cleanly. Each subscriber translates the event envelope
(docs/adr-event-bus.md) into its channel's own call; channels that need live
printer objects look them up via state.printers.
"""

import dataset
import state


def wire() -> None:
    """(Re)build the channel subscriptions from scratch. Safe to call again:
    the subscription set is reset first."""
    state.reset_subscriptions()
    _wire_dataset()


def _wire_dataset() -> None:
    async def before_print(payload: dict) -> None:
        pr = state.printers.get(payload["printer_id"])
        if not pr:
            return
        pi = ((payload.get("printer") or {}).get("status") or {}).get("PrintInfo") or {}
        await dataset.capture_before_print(pr, pi)

    state.subscribe("print_started", before_print)
