"""
One place every notification goes through.

Callers describe *what happened* (a Notification); this module decides
whether it should be sent (master switch, per-event setting, spam guard),
then hands it to every active channel on a worker thread so a slow or dead
service can never stall printer monitoring or the other channels.
"""

import concurrent.futures
import threading
import uuid
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import TypedDict

import notifiers
import push
import state
from features import is_enabled
from push import load_notif_settings

# event -> key in notification_settings.json that switches it on/off
EVENT_SETTING = {
    "print_started":   "started",
    "print_complete":  "finished",
    "print_cancelled": "finished",
    "print_paused":    "paused",
    "print_error":     "error",
    "printer_fault":   "error",
    "filament_runout": "filament_runout",
    "printer_offline": "offline",
    "printer_online":  "offline",
    "firmware_changed": "firmware",
    "nozzle_hot_idle": "nozzle_idle",
    "nozzle_overheat": "nozzle_printing",
    "layer_reached":   "layer",
    "spool_low":       "spool_low",
    "service_due":     "service",
    "spaghetti":       "spaghetti",
}
# Events that may carry a camera picture (when their setting asks for one).
IMAGE_EVENTS = {"print_started", "print_complete", "print_cancelled", "print_paused",
                "print_error", "printer_fault", "filament_runout", "layer_reached", "spaghetti"}
# Never suppressed by the spam guard: each one is a distinct, important moment.
ALWAYS_SEND = {"print_complete", "print_cancelled", "print_error", "printer_fault", "spaghetti", "test"}
MIN_INTERVAL_S = 10 * 60

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="notify")
_last_sent: dict = {}
_lock = threading.Lock()


# ── message registry (ADR-0002) ──────────────────────────────────────────────
# The only place prose exists for printer events. Templates are data: a
# future translation is a second dict plus a language selector, nothing
# else. {printer} is the printer's name; other fields are the event's data
# facts. Where the old inline prose had a fallback ("A print has started."
# when no filename), the entry is a small function — the literals inside it
# are still translatable strings.

def _reason_text(reason: dict) -> str:
    parts = []
    if reason.get("message"):
        parts.append(reason["message"])
    if reason.get("code") not in (None, ""):
        parts.append(f"(code {reason['code']})")
    if reason.get("action"):
        parts.append(reason["action"])
    return " ".join(parts)


class _Message(TypedDict):
    """One prose-registry entry: title is a format string, body either a
    format string or a callable of the event data (ADR-0002)."""
    title: str
    body: "str | Callable[[dict], str]"


MESSAGES: dict[str, _Message] = {
    "print_started":   {"title": "{printer} — Print started",
                        "body": lambda d: d.get("filename") or "A print has started."},
    "print_complete":  {"title": "{printer} — Print complete",
                        "body": lambda d: (f"{d['filename']} is done." if d.get("filename")
                                           else "Your print is complete.")},
    "print_cancelled": {"title": "{printer} — Print cancelled",
                        "body": lambda d: (f"{d['filename']} was cancelled." if d.get("filename")
                                           else "Your print was cancelled.")},
    "print_error":     {"title": "{printer} — Print error",
                        "body": lambda d: _reason_text(d.get("reason") or {}) or "The printer reported an error."},
    "print_paused":    {"title": "{printer} — Print paused",
                        "body": lambda d: _reason_text(d.get("reason") or {}) or "The print was paused."},
    "filament_runout": {"title": "{printer} — Filament ran out",
                        "body": lambda d: _reason_text(d.get("reason") or {}) or "Load filament and resume."},
    "printer_fault":   {"title": "{printer} — Printer fault",
                        "body": lambda d: _reason_text(d.get("reason") or {}) or "The printer reported a fault."},
    "printer_online":  {"title": "{printer} — Printer online",
                        "body": "The printer is connected again."},
    "printer_offline": {"title": "{printer} — Printer offline",
                        "body": "No contact with the printer for {minutes:g} minutes."},
    "nozzle_hot_idle": {"title": "{printer} — Nozzle hot",
                        "body": lambda d: f"Nozzle is {round(d['temp'])}°C while idle."},
    "nozzle_overheat": {"title": "{printer} — Nozzle overheat",
                        "body": lambda d: f"Nozzle is {round(d['temp'])}°C during print."},
    "layer_reached":   {"title": "{printer} — Layer {target} reached",
                        "body": "Currently on layer {layer}."},
    "firmware_changed": {"title": "{printer} — Firmware changed",
                         "body": lambda d: (f"{d['from']} → {d['to']}."
                                            + (" It has not been tested with this version of Spooler."
                                               if d.get("tested") is False else ""))},
    "service_due":     {"title": "{printer} — Service due: {task}",
                        "body": "{since:g} {unit} since last time (every {interval} {unit})."},
    "spaghetti":       {"title": "{printer} — Possible print failure",
                        "body": "The detector thinks this print may be failing (spaghetti). Check the printer."},
}


def _render(event: str, printer_name: str, data: dict) -> tuple[str, str]:
    m = MESSAGES[event]
    fields = {"printer": printer_name, **data}
    title = m["title"].format(**fields)
    body = m["body"](data) if callable(m["body"]) else m["body"].format(**fields)
    return title, body


# ── bus subscriber (ADR-0002): the notification channel reads the events ─────

async def _on_event(payload: dict) -> None:
    event = payload["event"]
    if not is_enabled("notifications") or not is_event_on(event):
        return
    data = dict(payload["data"])
    pr = state.printers.get(payload["printer_id"])
    name = (((payload.get("printer") or {}).get("name")) or getattr(pr, "name", "")
            or payload["printer_id"])
    image = None
    if wants_image(event) and pr is not None:
        image = await pr._camera_picture()
    title, body = _render(event, name, data)
    notify(Notification(event, payload["printer_id"], title, body, image,
                        data.pop("priority", "default"), name, data))


def wire() -> None:
    """Subscribe the notification channel to every event it renders.
    Called from channels.wire() at startup."""
    for event in MESSAGES:
        state.subscribe(event, _on_event)


@dataclass
class Notification:
    event: str
    printer_id: str
    title: str
    body: str = ""
    image: bytes | None = None
    priority: str = "default"          # low | default | high | urgent
    printer_name: str = ""
    extra: dict = field(default_factory=dict)
    time: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    # One id per event (not per channel), so a receiver can spot the same event twice.
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))


def event_settings(event: str) -> dict:
    key = EVENT_SETTING.get(event)
    return (load_notif_settings().get(key) or {}) if key else {}


def shared_on(event: str) -> bool:
    """Switched on in the shared settings, which drive the shared channels
    (ntfy, Telegram, Discord, webhook, MQTT) and devices of users without own choices."""
    return bool(event_settings(event).get("enabled"))


def key_wanted(key: str) -> bool:
    """Some recipient wants this setting key: the shared settings or any user's own choices."""
    return bool((load_notif_settings().get(key) or {}).get("enabled")) or push.any_user_wants(key)


def is_event_on(event: str) -> bool:
    """Cheap pre-check (before a camera picture is taken): master switch on and
    someone wants this event. Doesn't touch the spam guard."""
    key = EVENT_SETTING.get(event)
    return is_enabled("notifications") and key is not None and key_wanted(key)


def wants_image(event: str) -> bool:
    return event in IMAGE_EVENTS and bool(event_settings(event).get("image"))


def should_send(n: Notification) -> bool:
    if not is_enabled("notifications"):
        return False
    if n.event != "test" and not is_event_on(n.event):
        return False
    if n.event not in ALWAYS_SEND:
        key = (n.printer_id, n.event)
        now = time.monotonic()
        with _lock:
            last = _last_sent.get(key)
            if last is not None and now - last < MIN_INTERVAL_S:
                return False
            _last_sent[key] = now
    return True


def _deliver(n: Notification, channels: list) -> None:
    """`channels`: shared channel names, or (label, channel, settings lookup)
    for a user's own channel."""
    for ch in channels:
        label, name, get = ch if isinstance(ch, tuple) else (ch, ch, None)
        try:
            if get is None:
                notifiers.send(name, n)
            else:
                notifiers.send(name, n, get)
        except notifiers.NotifierError as e:
            print(f"[Notify] {label} failed: {e}")
        except Exception as e:  # a bug in one channel must not hide the others
            print(f"[Notify] {label} error: {type(e).__name__}")


def _user_channels(n: Notification) -> list:
    """Users' own channels that should get `n`: the user wants this event,
    may see the printer and is still allowed to have own channels."""
    import user_channels
    import users
    key = EVENT_SETTING.get(n.event)
    shared = load_notif_settings()
    out = []
    for user in user_channels.users_with_channels():
        if not users.has_permission(user, user_channels.PERMISSION):
            continue
        if not push._may_receive(user, key, n.printer_id, shared):
            continue
        out += [(f"{user}/{name}", name, get) for name, get in user_channels.active(user)]
    return out


def notify(n: Notification) -> bool:
    """Queue `n` for every active channel. Returns False when it was filtered
    out (or nothing is listening), True when it was handed to the worker."""
    if not should_send(n):
        return False
    channels = notifiers.active_channels()
    if n.event != "test" and not shared_on(n.event):
        # Only some users' own choices want it: just their devices and channels.
        channels = [c for c in channels if c == "webpush"]
    if n.event != "test":
        channels = channels + _user_channels(n)
    if not channels:
        return False
    names = [c[0] if isinstance(c, tuple) else c for c in channels]
    print(f"[Notify] {n.event} ({n.printer_name or n.printer_id}) -> {', '.join(names)}")
    _executor.submit(_deliver, n, channels)
    return True


def send_test(channel: str, get=None) -> None:
    """Send a test message through one channel right now (not queued), so the
    caller can report success or the readable failure. Ignores event settings."""
    n = Notification(event="test", printer_id="", title="Spooler — test notification",
                     body="If you can read this, this channel works.")
    notifiers.send(channel, n, get)


def reset_spam_guard() -> None:
    with _lock:
        _last_sent.clear()
