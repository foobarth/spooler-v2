"""Admin-defined webhook buttons: validation, storage, firing, audit.

A button is a named HTTP request the UI can fire with one tap — for example a
Home Assistant webhook that toggles the smart plug powering a printer. Admins
configure buttons under Settings (settings.manage permission); who may *press*
them is the separate buttons.trigger permission.

Storage: DATA_DIR/buttons.json (0600), same pattern as users.json.
"""

import json
import threading
import urllib.error
import urllib.request
import uuid

import control
from persistence import DATA_DIR, _atomic_write

FIRE_TIMEOUT = 10.0   # seconds; a webhook target that hangs must not hang the UI

BUTTONS_FILE = DATA_DIR / "buttons.json"
_lock = threading.RLock()

MAX_BUTTONS = 20
MAX_LABEL = 40
MAX_URL = 500
MAX_BODY = 4096
MAX_HEADERS = 8
ALLOWED_METHODS = ("GET", "POST")
METHODS_WITH_BODY = ("POST",)
WHEN_MODES = ("any", "idle")


def _load() -> list:
    try:
        data = json.loads(BUTTONS_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(buttons: list) -> None:
    _atomic_write(BUTTONS_FILE, json.dumps(buttons, indent=2), mode=0o600)


def _validate(label, url, method, body, headers, when, printers) -> dict:
    """Validate one button definition; returns the cleaned fields.
    Raises ValueError with a user-facing message."""
    if not isinstance(label, str) or not (clean := label.strip()) or len(clean) > MAX_LABEL:
        raise ValueError(f"Label must be 1–{MAX_LABEL} characters.")
    parsed_ok = False
    if isinstance(url, str) and len(url) <= MAX_URL:
        lower = url.lower()
        parsed_ok = lower.startswith(("http://", "https://")) and len(url) > len("https://")
    if not parsed_ok:
        raise ValueError("URL must be an http(s) address.")
    if method not in ALLOWED_METHODS:
        raise ValueError("Method must be GET or POST.")
    if body is not None:
        if method not in METHODS_WITH_BODY:
            raise ValueError("A body is only allowed with POST.")
        if not isinstance(body, str) or len(body) > MAX_BODY:
            raise ValueError(f"Body must be a string of up to {MAX_BODY} characters.")
    if headers is not None:
        if not isinstance(headers, dict) or len(headers) > MAX_HEADERS:
            raise ValueError(f"Headers must be an object with at most {MAX_HEADERS} entries.")
        for k, v in headers.items():
            if not isinstance(k, str) or not isinstance(v, str) or not k or len(k) > 64 or len(str(v)) > 256:
                raise ValueError("Header names and values must be short strings.")
    if when not in WHEN_MODES:
        raise ValueError("when must be \"any\" or \"idle\".")
    if printers != "*" and (not isinstance(printers, list) or not all(
            isinstance(x, str) and x for x in printers)):
        raise ValueError('printers must be "*" or a list of printer ids.')
    return {"label": clean, "url": url, "method": method, "body": body,
            "headers": headers or None, "when": when, "printers": printers}


def list_buttons() -> list:
    with _lock:
        return _load()


def add_button(label, url, *, method="POST", body=None, headers=None,
               when="any", confirm=False, printers="*") -> dict:
    with _lock:
        existing = _load()
        if len(existing) >= MAX_BUTTONS:
            raise ValueError(f"At most {MAX_BUTTONS} buttons.")
        clean = _validate(label, url, method, body, headers, when, printers)
        button = {"id": uuid.uuid4().hex[:12], "confirm": bool(confirm), **clean}
        existing.append(button)
        _save(existing)
        return button


def update_button(button_id: str, **changes) -> dict | None:
    with _lock:
        existing = _load()
        for i, b in enumerate(existing):
            if b.get("id") == button_id:
                merged = {**b, **{k: v for k, v in changes.items()}}
                clean = _validate(merged.get("label"), merged.get("url"),
                                  merged.get("method", "POST"), merged.get("body"),
                                  merged.get("headers"), merged.get("when", "any"),
                                  merged.get("printers", "*"))
                merged.update(clean)
                merged.pop("_id", None)
                existing[i] = merged
                _save(existing)
                return merged
    return None


def remove_button(button_id: str) -> bool:
    with _lock:
        existing = _load()
        kept = [b for b in existing if b.get("id") != button_id]
        if len(kept) == len(existing):
            return False
        _save(kept)
        return True


def get_button(button_id: str) -> dict | None:
    with _lock:
        for b in _load():
            if b.get("id") == button_id:
                return b
    return None


def public_view(button: dict) -> dict:
    """What a client without settings.manage may see: label and behaviour,
    but not the URL/body/headers — webhook addresses can carry tokens."""
    return {k: v for k, v in button.items() if k not in ("url", "body", "headers")}


def fire_button(button: dict, printer, user: str = "") -> dict:
    """Send the button's webhook. Never raises; returns {ok, status?, error?}.

    Blocking (urllib) — callers on the event loop wrap this in an executor,
    the same way prusa.py and spaghetti.py do their HTTP.
    """
    # idle guard first: a power button must not cut a running print
    if button.get("when") == "idle":
        state = control.display_state(printer)
        if state in control.BUSY_STATES:
            res = {"ok": False, "error": f"The printer is busy ({state}) — "
                    f"\"{button.get('label')}\" only runs while idle."}
            _audit(user, printer, button, res)
            return res

    method = button.get("method", "POST")
    body = button.get("body")
    headers = dict(button.get("headers") or {})
    if body and "Content-Type" not in {k.title() for k in headers}:
        headers["Content-Type"] = "application/json"
    try:
        req = urllib.request.Request(
            button["url"], data=body.encode() if body is not None else None,
            headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=FIRE_TIMEOUT) as resp:
            status = resp.status
    except urllib.error.HTTPError as e:
        res = {"ok": False, "status": e.code, "error": f"The target answered HTTP {e.code}"}
        _audit(user, printer, button, res)
        return res
    except Exception as e:
        res = {"ok": False, "error": f"Could not reach the target ({type(e).__name__}: {e})"}
        _audit(user, printer, button, res)
        return res

    res = {"ok": 200 <= status < 300, "status": status}
    if not res["ok"]:
        res["error"] = f"The target answered HTTP {status}"
    _audit(user, printer, button, res)
    return res


def _audit(user: str, printer, button: dict, res: dict) -> None:
    """One control-log line per press. The URL is deliberately not logged —
    webhook addresses can carry tokens."""
    if printer is None:   # unscoped button, fired from the menu
        printer = type("NoPrinter", (), {"id": "", "name": ""})()
    result = f"HTTP {res['status']}" if res.get("ok") else res.get("error", "failed")
    control.log_command(user, printer, "webhook_button",
                        {"button": button.get("label"), "method": button.get("method", "POST")},
                        result)
