"""buttons.py — admin-defined webhook buttons: validation, storage, firing."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import buttons
import control


# ── Loopback HTTP server: records what Spooler sent, never touches network ──
class _Recorder(BaseHTTPRequestHandler):
    last = None
    response = (200, b'{"ok":true}')

    def _handle(self):
        length = int(self.headers.get("Content-Length") or 0)
        _Recorder.last = {
            "method": self.command, "path": self.path,
            "body": self.rfile.read(length).decode() if length else None,
            "content_type": self.headers.get("Content-Type"),
        }
        status, payload = _Recorder.response
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(payload)

    do_GET = do_POST = _handle

    def log_message(self, *a):   # silence request logging
        pass


class _StatePrinter:
    """Minimal fake of what control.display_state() needs."""

    def __init__(self, pid="p1", status_code=0, connected=True):
        self.id, self.name, self.connected = pid, f"Printer {pid}", connected
        self._code = status_code

    def _decoded_printinfo(self):
        return {"Status": self._code}

    def _is_busy_between_prints(self):
        return False


@pytest.fixture()
def loopback():
    server = HTTPServer(("127.0.0.1", 0), _Recorder)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/api/webhook/x"
    server.shutdown()
    _Recorder.last = None
    _Recorder.response = (200, b'{"ok":true}')


class TestFiring:
    def test_fire_posts_body_to_loopback(self, loopback):
        b = buttons.add_button("Power", loopback, body='{"plug":"on"}')
        printer = _StatePrinter()
        res = buttons.fire_button(b, printer, user="ralf")
        assert res["ok"] is True and res["status"] == 200
        assert _Recorder.last["method"] == "POST"
        assert _Recorder.last["body"] == '{"plug":"on"}'
        assert _Recorder.last["content_type"] == "application/json"

    def test_fire_get_has_no_body(self, loopback):
        b = buttons.add_button("Ping", loopback, method="GET")
        res = buttons.fire_button(b, _StatePrinter(), user="ralf")
        assert res["ok"] is True
        assert _Recorder.last["method"] == "GET"
        assert _Recorder.last["body"] is None

    def test_fire_http_error_is_reported_not_raised(self, loopback):
        _Recorder.response = (500, b"boom")
        b = buttons.add_button("X", loopback)
        res = buttons.fire_button(b, _StatePrinter(), user="ralf")
        assert res["ok"] is False and res["status"] == 500

    def test_fire_dead_target_is_reported(self):
        b = buttons.add_button("X", "http://127.0.0.1:1/nope")
        res = buttons.fire_button(b, _StatePrinter(), user="ralf")
        assert res["ok"] is False and "error" in res

    def test_idle_guard_blocks_while_printing(self, loopback):
        b = buttons.add_button("Power", loopback, when="idle")
        printing = _StatePrinter(status_code=2)   # 2 = printing per base.py
        res = buttons.fire_button(b, printing, user="ralf")
        assert res["ok"] is False and "idle" in res["error"].lower()
        assert _Recorder.last is None             # nothing was sent

    def test_any_button_fires_while_printing(self, loopback):
        b = buttons.add_button("Speed", loopback, when="any")
        res = buttons.fire_button(b, _StatePrinter(status_code=2), user="ralf")
        assert res["ok"] is True

    def test_fire_offline_printer_is_fine_for_idle_buttons(self, loopback):
        # powering the plug ON while the printer is off is exactly the use case
        b = buttons.add_button("Power on", loopback, when="idle")
        off = _StatePrinter(connected=False)
        res = buttons.fire_button(b, off, user="ralf")
        assert res["ok"] is True


class TestAudit:
    def test_press_is_logged_without_the_url(self, loopback):
        secret_url = loopback + "?token=supersecret"
        b = buttons.add_button("Power", secret_url)
        buttons.fire_button(b, _StatePrinter(), user="ralf")
        entry = control.read_log()[0]
        assert entry["action"] == "webhook_button"
        assert entry["user"] == "ralf"
        assert entry["printer_id"] == "p1"
        assert "200" in entry["result"]
        assert "supersecret" not in json.dumps(entry)   # URL never logged

    def test_failed_press_is_logged_too(self):
        b = buttons.add_button("X", "http://127.0.0.1:1/nope")
        buttons.fire_button(b, _StatePrinter(), user="ralf")
        entry = control.read_log()[0]
        assert entry["action"] == "webhook_button"
        assert entry["result"] and "200" not in entry["result"]


# ── WS seam: dispatch through handle_browser_message ────────────────────────
import auth
import features
import state
import users
import ws_handler


class _WS:
    def __init__(self, user):
        self._spooler_user = user
        self.sent = []

    async def send(self, data):
        self.sent.append(json.loads(data))


@pytest.fixture()
def ws_env(monkeypatch):
    monkeypatch.setattr(auth, "AUTH_ENABLED", True)
    monkeypatch.setattr(ws_handler, "AUTH_ENABLED", True)
    users.ensure_initialised()
    users.create_user("boss", "secret1", "admin")
    users.create_user("watcher", "secret1", "viewer")
    p = _StatePrinter(pid="a")
    monkeypatch.setattr(state, "printers", {"a": p})
    monkeypatch.setattr(state, "tray_map", {})

    async def nobody(msg):
        pass
    monkeypatch.setattr(state, "broadcast_to_browsers", nobody)
    return p


async def _do(user, **msg):
    ws = _WS(user)
    await ws_handler.handle_browser_message(ws, json.dumps({"printer_id": "a", **msg}))
    return ws


class TestWsPermissions:
    async def test_viewer_cannot_run_a_button(self, ws_env, loopback):
        b = buttons.add_button("X", loopback)
        ws = await _do("watcher", action="run_button", button_id=b["id"])
        assert any("permission" in m.get("message", "") for m in ws.sent)

    async def test_admin_can_run_a_button(self, ws_env, loopback):
        b = buttons.add_button("X", loopback)
        ws = await _do("boss", action="run_button", button_id=b["id"])
        result = next(m for m in ws.sent if m.get("type") == "button_result")
        assert result["ok"] is True and result["status"] == 200

    async def test_unknown_button_is_an_error(self, ws_env):
        ws = await _do("boss", action="run_button", button_id="nope")
        assert any("Unknown button" in m.get("message", "") for m in ws.sent)

    async def test_scoped_button_refuses_other_printer(self, ws_env, loopback):
        b = buttons.add_button("X", loopback, printers=["other"])
        ws = await _do("boss", action="run_button", button_id=b["id"], printer_id="a")
        assert any("not for this printer" in m.get("message", "") for m in ws.sent)

    async def test_feature_off_refuses_firing(self, ws_env, monkeypatch, loopback):
        b = buttons.add_button("X", loopback)
        monkeypatch.setattr(features, "is_enabled", lambda key: False)
        ws = await _do("boss", action="run_button", button_id=b["id"])
        assert any("turned off" in m.get("message", "") for m in ws.sent)

    async def test_idle_button_without_printer_context_refuses(self, ws_env, loopback):
        b = buttons.add_button("X", loopback, when="idle", printers=["a"])
        ws = await _do("boss", action="run_button", button_id=b["id"], printer_id=None)
        assert any("printer context" in m.get("message", "") for m in ws.sent)


class TestWsConfig:
    async def test_viewer_sees_buttons_without_their_urls(self, ws_env, loopback):
        buttons.add_button("Power", loopback + "?token=secret")
        ws = await _do("watcher", action="list_buttons")
        listing = next(m for m in ws.sent if m.get("type") == "buttons")["buttons"]
        assert listing[0]["label"] == "Power"
        assert "token=secret" not in json.dumps(ws.sent)
        assert "url" not in listing[0]

    async def test_admin_sees_full_buttons(self, ws_env, loopback):
        buttons.add_button("Power", loopback + "?token=secret")
        ws = await _do("boss", action="list_buttons")
        listing = next(m for m in ws.sent if m.get("type") == "buttons")["buttons"]
        assert listing[0]["url"].endswith("token=secret")

    async def test_save_button_needs_settings_manage(self, ws_env, loopback):
        ws = await _do("watcher", action="save_button", label="X", url=loopback)
        assert any("permission" in m.get("message", "") for m in ws.sent)

    async def test_admin_can_save_and_invalid_save_reports_valueerror(self, ws_env):
        ws = await _do("boss", action="save_button", label="Ok", url="http://example.com/h")
        assert any(m.get("type") == "buttons_saved" for m in ws.sent)
        ws = await _do("boss", action="save_button", label="Bad", url="ftp://nope")
        assert any("URL must be" in m.get("message", "") for m in ws.sent)

    async def test_save_adds_when_no_id_updates_with_id(self, ws_env, loopback):
        b = buttons.add_button("Old", loopback)
        ws = await _do("boss", action="save_button", id=b["id"], label="New", url=loopback)
        saved = next(m for m in ws.sent if m.get("type") == "buttons_saved")["button"]
        assert saved["id"] == b["id"] and saved["label"] == "New"
        assert buttons.get_button(b["id"])["label"] == "New"

    async def test_delete_button(self, ws_env, loopback):
        b = buttons.add_button("X", loopback)
        ws = await _do("boss", action="delete_button", button_id=b["id"])
        assert buttons.get_button(b["id"]) is None
        assert any(m.get("type") == "info" and m.get("message") == "Button removed" for m in ws.sent)


class TestValidation:
    def test_add_button_stores_and_lists(self):
        buttons.add_button("Power printer", "http://ha.local:8123/api/webhook/abc123")
        got = buttons.list_buttons()
        assert len(got) == 1
        assert got[0]["label"] == "Power printer"
        assert got[0]["method"] == "POST"
        assert got[0]["when"] == "any"
        assert got[0]["printers"] == "*"

    def test_add_button_assigns_stable_ids(self):
        a = buttons.add_button("One", "http://example.com/a")
        b = buttons.add_button("Two", "http://example.com/b")
        assert a["id"] and b["id"] and a["id"] != b["id"]

    @pytest.mark.parametrize("url", [
        "ftp://example.com/x",           # wrong scheme
        "not a url",
        "",                              # empty
        "file:///etc/passwd",            # local scheme
        "https://" + "x" * 600,          # too long
    ])
    def test_add_button_rejects_bad_url(self, url):
        with pytest.raises(ValueError):
            buttons.add_button("X", url)

    def test_add_button_accepts_https(self):
        b = buttons.add_button("X", "https://example.com/hook")
        assert b["url"].startswith("https://")

    def test_add_button_rejects_bad_label(self):
        for label in ("", "   ", "x" * 41, None):
            with pytest.raises(ValueError):
                buttons.add_button(label, "http://example.com/x")

    def test_add_button_rejects_bad_method(self):
        with pytest.raises(ValueError):
            buttons.add_button("X", "http://example.com/x", method="DELETE")

    def test_add_button_get_rejects_body(self):
        with pytest.raises(ValueError):
            buttons.add_button("X", "http://example.com/x", method="GET", body='{"a":1}')

    def test_add_button_rejects_bad_when(self):
        with pytest.raises(ValueError):
            buttons.add_button("X", "http://example.com/x", when="sometimes")

    def test_add_button_rejects_bad_printers_value(self):
        with pytest.raises(ValueError):
            buttons.add_button("X", "http://example.com/x", printers=["not-a-dict", 42])

    def test_add_button_rejects_non_dict_headers(self):
        with pytest.raises(ValueError):
            buttons.add_button("X", "http://example.com/x", headers=["a", "b"])

    def test_button_count_is_capped(self):
        for i in range(buttons.MAX_BUTTONS):
            buttons.add_button(f"B{i}", "http://example.com/x")
        with pytest.raises(ValueError):
            buttons.add_button("Overflow", "http://example.com/x")

    def test_update_and_remove(self):
        b = buttons.add_button("Old", "http://example.com/x")
        buttons.update_button(b["id"], label="New", when="idle")
        got = buttons.list_buttons()[0]
        assert got["label"] == "New" and got["when"] == "idle"
        assert buttons.remove_button(b["id"]) is True
        assert buttons.list_buttons() == []
        assert buttons.remove_button("nope") is False

    def test_update_rejects_bad_values_too(self):
        b = buttons.add_button("X", "http://example.com/x")
        with pytest.raises(ValueError):
            buttons.update_button(b["id"], url="ftp://nope")

    def test_persisted_across_reload(self, tmp_path):
        # storage goes through BUTTONS_FILE, which conftest redirects per test
        buttons.add_button("Durable", "http://example.com/x")
        again = buttons.list_buttons()
        assert again and again[0]["label"] == "Durable"
