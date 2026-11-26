"""External camera token: ?token= grants GET access to /api/camera/<id> and
/api/snapshot/<id> only — image URLs for Home Assistant, where no browser
session exists. Empty token = off; wrong tokens count against a brute-force
lockout; regenerating the token revokes the old one. Everything else (every
other path, every non-GET method) ignores the token entirely."""

import io
import json

import pytest

import api_tokens
import auth
import camera_hub
import config
import http_handler
import state
import users
from config import set as config_set
from printers.base import PrinterConnection
from test_camera_hub import FRAME, _serve

TOKEN = "c4m-t0ken-abc123"


@pytest.fixture(autouse=True)
def login_on(monkeypatch):
    monkeypatch.setattr(auth, "AUTH_ENABLED", True)
    monkeypatch.setattr(http_handler, "AUTH_ENABLED", True, raising=False)
    auth._sessions.clear()
    users.ensure_initialised()
    yield
    auth._sessions.clear()
    api_tokens.reset_failures()


@pytest.fixture
def camera_token():
    config_set("camera.external_token", TOKEN)
    yield TOKEN
    config_set("camera.external_token", "")


def _call(method, path, cookie=None, body=None):
    h = object.__new__(http_handler.SPHandler)
    h.command, h.path, h.request_version, h.requestline = method, path, "HTTP/1.1", f"{method} {path} HTTP/1.1"
    raw = json.dumps(body).encode() if body is not None else b""
    hdrs = {"Content-Length": str(len(raw))}
    if cookie:
        hdrs["Cookie"] = f"{auth.SESSION_COOKIE}={cookie}"
    h.headers = type("H", (dict,), {"get": lambda self, k, d=None: dict.get(self, k, d)})(hdrs)
    h.client_address = ("10.0.0.9", 1234)
    h.rfile, h.wfile = io.BytesIO(raw), io.BytesIO()
    h.server = type("S", (), {})()
    getattr(h, f"do_{method}")()
    out = h.wfile.getvalue()
    _, _, payload = out.partition(b"\r\n\r\n")
    status = int(out.split(b" ")[1]) if out else 0
    try:
        data = json.loads(payload)
    except Exception:
        data = None
    return status, data


def test_without_token_and_without_session_the_endpoints_stay_401():
    assert _call("GET", "/api/camera/no-such-printer")[0] == 401
    assert _call("GET", "/api/snapshot/no-such-printer")[0] == 401


def test_wrong_token_is_401_and_counts_toward_the_lockout(camera_token):
    status, _ = _call("GET", "/api/camera/no-such-printer?token=wrong")
    assert status == 401
    assert api_tokens.is_blocked("10.0.0.9") is False
    for _ in range(api_tokens.MAX_FAILURES):
        _call("GET", "/api/camera/no-such-printer?token=wrong")
    assert api_tokens.is_blocked("10.0.0.9") is True


def test_valid_token_gets_past_auth(camera_token):
    # the printer does not exist: the response proves auth passed and the
    # camera lookup itself failed
    status, data = _call("GET", f"/api/camera/no-such-printer?token={TOKEN}")
    assert status != 401
    status, _ = _call("GET", f"/api/snapshot/no-such-printer?token={TOKEN}")
    assert status != 401


def test_the_token_is_get_only(camera_token):
    # a POST must not benefit from the token: do_POST's session gate rejects it
    assert _call("POST", f"/api/camera/no-such-printer?token={TOKEN}")[0] == 401


def test_token_grants_nothing_anywhere_else(camera_token):
    assert _call("GET", "/api/printers")[0] == 401
    assert _call("GET", "/api/history")[0] == 401
    assert _call("GET", f"/api/printers?token={TOKEN}")[0] == 401


def test_empty_token_means_the_feature_is_off():
    # an empty configured token must not let an empty query token through
    status, _ = _call("GET", "/api/camera/no-such-printer?token=")
    assert status == 401


def test_regenerating_the_token_revokes_the_old_one(camera_token):
    status, data = _call("POST", "/api/camera-token", _admin_session())
    assert status == 200 and data["token"] and data["token"] != TOKEN
    assert config.get("camera.external_token") == data["token"]
    status, _ = _call("GET", f"/api/camera/no-such-printer?token={TOKEN}")
    assert status == 401


def test_generating_the_token_needs_a_session():
    assert _call("POST", "/api/camera-token")[0] == 401


def test_generating_the_token_needs_the_permission():
    users.create_user("kid", "secret1", "viewer")
    assert _call("POST", "/api/camera-token", _session("kid"))[0] == 403


def _session(username):
    return auth._create_session(username)


def _admin_session():
    users.create_user("boss", "secret1", "admin")
    return _session("boss")


# ── The camera still for Home Assistant ──────────────────────────────────────
# HA's mjpeg camera fetches still_image_url first (?token=). The URL carries a
# PRINTER id, and it must answer with a live frame of that printer's camera —
# the print-history snapshot handler (which the signed-in browser uses for
# /api/snapshot/<history entry id>) can never serve it.

def _get(path, wfile=None):
    """do_GET through the real dispatch; returns (status, content_type, payload)."""
    h = object.__new__(http_handler.SPHandler)
    h.command, h.path, h.request_version, h.requestline = "GET", path, "HTTP/1.1", f"GET {path} HTTP/1.1"
    h.headers = type("H", (dict,), {"get": lambda self, k, d=None: dict.get(self, k, d)})({})
    h.client_address = ("10.0.0.9", 1234)
    h.rfile, h.wfile = io.BytesIO(b""), wfile or io.BytesIO()
    h.server = type("S", (), {})()
    h.do_GET()
    out = h.wfile.getvalue()
    head, _, payload = out.partition(b"\r\n\r\n")
    status = int(head.split(b" ")[1]) if out else 0
    ctype = next((l.split(b": ", 1)[1].decode() for l in head.split(b"\r\n")
                  if l.lower().startswith(b"content-type:")), "")
    return status, ctype, payload


class _HangUpAfterFirstFrame(io.BytesIO):
    """A viewer that closes the picture once the first frame arrived: the
    headers flush is write 1, the first frame write 2, the next raises."""
    def __init__(self):
        super().__init__()
        self._writes = 0

    def write(self, b):
        self._writes += 1
        if self._writes > 2:
            raise ConnectionResetError
        return super().write(b)


@pytest.fixture
def camera_printer(monkeypatch, camera_token):
    """A connected printer whose camera is the real fake MJPEG upstream."""
    monkeypatch.setattr(camera_hub, "LINGER", 0.2)
    srv, url = _serve()
    p = PrinterConnection("sn-cc2-001", "10.0.0.5", "Bench")
    p.connected = True
    p.camera_url = url
    monkeypatch.setitem(state.printers, p.id, p)
    yield p
    srv.shutdown()
    camera_hub.hubs._hubs.clear()


def test_the_still_image_url_serves_a_live_camera_frame(camera_printer):
    status, ctype, payload = _get(f"/api/snapshot/{camera_printer.id}?token={TOKEN}")
    assert status == 200
    assert ctype == "image/jpeg"
    assert payload == FRAME


def test_a_broken_camera_hub_still_answers_the_stream_request(camera_printer, monkeypatch):
    # anything that blows up before the first byte of the answer must still be
    # answered (503) — an unresponsive request reads to Home Assistant as a
    # dropped connection, which its stream proxy reports as a server error
    class _Dead:
        def next(self, timeout):
            raise RuntimeError("hub exploded")
        def close(self):
            pass
    monkeypatch.setattr(camera_hub.hubs, "subscribe", lambda url: _Dead())
    status, ctype, payload = _get(f"/api/camera/{camera_printer.id}?token={TOKEN}")
    assert status == 503
    assert "error" in (payload.decode() if payload else "")


def test_the_stream_url_serves_the_camera_through_the_token(camera_printer):
    status, ctype, payload = _get(f"/api/camera/{camera_printer.id}?token={TOKEN}",
                                  wfile=_HangUpAfterFirstFrame())
    assert status == 200
    assert ctype.startswith("multipart/x-mixed-replace")
    assert FRAME in payload
