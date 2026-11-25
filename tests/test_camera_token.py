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
import config
import http_handler
import users
from config import set as config_set

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
