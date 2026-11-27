"""notify.py, notifiers.py, snapshot.py and the printer-side event hooks."""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import config
import features
import notifiers
import notify
import push
import snapshot
import state
from notify import Notification
from printers.base import PrinterConnection


@pytest.fixture(autouse=True)
def clean_guard():
    notify.reset_spam_guard()
    yield
    notify.reset_spam_guard()


def _settings(**kw):
    push.save_notif_settings(kw)


# ── should_send ──────────────────────────────────────────────────────────────

def test_event_must_be_switched_on():
    n = Notification("print_started", "p1", "t")
    assert not notify.should_send(n)
    _settings(started={"enabled": True})
    assert notify.should_send(n)


def test_master_switch_off_blocks_everything():
    _settings(finished={"enabled": True})
    features.set_enabled("notifications", False)
    assert not notify.should_send(Notification("print_complete", "p1", "t"))


def test_spam_guard_blocks_repeat_but_not_completion_or_error():
    _settings(paused={"enabled": True}, finished={"enabled": True}, error={"enabled": True})
    paused = Notification("print_paused", "p1", "t")
    assert notify.should_send(paused) and not notify.should_send(paused)
    assert notify.should_send(Notification("print_paused", "p2", "t"))      # other printer is fine
    for ev in ("print_complete", "print_complete", "print_error", "print_error"):
        assert notify.should_send(Notification(ev, "p1", "t"))


def test_test_event_ignores_settings():
    assert notify.should_send(Notification("test", "", "t"))


def test_pause_and_error_share_settings_keys():
    assert notify.EVENT_SETTING["print_complete"] == notify.EVENT_SETTING["print_cancelled"] == "finished"
    assert notify.EVENT_SETTING["printer_offline"] == notify.EVENT_SETTING["printer_online"] == "offline"


def test_image_only_when_event_supports_and_asks():
    _settings(finished={"enabled": True, "image": True}, offline={"enabled": True, "image": True},
              paused={"enabled": True})
    assert notify.wants_image("print_complete")
    assert not notify.wants_image("print_paused")        # supports images, not asked for
    assert not notify.wants_image("printer_offline")     # never carries one


# ── channels ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sent(monkeypatch):
    calls = []
    monkeypatch.setattr(notifiers, "_request", lambda url, data=None, headers=None, method=None:
                        calls.append({"url": url, "data": data, "headers": headers or {}, "method": method}) or b"{}")
    return calls


def _decode_header(value):
    from email.header import decode_header, make_header
    return str(make_header(decode_header(value)))


def test_ntfy_text_message(sent):
    config.set("ntfy.topic", "my printers")
    config.set("ntfy.token", "tk_secret")
    notifiers.send("ntfy", Notification("print_error", "p1", "Bench — Print error", "Hotend fault", priority="urgent"))
    c = sent[0]
    assert c["url"] == "https://ntfy.sh/my%20printers" and c["method"] == "POST"
    assert c["data"] == b"Hotend fault"
    assert _decode_header(c["headers"]["Title"]) == "Bench — Print error" and c["headers"]["Priority"] == "5"
    assert c["headers"]["Authorization"] == "Bearer tk_secret"


def test_ntfy_picture_goes_as_attachment(sent):
    config.set("ntfy.topic", "t")
    notifiers.send("ntfy", Notification("print_complete", "p1", "Done", "x.gcode", image=b"\xff\xd8JPEG\xff\xd9"))
    c = sent[0]
    assert c["method"] == "PUT" and c["data"].startswith(b"\xff\xd8")
    assert c["headers"]["Filename"] == "snapshot.jpg" and c["headers"]["Message"] == "x.gcode"
    assert _decode_header(c["headers"]["Title"]) == "Done"


def test_ntfy_non_latin_title_is_header_safe(sent):
    config.set("ntfy.topic", "t")
    notifiers.send("ntfy", Notification("x", "p", "Skriver ✓ klar — æøå", "b"))
    sent[0]["headers"]["Title"].encode("latin-1")   # must not raise
    assert _decode_header(sent[0]["headers"]["Title"]) == "Skriver ✓ klar — æøå"


def test_telegram_message_and_photo(sent):
    config.set("telegram.token", "123:ABC")
    config.set("telegram.chat_id", "42")
    notifiers.send("telegram", Notification("x", "p", "Title", "Body"))
    assert sent[0]["url"] == "https://api.telegram.org/bot123:ABC/sendMessage"
    assert json.loads(sent[0]["data"]) == {"chat_id": "42", "text": "Title\nBody"}
    notifiers.send("telegram", Notification("x", "p", "Title", "Body", image=b"\xff\xd8x\xff\xd9"))
    assert sent[1]["url"].endswith("/sendPhoto")
    assert b'name="photo"' in sent[1]["data"] and b'name="caption"' in sent[1]["data"]
    assert sent[1]["headers"]["Content-Type"].startswith("multipart/form-data")


def test_telegram_topic_number(sent):
    config.set("telegram.token", "123:ABC")
    config.set("telegram.chat_id", "-100123")
    config.set("telegram.thread_id", "7")
    notifiers.send("telegram", Notification("x", "p", "Title", "Body"))
    assert json.loads(sent[0]["data"]) == {"chat_id": "-100123", "text": "Title\nBody", "message_thread_id": 7}
    notifiers.send("telegram", Notification("x", "p", "Title", "Body", image=b"\xff\xd8x\xff\xd9"))
    assert b'name="message_thread_id"' in sent[1]["data"]
    config.set("telegram.thread_id", "abc")
    with pytest.raises(notifiers.NotifierError):
        notifiers.send("telegram", Notification("x", "p", "Title", "Body"))


def test_discord_json_and_picture(sent):
    config.set("discord.webhook", "https://discord.com/api/webhooks/1/abc")
    notifiers.send("discord", Notification("x", "p", "T", "B"))
    assert json.loads(sent[0]["data"])["content"] == "**T**\nB"
    notifiers.send("discord", Notification("x", "p", "T", "B", image=b"\xff\xd8x\xff\xd9"))
    assert b'name="payload_json"' in sent[1]["data"] and b'name="files[0]"' in sent[1]["data"]


def test_webhook_payload(sent):
    config.set("webhook.url", "https://example.org/hook")
    notifiers.send("webhook", Notification("print_paused", "p1", "T", "B", printer_name="Bench",
                                           extra={"code": 704}, priority="high"))
    body = json.loads(sent[0]["data"])
    assert body["event"] == "print_paused" and body["printer"] == "Bench" and body["details"] == {"code": 704}
    assert body["has_image"] is False


@pytest.mark.parametrize("channel,keys", [
    ("ntfy", {}), ("telegram", {"telegram.token": "x"}), ("discord", {}), ("webhook", {}),
    ("mqtt", {"mqtt.url": "mqtt://192.168.1.60:1883"}),
])
def test_missing_settings_give_readable_error(sent, channel, keys):
    for k, v in keys.items():
        config.set(k, v)
    with pytest.raises(notifiers.NotifierError):
        notifiers.send(channel, Notification("x", "p", "T"))
    assert not sent


# ── MQTT channel: rides the MQTT integration's broker connection ────────────

def test_mqtt_channel_registered_and_configured_by_the_broker_url():
    feat, keys, _ = notifiers.CHANNELS["mqtt"]
    assert feat == "notify_mqtt" and keys == ("mqtt.url",)
    assert not notifiers.is_configured("mqtt")
    config.set("mqtt.url", "mqtt://192.168.1.60:1883")
    assert notifiers.is_configured("mqtt")


def test_mqtt_channel_is_not_a_user_channel():
    """One broker is shared: a per-user MQTT channel would fan the same
    notification out repeatedly to the same topic."""
    assert "mqtt" not in notifiers.USER_CHANNELS


def test_notify_mqtt_requires_the_mqtt_integration():
    config.set("mqtt.url", "mqtt://192.168.1.60:1883")
    assert features.is_enabled("notify_mqtt") is True
    features.set_enabled("mqtt_integration", False)     # cascades notify_mqtt off
    assert features.is_enabled("notify_mqtt") is False
    features.set_enabled("mqtt_integration", True)
    assert features.is_enabled("notify_mqtt") is False  # dependents are not auto-reenabled
    features.set_enabled("notify_mqtt", True)
    assert features.is_enabled("notify_mqtt") is True


def test_bad_urls_rejected(sent):
    config.set("webhook.url", "file:///etc/passwd")
    with pytest.raises(notifiers.NotifierError, match="http"):
        notifiers.send("webhook", Notification("x", "p", "T"))
    config.set("discord.webhook", "ftp://x")
    with pytest.raises(notifiers.NotifierError):
        notifiers.send("discord", Notification("x", "p", "T"))


def test_request_errors_never_contain_the_secret_url(monkeypatch):
    import urllib.error
    def boom(*a, **k):
        raise urllib.error.URLError("connection refused")
    monkeypatch.setattr(notifiers.urllib.request, "urlopen", boom)
    with pytest.raises(notifiers.NotifierError) as e:
        notifiers._request("https://api.telegram.org/botSECRETTOKEN/sendMessage", b"{}")
    assert "SECRETTOKEN" not in str(e.value) and "refused" in str(e.value)


def test_active_channels_need_config_and_feature(monkeypatch):
    assert notifiers.active_channels() == ["webpush"]
    config.set("ntfy.topic", "t")
    assert notifiers.active_channels() == ["webpush", "ntfy"]
    features.set_enabled("notify_ntfy", False)
    assert notifiers.active_channels() == ["webpush"]
    features.set_enabled("notifications", False)
    assert notifiers.active_channels() == []


def test_feature_missing_flag_reflects_config():
    byk = {f["key"]: f for f in features.describe_all()}
    assert byk["notify_ntfy"]["missing"] is True and byk["notify_webpush"]["missing"] is False
    config.set("ntfy.topic", "t")
    assert {f["key"]: f for f in features.describe_all()}["notify_ntfy"]["missing"] is False


def test_failing_channel_does_not_stop_the_others(monkeypatch):
    got = []
    def fake_send(ch, n):
        if ch == "ntfy":
            raise notifiers.NotifierError("down")
        got.append(ch)
    monkeypatch.setattr(notifiers, "send", fake_send)
    notify._deliver(Notification("x", "p", "T"), ["ntfy", "webhook"])
    assert got == ["webhook"]


def test_secrets_are_masked_in_integrations_listing():
    config.set("telegram.token", "123:ABC")
    config.set("discord.webhook", "https://discord.com/api/webhooks/1/abc")
    dump = json.dumps(config.describe_all())
    assert "123:ABC" not in dump and "api/webhooks" not in dump


# ── snapshot ─────────────────────────────────────────────────────────────────

class _Mjpeg(BaseHTTPRequestHandler):
    status = 200

    def do_GET(self):
        self.send_response(_Mjpeg.status)
        self.send_header("Content-Type", "multipart/x-mixed-replace; boundary=frame")
        self.end_headers()
        if _Mjpeg.status != 200:
            return
        frame = b"\xff\xd8" + b"JPEGDATA" * 50 + b"\xff\xd9"
        try:
            self.wfile.write(b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + frame + b"\r\n--frame\r\n" + frame)
            self.wfile.flush()
        except OSError:
            pass

    def log_message(self, *a):
        pass


@pytest.fixture
def camera():
    _Mjpeg.status = 200
    srv = HTTPServer(("127.0.0.1", 0), _Mjpeg)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}/?action=stream"
    srv.shutdown()


def test_grab_jpeg_returns_first_complete_frame(camera):
    jpg = snapshot.grab_jpeg(camera, timeout=3)
    assert jpg.startswith(b"\xff\xd8") and jpg.endswith(b"\xff\xd9") and b"--frame" not in jpg


def test_grab_jpeg_none_on_error_or_bad_url(camera):
    _Mjpeg.status = 503
    assert snapshot.grab_jpeg(camera, timeout=2) is None
    assert snapshot.grab_jpeg("http://127.0.0.1:1/", timeout=1) is None
    assert snapshot.grab_jpeg("", timeout=1) is None
    assert snapshot.grab_jpeg("file:///etc/passwd", timeout=1) is None


# ── printer-side hooks ───────────────────────────────────────────────────────

@pytest.fixture
def printer(monkeypatch):
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.connected = True
    captured = []
    monkeypatch.setattr(notify, "is_event_on", lambda event: True)
    monkeypatch.setattr(notify, "notify", lambda n: captured.append(n) or True)
    p.captured = captured
    return p


def _reason(kind="pause", initiated_by="printer", category="unknown", **kw):
    return {"kind": kind, "initiated_by": initiated_by, "category": category,
            "code": kw.get("code", ""), "message": kw.get("message", ""), "action": kw.get("action", ""),
            "raw": {}, "since": "x"}


def test_pause_from_printer_notifies_with_cause(printer):
    printer._notify_reason(_reason(category="thermal", code=103, message="Hotend isn't heating", action="Check it."))
    n = printer.captured[0]
    assert n.event == "print_paused" and n.title == "Bench — Print paused"
    assert "Hotend isn't heating" in n.body and "(code 103)" in n.body and "Check it." in n.body
    assert n.extra["reason"]["code"] == 103


def test_pause_started_from_spooler_is_silent(printer):
    printer._notify_reason(_reason(initiated_by="spooler"))
    assert printer.captured == []


def test_error_notifies_urgently(printer):
    printer._notify_reason(_reason(kind="error", category="fan", code=701, message="Mainboard fan fault."))
    n = printer.captured[0]
    assert n.event == "print_error" and n.priority == "urgent" and "701" in n.body


def test_filament_runout_has_its_own_event_for_pause_and_error(printer):
    printer._notify_reason(_reason(kind="pause", category="filament_runout", code=1260, message="Filament break."))
    printer._notify_reason(_reason(kind="error", category="filament_runout", code=109))
    assert [n.event for n in printer.captured] == ["filament_runout", "filament_runout"]


def test_error_without_known_cause_still_says_something(printer):
    printer._notify_reason(_reason(kind="error"))
    assert printer.captured[0].body == "The printer reported an error."


@pytest.mark.asyncio
async def test_emit_takes_camera_picture_when_asked(printer, monkeypatch):
    printer.camera_url = "http://cam/"
    monkeypatch.setattr(state, "printers", {"pid1": printer})
    monkeypatch.setattr(notify, "wants_image", lambda event: True)
    import printers.base as base
    monkeypatch.setattr(base, "grab_jpeg", lambda url: b"\xff\xd8x\xff\xd9")
    await state.publish_now("print_complete", printer_id=printer.id, printer=printer.to_dict())
    assert printer.captured and printer.captured[0].image == b"\xff\xd8x\xff\xd9"


@pytest.mark.asyncio
async def test_offline_notice_comes_after_delay_and_online_after_return(printer, monkeypatch):
    import printers.base as base
    monkeypatch.setattr(notify, "event_settings", lambda event: {"minutes": 5})
    real_sleep = asyncio.sleep
    delays = []
    async def fast_sleep(t):
        delays.append(t)
        await real_sleep(0)
    monkeypatch.setattr(base.asyncio, "sleep", fast_sleep)
    printer.connected = False
    printer._track_connection_for_notifications()
    await real_sleep(0.05)
    assert delays == [300.0]
    assert [n.event for n in printer.captured] == ["printer_offline"]
    printer.connected = True
    printer._track_connection_for_notifications()
    await real_sleep(0.05)
    assert [n.event for n in printer.captured] == ["printer_offline", "printer_online"]


@pytest.mark.asyncio
async def test_quick_reconnect_cancels_offline_notice(printer, monkeypatch):
    monkeypatch.setattr(notify, "event_settings", lambda event: {"minutes": 5})
    printer.connected = False
    printer._track_connection_for_notifications()
    assert printer._offline_task is not None
    printer.connected = True
    printer._track_connection_for_notifications()
    await asyncio.sleep(0.05)
    assert printer.captured == [] and printer._offline_task is None


# ── a real HTTP round trip (nothing mocked) ──────────────────────────────────

class _Hook(BaseHTTPRequestHandler):
    seen = []
    code = 200

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        _Hook.seen.append({"headers": dict(self.headers), "body": self.rfile.read(n)})
        self.send_response(_Hook.code)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *a):
        pass


@pytest.fixture
def hook():
    _Hook.seen, _Hook.code = [], 200
    srv = HTTPServer(("127.0.0.1", 0), _Hook)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}/hook"
    srv.shutdown()


def test_webhook_round_trip_and_test_message(hook):
    config.set("webhook.url", hook)
    notify.send_test("webhook")
    body = json.loads(_Hook.seen[0]["body"])
    assert body["event"] == "test" and _Hook.seen[0]["headers"]["Content-Type"] == "application/json"


def test_webhook_http_error_is_readable(hook):
    config.set("webhook.url", hook)
    _Hook.code = 500
    with pytest.raises(notifiers.NotifierError, match="HTTP 500"):
        notify.send_test("webhook")


def test_notify_hands_off_to_worker_and_filters(hook, monkeypatch):
    config.set("webhook.url", hook)
    _settings(paused={"enabled": True})
    done = threading.Event()
    real = notify._deliver
    monkeypatch.setattr(notify, "_deliver", lambda n, ch: (real(n, ch), done.set()))
    assert notify.notify(Notification("print_paused", "p1", "T", "B"))
    assert done.wait(3)
    assert any(json.loads(r["body"])["event"] == "print_paused" for r in _Hook.seen)
    assert not notify.notify(Notification("print_started", "p1", "T"))   # event off


def _record_events(p):
    """Record published event names at the bus: the printer publishes now,
    it no longer calls _emit (ADR-0002)."""
    for ev in notify.MESSAGES:
        state.subscribe(ev, lambda payload, ev=ev, p=p: p.emitted.append(ev))


def test_hot_nozzle_idle_warns_once_even_if_the_status_flickers():
    """Real CC2: ten of these arrived. A busy/preparing status between two idle ones must not re-arm it."""
    push.save_notif_settings({"nozzle_idle": {"enabled": True, "threshold": 40}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)

    def at(status, nozzle):
        p.status = {"PrintInfo": {"Status": status}, "TempOfNozzle": nozzle}
        p._check_notifications()
    for status, nozzle in [(0, 180), (15, 175), (0, 170), (15, 160), (0, 150), (0, 41), (0, 43), (0, 38)]:
        at(status, nozzle)
    assert p.emitted.count("nozzle_hot_idle") == 1
    at(0, 30)                                   # cooled: armed again
    at(0, 120)
    assert p.emitted.count("nozzle_hot_idle") == 2


def test_print_complete_is_announced_even_if_the_printer_is_busy_just_before_it_goes_idle():
    """Real CC2: history was saved but no 'complete' notification came; its last status before idle was a busy one."""
    push.save_notif_settings({"finished": {"enabled": True}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)

    def at(status):
        p.status = {"PrintInfo": {"Status": status, "Filename": "a.gcode"}}
        p._check_notifications()
    for status in (0, 15, 3, 3, 10, 0, 0):
        at(status)
    assert p.emitted == ["print_complete"]          # once, not again on the second idle
    for status in (1, 0):                           # homing afterwards isn't a print
        at(status)
    assert p.emitted == ["print_complete"]


def _layer_printer():
    push.save_notif_settings({"layer": {"enabled": True, "layer": 3}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)

    def at(status, layer):
        p.status = {"PrintInfo": {"Status": status, "CurrentLayer": layer}}
        p._check_notifications()
    return p, at


def test_layer_notice_comes_once_per_print_not_on_every_later_layer():
    """Real printers: 'Layer 3 reached' kept arriving at other layers after a busy/paused status in between."""
    p, at = _layer_printer()
    for status, layer in [(0, 0), (3, 1), (3, 2), (3, 3), (3, 4), (10, 4), (3, 5), (5, 5), (3, 6), (3, 0), (3, 7)]:
        at(status, layer)
    assert p.emitted == ["layer_reached"]
    for status, layer in [(0, 0), (0, 0), (3, 1), (3, 3), (3, 4)]:     # the next print gets its own
        at(status, layer)
    assert p.emitted == ["layer_reached", "layer_reached"]


def test_layer_notice_is_not_sent_late_after_a_restart_in_the_middle_of_a_print():
    p, at = _layer_printer()
    for status, layer in [(3, 9), (3, 10), (3, 11)]:                   # Spooler just started; the print is far past layer 3
        at(status, layer)
    assert p.emitted == []


def test_layer_notice_can_carry_a_picture():
    """To check that the print hasn't come loose from the bed."""
    push.save_notif_settings({"layer": {"enabled": True, "layer": 3, "image": True}})
    assert notify.wants_image("layer_reached")
    push.save_notif_settings({"layer": {"enabled": True, "layer": 3, "image": False}})
    assert not notify.wants_image("layer_reached")


def test_print_complete_and_layer_notices_work_for_the_cc1_printing_status():
    """Real CC1: a finished print sent no notification because its printing status (13) wasn't counted as a print."""
    push.save_notif_settings({"finished": {"enabled": True}, "layer": {"enabled": True, "layer": 3}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)

    def at(status, layer=0):
        p.status = {"PrintInfo": {"Status": status, "Filename": "a.gcode", "CurrentLayer": layer}}
        p._check_notifications()
    for status, layer in [(0, 0), (18, 0), (13, 1), (13, 3), (13, 4), (9, 4), (9, 4)]:
        at(status, layer)
    assert p.emitted == ["layer_reached", "print_complete"]


def test_outgoing_requests_do_not_use_the_default_python_user_agent(monkeypatch):
    """Discord sits behind Cloudflare, which answers 403 to "Python-urllib"."""
    import notifiers
    seen = []

    class _Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b""

    def fake_urlopen(req, timeout=None):
        seen.append(req)
        return _Resp()

    monkeypatch.setattr(notifiers.urllib.request, "urlopen", fake_urlopen)
    notifiers._request("https://example.com/hook", data=b"{}")
    notifiers._request("https://example.com/hook", headers={"user-agent": "Mine"})
    assert seen[0].get_header("User-agent") == "Spooler"
    assert seen[1].get_header("User-agent") == "Mine"


# ── message registry: every event renders the exact prose that used to be
# built inline in printers/base.py (verbatim parity — behavior must not
# change in the migration, ADR-0002). ─────────────────────────────────────────

REASON = {"kind": "error", "category": "filament_runout", "code": 42, "message": "Nozzle thermal runaway",
          "action": "Check the filament sensor", "initiated_by": "printer", "raw": {}, "since": "x"}

RENDER_CASES = [
    ("print_started", "Bench", {"filename": "benchy.gcode"}, ("Bench — Print started", "benchy.gcode")),
    ("print_started", "Bench", {}, ("Bench — Print started", "A print has started.")),
    ("print_complete", "Bench", {"filename": "benchy.gcode"}, ("Bench — Print complete", "benchy.gcode is done.")),
    ("print_complete", "Bench", {}, ("Bench — Print complete", "Your print is complete.")),
    ("print_cancelled", "Bench", {"filename": "b.gcode"}, ("Bench — Print cancelled", "b.gcode was cancelled.")),
    ("print_cancelled", "Bench", {}, ("Bench — Print cancelled", "Your print was cancelled.")),
    ("print_error", "Bench", {"reason": REASON},
     ("Bench — Print error", "Nozzle thermal runaway (code 42) Check the filament sensor")),
    ("print_error", "Bench", {}, ("Bench — Print error", "The printer reported an error.")),
    ("print_paused", "Bench", {"reason": REASON},
     ("Bench — Print paused", "Nozzle thermal runaway (code 42) Check the filament sensor")),
    ("print_paused", "Bench", {}, ("Bench — Print paused", "The print was paused.")),
    ("filament_runout", "Bench", {"reason": REASON},
     ("Bench — Filament ran out", "Nozzle thermal runaway (code 42) Check the filament sensor")),
    ("filament_runout", "Bench", {}, ("Bench — Filament ran out", "Load filament and resume.")),
    ("printer_fault", "Bench", {"reason": REASON},
     ("Bench — Printer fault", "Nozzle thermal runaway (code 42) Check the filament sensor")),
    ("printer_fault", "Bench", {}, ("Bench — Printer fault", "The printer reported a fault.")),
    ("printer_online", "Bench", {}, ("Bench — Printer online", "The printer is connected again.")),
    ("printer_offline", "Bench", {"minutes": 7.5},
     ("Bench — Printer offline", "No contact with the printer for 7.5 minutes.")),
    ("nozzle_hot_idle", "Bench", {"temp": 60.4}, ("Bench — Nozzle hot", "Nozzle is 60°C while idle.")),
    ("nozzle_overheat", "Bench", {"temp": 270.5}, ("Bench — Nozzle overheat", "Nozzle is 270°C during print.")),
    ("layer_reached", "Bench", {"layer": 12, "target": 10}, ("Bench — Layer 10 reached", "Currently on layer 12.")),
    ("firmware_changed", "Bench", {"from": "1.0", "to": "1.1", "tested": True},
     ("Bench — Firmware changed", "1.0 → 1.1.")),
    ("firmware_changed", "Bench", {"from": "1.0", "to": "1.1", "tested": False},
     ("Bench — Firmware changed", "1.0 → 1.1. It has not been tested with this version of Spooler.")),
    ("service_due", "Bench", {"task": "Oil the rods", "since": 10.0, "unit": "h", "interval": 250},
     ("Bench — Service due: Oil the rods", "10 h since last time (every 250 h).")),
    ("spaghetti", "Bench", {}, ("Bench — Possible print failure",
                                "The detector thinks this print may be failing (spaghetti). Check the printer.")),
]


@pytest.mark.parametrize("event,name,data,expected", RENDER_CASES)
def test_registry_renders_the_exact_previous_prose(event, name, data, expected):
    assert notify._render(event, name, data) == expected


def test_registry_covers_every_event_the_channel_knows():
    # spool_low is excluded: spoolman emits it directly (channel-side, its
    # own Notification) — it never rode printer._emit and never will ride
    # the bus's printer events.
    assert set(notify.MESSAGES) == set(notify.EVENT_SETTING) - {"spool_low"}


# ── bus subscriber ───────────────────────────────────────────────────────────

@pytest.fixture
def wired_and_notifying(monkeypatch):
    """Wire the notify channel; record the Notifications it builds instead
    of delivering them."""
    import channels
    import state
    monkeypatch.setattr(state, "_subscriptions", {})
    channels.wire()
    seen = []
    monkeypatch.setattr(notify, "notify", lambda n: seen.append(n) or True)
    return seen


@pytest.mark.asyncio
async def test_subscriber_builds_the_notification_from_the_event(wired_and_notifying, monkeypatch):
    seen = wired_and_notifying
    monkeypatch.setattr(state, "printers", {"p1": type("P", (), {"name": "Bench"})()})
    features.set_enabled("notifications", True)
    _settings(started={"enabled": True})

    await state.publish_now("print_started", printer_id="p1", data={"filename": "benchy.gcode"})
    assert len(seen) == 1
    n = seen[0]
    assert (n.event, n.printer_id, n.title, n.body) == (
        "print_started", "p1", "Bench — Print started", "benchy.gcode")


@pytest.mark.asyncio
async def test_subscriber_skips_events_switched_off(wired_and_notifying):
    features.set_enabled("notifications", True)     # event itself stays off
    await state.publish_now("print_started", printer_id="p1", data={})
    assert wired_and_notifying == []


# ── nozzle overheat: once per print, re-armed by cooling ─────────────────────

def _overheat_printer():
    push.save_notif_settings({"nozzle_printing": {"enabled": True, "threshold": 260}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)

    def at(status, nozzle):
        p.status = {"PrintInfo": {"Status": status}, "TempOfNozzle": nozzle}
        p._check_notifications()
    return p, at


def test_nozzle_overheat_fires_once_while_hot_and_rearms_after_cooling():
    p, at = _overheat_printer()
    at(3, 270)                       # printing, over the threshold: fires
    at(3, 280)                       # still hot: no repeat
    assert p.emitted.count("nozzle_overheat") == 1
    at(3, 250)                       # cooled below: re-arms
    at(3, 265)
    assert p.emitted.count("nozzle_overheat") == 2


def test_nozzle_overheat_resets_when_the_printer_stops_printing():
    p, at = _overheat_printer()
    at(3, 270)
    assert p.emitted.count("nozzle_overheat") == 1
    at(0, 270)                       # idle and still hot: no overheat event…
    at(3, 270)                       # …but the guard is re-armed for the next print
    assert p.emitted.count("nozzle_overheat") == 2


def test_nozzle_overheat_is_silent_when_the_setting_is_off():
    push.save_notif_settings({"nozzle_printing": {"enabled": False}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)
    p.status = {"PrintInfo": {"Status": 3}, "TempOfNozzle": 270}
    p._check_notifications()
    p.status = {"PrintInfo": {"Status": 3}, "TempOfNozzle": 280}
    p._check_notifications()
    assert p.emitted.count("nozzle_overheat") == 0


# ── cancelled: the filename rides along, fires once per print ────────────────

def test_print_cancelled_carries_the_filename_and_fires_once():
    push.save_notif_settings({"finished": {"enabled": True}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)

    def at(status):
        p.status = {"PrintInfo": {"Status": status, "Filename": "benchy.gcode"}}
        p._check_notifications()
    at(0)
    at(3)                            # printing
    at(8)                            # cancelled
    assert p.emitted.count("print_cancelled") == 1
    at(0)
    at(0)
    assert p.emitted.count("print_cancelled") == 1, "once per print, not on later idles"


# ── service reminders ────────────────────────────────────────────────────────

def test_service_due_publishes_when_a_reminder_comes_due(monkeypatch):
    import service
    features.set_enabled("service_log", True)
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)
    monkeypatch.setattr(service, "take_due",
                        lambda pid: [{"name": "Oil the rods", "since": 10.0,
                                      "unit": "h", "interval": 250}])
    p._announce_service_due()
    assert p.emitted == ["service_due"]


def test_service_due_is_silent_when_service_log_is_off():
    features.set_enabled("service_log", False)
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)
    p._announce_service_due()
    assert p.emitted == []


async def test_offline_notice_falls_back_to_five_minutes_on_a_bad_setting(monkeypatch):
    """The minutes field is free text in settings storage: 'abc' must fall back
    to the 5-minute default, not crash the offline task."""
    import printers.base as base
    push.save_notif_settings({"offline": {"enabled": True, "minutes": "abc"}})
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.emitted = []
    _record_events(p)

    real_sleep = asyncio.sleep
    delays = []

    async def fast_sleep(t):
        delays.append(t)
        await real_sleep(0)

    monkeypatch.setattr(base.asyncio, "sleep", fast_sleep)
    p.connected = False
    p._track_connection_for_notifications()
    await real_sleep(0.05)
    assert delays == [300.0]
    assert p.emitted == ["printer_offline"]


def test_check_notifications_is_silent_when_the_master_switch_is_off():
    features.set_enabled("notifications", False)
    p = PrinterConnection("pid1", "10.0.0.5", "Bench")
    p.status = {"PrintInfo": {"Status": 0}, "TempOfNozzle": 300}
    p._check_notifications()          # early return: nothing loads, nothing publishes


# ── _deliver: one channel's failure never hides the others ───────────────────

@pytest.fixture
def broken_channel():
    import notifiers
    def broken(n, get=None):
        raise RuntimeError("channel bug")
    notifiers.CHANNELS["broken"] = ("notify_test", (), broken)
    yield "broken"
    notifiers.CHANNELS.pop("broken", None)


def test_a_notifier_error_is_logged_not_raised(capsys):
    import notifiers
    import notify
    try:
        notifiers.CHANNELS["failing"] = ("notify_test", (),
                                         lambda n, get=None: (_ for _ in ()).throw(notifiers.NotifierError("The service answered HTTP 503.")))
        notify._deliver(notify.Notification("test", "p", "T"), ["failing"])   # must not raise
    finally:
        notifiers.CHANNELS.pop("failing", None)
    assert "failed: The service answered HTTP 503." in capsys.readouterr().out


def test_a_channel_bug_is_isolated_and_logged(broken_channel, capsys):
    import notify
    notify._deliver(notify.Notification("test", "p", "T"), [broken_channel])   # must not raise
    assert "error: RuntimeError" in capsys.readouterr().out


def test_one_failing_channel_does_not_stop_the_next(broken_channel, capsys):
    import notifiers
    import notify
    got = []
    notifiers.CHANNELS["good"] = ("notify_test", (), lambda n, get=None: got.append(n.id) or b"")
    try:
        notify._deliver(notify.Notification("test", "p", "T"), ["broken", "good"])
    finally:
        notifiers.CHANNELS.pop("good", None)
    assert len(got) == 1, "the second channel still received the notification"


def test_a_users_own_channel_gets_their_settings_lookup():
    import notifiers
    import notify
    got = []
    notifiers.CHANNELS["good"] = ("notify_test", (), lambda n, get: got.append(get("ntfy.topic")) or b"")
    try:
        notify._deliver(notify.Notification("test", "p", "T"),
                        [("anna/good", "good", lambda key: "anna-topic" if key == "ntfy.topic" else None)])
    finally:
        notifiers.CHANNELS.pop("good", None)
    assert got == ["anna-topic"], "the user's own settings lookup rides to the sender"
