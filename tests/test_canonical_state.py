"""printers/base.py — canonical numeric state.

The protocol-key fallback chains (CC1 SDCP names vs CC2/Prusa/Moonraker
names) and the progress rules the frontend's getProgress() uses live here,
once, in to_dict(). Every consumer of printer state — browser UI, MQTT
bridge, future channels — reads these keys and owns no protocol logic.
"""

from printers.base import PrinterConnection


# Real CC1 SDCP shape: no top-level SpeedFactor exists — the CC1 reports its
# speed as PrintSpeedPct inside PrintInfo (the card reads the same field).
CC1_STATUS = {"TempOfNozzle": 210, "TempTargetNozzle": 215, "TempOfHotbed": 60,
              "TempTargetHotbed": 65, "TempOfBox": 30,
              "PrintInfo": {"Status": 3, "CurrentLayer": 12, "TotalLayer": 100,
                            "PrintTime": 120, "RemainTime": 300,
                            "PrintSpeedPct": 95}}


def _p(status, **attrs):
    p = PrinterConnection("p1", "192.0.2.5", "Workshop")
    p.connected = True
    p.attrs = {"FirmwareVersion": "1.2.3"}
    p.status = status
    return p


def test_canonical_keys_read_cc1_names():
    d = _p(CC1_STATUS).to_dict()
    assert d["nozzle_temp"] == 210 and d["nozzle_target"] == 215
    assert d["bed_temp"] == 60 and d["bed_target"] == 65
    assert d["chamber_temp"] == 30
    assert d["layer"] == 12 and d["total_layer"] == 100
    assert d["time_remaining"] == 300
    assert d["speed_factor"] == 95


def test_canonical_keys_read_cc2_names():
    d = _p({"NozzleTemp": 1, "NozzleTempTarget": 2, "BedTemp": 3,
            "BedTempTarget": 4, "ChamberTemp": 5}).to_dict()
    assert (d["nozzle_temp"], d["nozzle_target"], d["bed_temp"],
            d["bed_target"], d["chamber_temp"]) == (1, 2, 3, 4, 5)


def test_canonical_fan_keys_read_the_status_fanspeed_block():
    """CC1 and CC2 both put fan percents into status['FanSpeed'] under the
    spooler names (part/aux/chamber) — the canonical keys project them like
    every other numeric."""
    d = _p({"FanSpeed": {"part": 85, "aux": 0, "chamber": 100}}).to_dict()
    assert (d["fan_part"], d["fan_aux"], d["fan_chamber"]) == (85, 0, 100)


def test_canonical_fan_keys_absent_read_zero():
    """A protocol without fan data (PrusaLink, Moonraker today) reads 0 like
    the temps do; consumers gate fan entities on capabilities, not values."""
    d = _p({"NozzleTemp": 1}).to_dict()
    assert (d["fan_part"], d["fan_aux"], d["fan_chamber"]) == (0, 0, 0)


def test_canonical_fan_keys_of_an_offline_printer_read_zero():
    """An offline printer's last readings are history, not a measurement —
    same rule as the canonical temps."""
    p = PrinterConnection("p1", "192.0.2.5", "Workshop")
    p.connected = False
    p.status = {"FanSpeed": {"part": 85, "aux": 0, "chamber": 100}}
    d = p.to_dict()
    assert (d["fan_part"], d["fan_aux"], d["fan_chamber"]) == (0, 0, 0)


def test_speed_factor_reads_the_cc1_print_speed_pct():
    # Review reproduction: a CC1 printing at 160 % must not report 100 —
    # SpeedFactor belongs to CC2/Moonraker/PrusaLink, PrintSpeedPct to CC1.
    d = _p({"PrintInfo": {"Status": 3, "PrintSpeedPct": 160}}).to_dict()
    assert d["speed_factor"] == 160


def test_speed_factor_still_reads_cc2_speedfactor():
    d = _p({"SpeedFactor": 130}).to_dict()
    assert d["speed_factor"] == 130


def test_progress_prefers_printer_percentage():
    # TotalTicks == 100 means CurrentTicks IS the percentage (CC2/Moonraker/
    # PrusaLink); it beats layers, which understate progress (issue #56).
    d = _p({"PrintInfo": {"TotalTicks": 100, "CurrentTicks": 42,
                          "TotalLayer": 100, "CurrentLayer": 99}}).to_dict()
    assert d["progress"] == 42


def test_progress_from_layers_when_no_percentage():
    d = _p({"PrintInfo": {"TotalTicks": 700, "CurrentTicks": 40,
                          "TotalLayer": 100, "CurrentLayer": 25}}).to_dict()
    assert d["progress"] == 25


def test_progress_at_layer_zero_matches_the_card():
    # getProgress() uses layers as soon as TotalLayer > 0, so layer 0 of 200
    # is 0 % — it must not fall through to the tick fraction.
    d = _p({"PrintInfo": {"TotalLayer": 200, "CurrentLayer": 0,
                          "TotalTicks": 700, "CurrentTicks": 7}}).to_dict()
    assert d["progress"] == 0


def test_progress_none_when_unknown():
    assert _p({}).to_dict()["progress"] is None


def test_canonical_temps_default_to_zero_like_the_ui():
    d = _p({}).to_dict()
    assert d["nozzle_temp"] == 0 and d["nozzle_target"] == 0
    assert d["bed_temp"] == 0 and d["bed_target"] == 0
    assert d["chamber_temp"] == 0 and d["chamber_target"] == 0
    assert d["layer"] == 0 and d["total_layer"] == 0
    assert d["time_remaining"] == 0 and d["speed_factor"] == 100


def test_offline_temps_read_zero_like_the_card():
    # An offline printer's last readings are history, not a measurement —
    # the card shows "--" (app-render.js zeroes when !connected); the
    # canonical keys must not hand a consumer stale values instead.
    p = _p(CC1_STATUS)
    p.connected = False
    d = p.to_dict()
    assert d["nozzle_temp"] == 0 and d["nozzle_target"] == 0
    assert d["bed_temp"] == 0 and d["bed_target"] == 0
    assert d["chamber_temp"] == 0 and d["chamber_target"] == 0


# ── Canvas promotion (docs/adr-mqtt-bridge.md): to_dict["canvas"] ────────────
# The raw blob both adapters pass through as status["canvas_info"] (CC1
# CMD_CANVAS, CC2 method 2005) is mapped once, here, to canonical keys.
# Verified live 2026-11-24 (CC2_INTEGRATION.md §4b): exactly nine per-tray
# fields exist (filament_code is protocol noise and dropped); tray status
# 0/1/2 is dynamic state (0 empty, 1 loaded/idle, 2 active); active_tray_id
# -1 is transient (between prints / at start). Canonical keeps the numbers;
# presentation decides how to render them.

CANVAS_INFO = {
    "active_canvas_id": 0,
    "active_tray_id": 2,
    "auto_refill": False,
    "canvas_list": [{
        "canvas_id": 0,
        "connected": 1,
        "tray_list": [
            {"tray_id": 0, "status": 1, "brand": "ELEGOO", "filament_code": "0x0001",
             "filament_color": "#FFFFFF", "filament_name": "PLA+", "filament_type": "PLA",
             "min_nozzle_temp": 190, "max_nozzle_temp": 230},
            {"tray_id": 1, "status": 1, "brand": "ELEGOO", "filament_code": "0x0001",
             "filament_color": "#000000", "filament_name": "PLA+", "filament_type": "PLA",
             "min_nozzle_temp": 190, "max_nozzle_temp": 230},
            {"tray_id": 2, "status": 2, "brand": "ELEGOO", "filament_code": "0x0006",
             "filament_color": "#FFFFFF", "filament_name": "PLA Matte", "filament_type": "PLA",
             "min_nozzle_temp": 190, "max_nozzle_temp": 230},
            {"tray_id": 3, "status": 0, "brand": "ELEGOO", "filament_code": "0x0100",
             "filament_color": "#BCBCBC", "filament_name": "PETG", "filament_type": "PETG",
             "min_nozzle_temp": 230, "max_nozzle_temp": 260},
        ],
    }],
}


def test_canvas_promoted_to_canonical_keys():
    c = _p({"canvas_info": CANVAS_INFO}).to_dict()["canvas"]
    assert c["active_tray"] == 2
    assert c["trays"] == [
        {"id": 0, "status": 1, "material": "PLA",  "name": "PLA+",      "brand": "ELEGOO", "color": "#FFFFFF",  "min_temp": 190, "max_temp": 230},
        {"id": 1, "status": 1, "material": "PLA",  "name": "PLA+",      "brand": "ELEGOO", "color": "#000000",  "min_temp": 190, "max_temp": 230},
        {"id": 2, "status": 2, "material": "PLA",  "name": "PLA Matte", "brand": "ELEGOO", "color": "#FFFFFF",  "min_temp": 190, "max_temp": 230},
        {"id": 3, "status": 0, "material": "PETG", "name": "PETG",      "brand": "ELEGOO", "color": "#BCBCBC",  "min_temp": 230, "max_temp": 260},
    ]


def test_canvas_absent_is_none():
    assert _p({}).to_dict()["canvas"] is None


def test_canvas_negative_active_tray_passes_through():
    ci = {**CANVAS_INFO, "active_tray_id": -1}
    assert _p({"canvas_info": ci}).to_dict()["canvas"]["active_tray"] == -1


def test_canvas_raw_blob_still_reaches_the_browser():
    # The browser keeps reading status.canvas_info unchanged.
    p = _p({"canvas_info": CANVAS_INFO})
    p.to_dict()
    assert p.status["canvas_info"] is CANVAS_INFO


def test_canvas_skips_malformed_tray_entries():
    ci = {"active_tray_id": 1, "canvas_list": [
        {"canvas_id": 0, "tray_list": [
            {"tray_id": 0, "filament_type": "PLA", "status": 2},
        ]},
        {"canvas_id": 1, "tray_list": ["junk", None, {"no_id": True}]},
    ]}
    c = _p({"canvas_info": ci}).to_dict()["canvas"]
    assert c == {"active_tray": 1, "trays": [
        {"id": 0, "status": 2, "material": "PLA", "name": None, "brand": None,
         "color": None, "min_temp": None, "max_temp": None},
    ]}


def test_canvas_without_trays_is_none():
    ci = {"active_tray_id": -1, "canvas_list": [{"canvas_id": 0, "tray_list": []}]}
    assert _p({"canvas_info": ci}).to_dict()["canvas"] is None
