"""Offline regression tests for station selection and download retry."""

import collections
import http.server
import threading

import pandas as pd
import pytest

from lcd import download
from lcd.download import Region, _download_one, _make_session, select_stations

INVENTORY = """USAF,WBAN,STATION NAME,CTRY,ST,CALL,LAT,LON,ELEV(M),BEGIN,END
701337,103,ACTIVE AIRPORT,US,AK,PAIW,65.6,-168.1,7,20090122,20230907
701338,104,CLOSED AIRPORT,US,AK,PAXX,65.7,-168.2,8,20000101,20100315
701339,105,STALE AIRPORT,US,AK,PAYY,65.8,-168.3,9,20000101,20220601
"""


@pytest.fixture
def inventory(tmp_path):
    path = tmp_path / "inv.stations"
    path.write_text(INVENTORY)
    return path


def test_stations_active_at_snapshot_are_open_ended(inventory):
    """Stations still reporting when the inventory was built stay selectable."""
    region = Region(64, 67, -170, -166, 2025, 2025)
    selected = select_stations(region, inventory)
    assert list(selected["station_id"]) == ["70133700103"]
    assert int(selected["end"].iloc[0]) == max(2023, pd.Timestamp.now().year)


def test_ended_stations_are_not_extended(inventory):
    region = Region(64, 67, -170, -166, 2000, 2026)
    selected = select_stations(region, inventory).set_index("station_id")
    assert selected.loc["70133800104", "end"] == 2010
    assert selected.loc["70133900105", "end"] == 2022


def _serve(handler):
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def test_truncated_body_is_retried(tmp_path, monkeypatch):
    monkeypatch.setattr(download, "_TRANSFER_BACKOFF", 0.0)
    body = b"x" * 100_000
    hits = collections.Counter()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            hits[self.path] += 1
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if hits[self.path] == 1:  # first attempt: body cut short
                self.wfile.write(body[:10_000])
                self.wfile.flush()
                self.close_connection = True
                return
            self.wfile.write(body)

    srv, url = _serve(Handler)
    try:
        status = _download_one(_make_session(), url, 2024, "70133700103", tmp_path, 10.0)
    finally:
        srv.shutdown()
    dest = tmp_path / "2024" / "70133700103.csv"
    assert status == "ok"
    assert dest.stat().st_size == len(body)
    assert sum(hits.values()) == 2


def test_http_errors_are_not_retried(tmp_path, monkeypatch):
    monkeypatch.setattr(download, "_TRANSFER_BACKOFF", 0.0)
    hits = collections.Counter()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            hits[self.path] += 1
            self.send_response(404 if self.path.endswith("a.csv") else 403)
            self.send_header("Content-Length", "0")
            self.end_headers()

    srv, url = _serve(Handler)
    try:
        session = _make_session()
        missing = _download_one(session, url, 2024, "a", tmp_path, 5.0)
        denied = _download_one(session, url, 2024, "b", tmp_path, 5.0)
    finally:
        srv.shutdown()
    assert (missing, denied) == ("missing", "error")
    assert sum(hits.values()) == 2
    assert not list(tmp_path.rglob("*.csv"))


def test_intensity_prefixed_tokens_are_convective():
    """Real LCD strings: '+TS:7 RA:02 BR:1' is a heavy thunderstorm (AU group)."""
    from lcd.classify import precip_type

    df = pd.DataFrame(
        {
            "au": ["+TS:7 RA:02 BR:1", "-TS:7", "TS:7", "-RA:02 BR:1", "", ""],
            "aw": ["", "", "", "RA", "+FC", "SHRA"],
            "mw": ["RA", "", "", "RA", "", ""],
            "prec": [13.2, 1.0, 1.0, 1.0, 1.0, 1.0],
        }
    )
    assert list(precip_type(df)) == [
        "convective",
        "convective",
        "convective",
        "stratiform",
        "convective",
        "convective",
    ]
