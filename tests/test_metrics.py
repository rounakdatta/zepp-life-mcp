"""/metrics: the sync watched from outside, without a credential that opens the archive."""

import base64
import json
from datetime import UTC, date, datetime

import httpx
import pytest
from starlette.testclient import TestClient

from zepp_life_mcp import server
from zepp_life_mcp.metrics import (
    ProbeResult,
    UpstreamProbe,
    day_seconds,
    newest_filed_day,
    render_metrics,
)
from zepp_life_mcp.models import DailyActivity
from zepp_life_mcp.storage import Database


def _summary(steps: int = 1200) -> str:
    return base64.b64encode(json.dumps({"stp": {"ttl": steps}}).encode()).decode()


def _band_response(*days: str, empty: tuple[str, ...] = ()) -> dict:
    rows = [{"date_time": d, "summary": _summary()} for d in days]
    rows += [{"date_time": d, "summary": ""} for d in empty]
    return {"code": 1, "message": "success", "data": rows}


def _activity(day: str, user_id: str = "u1") -> DailyActivity:
    return DailyActivity(
        id=f"a-{day}",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id=user_id,
        device_id=None,
        collected_at=None,
        date=day,
        steps=1000,
        distance_m=900,
        active_kcal=60,
        total_kcal=None,
        floors=None,
        active_minutes=None,
    )


def _metric(text: str, name: str, **labels: str) -> float | None:
    wanted = name + (
        "{" + ",".join(f'{k}="{v}"' for k, v in sorted(labels.items())) + "}" if labels else ""
    )
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        key, _, value = line.rpartition(" ")
        if key == wanted:
            return float(value)
    return None


# -- newest_filed_day ---------------------------------------------------------


def test_newest_filed_day_ignores_days_without_a_summary():
    # Zepp can list a day before the phone has sent anything for it; the sync
    # does not count such a day, so the probe must not either, or the archive
    # would look a day behind forever.
    rows = _band_response("2026-11-09", "2026-11-10", empty=("2026-11-11",))["data"]
    assert newest_filed_day(rows) == date(2026, 11, 10)


def test_newest_filed_day_survives_junk_rows():
    rows = [None, "x", {"date_time": "not-a-date", "summary": _summary()}, {"summary": _summary()}]
    assert newest_filed_day(rows) is None


# -- UpstreamProbe ------------------------------------------------------------


class _Clock:
    def __init__(self, now: float):
        self.now = now

    def __call__(self) -> float:
        return self.now


NOW = datetime(2026, 11, 11, 17, 30, tzinfo=UTC).timestamp()


async def test_probe_asks_for_a_small_window_in_the_users_timezone_and_reports_the_newest_day():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_band_response("2026-11-09", "2026-11-10"))

    probe = UpstreamProbe(
        "tok",
        "u1",
        timezone="Asia/Kolkata",
        transport=httpx.MockTransport(handler),
        clock=_Clock(NOW),
    )
    result = await probe.result()

    assert result == ProbeResult(NOW, True, 200, date(2026, 11, 10))
    params = dict(seen[0].url.params)
    # 17:30 UTC is 23:00 IST on the 11th: "today" is the user's day, not UTC's.
    assert params["to_date"] == "2026-11-11"
    assert params["from_date"] == "2026-11-09"
    assert params["userid"] == "u1"
    assert seen[0].headers["apptoken"] == "tok"


async def test_probe_caches_between_intervals():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=_band_response("2026-11-10"))

    clock = _Clock(NOW)
    probe = UpstreamProbe(
        "tok", "u1", interval=600, transport=httpx.MockTransport(handler), clock=clock
    )
    await probe.result()
    clock.now += 599
    await probe.result()
    assert calls == 1
    clock.now += 1
    await probe.result()
    assert calls == 2


@pytest.mark.parametrize(
    ("response", "status"),
    [
        (httpx.Response(401, json={"code": 0, "message": "invalid token"}), 401),
        (httpx.Response(200, json={"code": 0, "message": "invalid token"}), 200),
        (httpx.Response(200, text="<html>not json</html>"), 200),
        (httpx.Response(200, json={"code": 1, "data": {"unexpected": "shape"}}), 200),
    ],
)
async def test_a_rejected_or_odd_answer_is_not_ok(response, status):
    probe = UpstreamProbe(
        "tok", "u1", transport=httpx.MockTransport(lambda r: response), clock=_Clock(NOW)
    )
    result = await probe.result()
    assert result.ok is False
    assert result.http_status == status
    assert result.newest_day is None


async def test_a_network_error_is_not_ok_and_does_not_raise():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    probe = UpstreamProbe("tok", "u1", transport=httpx.MockTransport(handler), clock=_Clock(NOW))
    assert await probe.result() == ProbeResult(NOW, False, None, None)


async def test_no_token_means_no_request():
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError("probe sent a request without a token")

    probe = UpstreamProbe(None, "u1", transport=httpx.MockTransport(handler), clock=_Clock(NOW))
    assert (await probe.result()).ok is False


async def test_base64_encoded_data_is_understood():
    encoded = base64.b64encode(
        json.dumps(_band_response("2026-11-08", "2026-11-10")["data"]).encode()
    ).decode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 1, "data": encoded})

    probe = UpstreamProbe("tok", "u1", transport=httpx.MockTransport(handler), clock=_Clock(NOW))
    assert (await probe.result()).newest_day == date(2026, 11, 10)


# -- render_metrics -----------------------------------------------------------


def test_render_reports_sync_state_archive_and_upstream(tmp_path):
    db = Database(tmp_path / "z.db")
    db.insert_daily_activity(_activity("2026-11-09"))
    db.insert_daily_activity(_activity("2026-11-10"))
    db.update_sync_state("cloud_session", "u1", "daily_activity", success=True)
    db.update_sync_state("cloud_session", "u1", "sleep", success=False, error="HTTP 401")

    probe = ProbeResult(NOW, True, 200, date(2026, 11, 10))
    text = render_metrics(db, "cloud_session", "u1", probe)

    assert _metric(text, "zepp_sync_last_attempt_failed", data_type="daily_activity") == 0
    assert _metric(text, "zepp_sync_last_attempt_failed", data_type="sleep") == 1
    # A failed pass records an attempt but never a success.
    assert _metric(text, "zepp_sync_last_success_timestamp_seconds", data_type="sleep") is None
    assert _metric(text, "zepp_sync_last_attempt_timestamp_seconds", data_type="sleep") is not None
    assert _metric(
        text, "zepp_archive_newest_day_timestamp_seconds", data_type="daily_activity"
    ) == day_seconds(date(2026, 11, 10))
    assert _metric(text, "zepp_upstream_newest_day_timestamp_seconds") == day_seconds(
        date(2026, 11, 10)
    )
    assert _metric(text, "zepp_upstream_probe_ok") == 1
    assert _metric(text, "zepp_upstream_probe_http_status") == 200


def test_render_never_exposes_archive_values(tmp_path):
    db = Database(tmp_path / "z.db")
    db.insert_daily_activity(_activity("2026-11-10"))
    db.update_sync_state("cloud_session", "u1", "daily_activity", success=True)
    text = render_metrics(db, "cloud_session", "u1", ProbeResult(NOW, True, 200, None))
    # Steps are 1000 and distance 900 in the fixture; neither may appear.
    values = {line.rpartition(" ")[2] for line in text.splitlines() if not line.startswith("#")}
    assert "1000" not in values and "900" not in values
    # And no upstream day when Zepp had none to report.
    assert _metric(text, "zepp_upstream_newest_day_timestamp_seconds") is None


def test_a_failed_probe_drops_the_upstream_day_but_reports_why(tmp_path):
    db = Database(tmp_path / "z.db")
    text = render_metrics(db, "cloud_session", "u1", ProbeResult(NOW, False, 401, None))
    assert _metric(text, "zepp_upstream_probe_ok") == 0
    assert _metric(text, "zepp_upstream_probe_http_status") == 401
    assert _metric(text, "zepp_upstream_newest_day_timestamp_seconds") is None


def test_the_exposition_is_parseable(tmp_path):
    db = Database(tmp_path / "z.db")
    db.insert_daily_activity(_activity("2026-11-10"))
    db.update_sync_state("cloud_session", "u1", "daily_activity", success=True)
    text = render_metrics(db, "cloud_session", "u1", ProbeResult(NOW, True, 200, None))
    assert text.endswith("\n")
    for line in text.splitlines():
        if line.startswith("# HELP ") or line.startswith("# TYPE "):
            continue
        name_and_labels, _, value = line.rpartition(" ")
        float(value)
        assert name_and_labels.startswith("zepp_")


# -- the HTTP route -----------------------------------------------------------


def test_metrics_needs_no_bearer_but_mcp_still_does(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("ZEPP_DATABASE_PATH", str(tmp_path / "z.db"))
    monkeypatch.setenv("ZEPP_MODE", "cloud_session")
    monkeypatch.setenv("ZEPP_APP_TOKEN", "tok")
    monkeypatch.setenv("ZEPP_USER_ID", "u1")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_band_response("2026-11-10"))

    monkeypatch.setattr(
        server,
        "_upstream_probe",
        UpstreamProbe("tok", "u1", transport=httpx.MockTransport(handler)),
    )
    monkeypatch.setattr(server, "context", server.RuntimeContext())

    app = server.build_http_app("secret-bearer", read_only=True)
    with TestClient(app) as client:
        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert metrics.headers["content-type"].startswith("text/plain")
        assert _metric(metrics.text, "zepp_upstream_probe_ok") == 1

        assert client.post("/mcp", json={}).status_code == 401
        assert client.get("/healthz").status_code == 200
