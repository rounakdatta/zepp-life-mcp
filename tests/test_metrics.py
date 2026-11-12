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


def test_newest_filed_day_finds_the_real_day_among_junk_rows():
    rows = [
        None,
        "x",
        {"date_time": "not-a-date", "summary": _summary()},
        {"summary": _summary()},
        {"date_time": "2026-11-07", "summary": _summary()},
    ]
    assert newest_filed_day(rows) == date(2026, 11, 7)


# -- UpstreamProbe ------------------------------------------------------------


class _Clock:
    def __init__(self, now: float):
        self.now = now

    def __call__(self) -> float:
        return self.now


# 20:00 UTC on the 11th is 01:30 IST on the 12th: the user's "today" and UTC's
# differ, so a probe that ignored the timezone would ask for the wrong window.
NOW = datetime(2026, 11, 11, 20, 0, tzinfo=UTC).timestamp()


def _creds(token="tok", user="u1"):
    return lambda: (token, user)


async def test_probe_asks_for_a_month_in_the_users_timezone_and_reports_the_newest_day():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_band_response("2026-11-09", "2026-11-10"))

    probe = UpstreamProbe(
        _creds(),
        timezone="Asia/Kolkata",
        transport=httpx.MockTransport(handler),
        clock=_Clock(NOW),
    )
    result = await probe.refresh()

    assert result == ProbeResult(NOW, True, 200, date(2026, 11, 10), NOW)
    params = dict(seen[0].url.params)
    assert params["to_date"] == "2026-11-12"
    # Wide enough that a phone silent for days still has a newest day to report,
    # so "the phone isn't uploading" stays true instead of turning into no data.
    assert params["from_date"] == "2026-10-13"
    assert params["userid"] == "u1"
    assert seen[0].headers["apptoken"] == "tok"


async def test_a_renewed_token_is_used_without_a_restart():
    tokens = iter(["old", "new"])
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["apptoken"])
        return httpx.Response(200, json=_band_response("2026-11-10"))

    probe = UpstreamProbe(
        lambda: (next(tokens), "u1"), transport=httpx.MockTransport(handler), clock=_Clock(NOW)
    )
    await probe.refresh()
    await probe.refresh()
    assert seen == ["old", "new"]


async def test_a_scrape_never_waits_on_zepp():
    import asyncio

    release = asyncio.Event()
    calls = 0

    class Slow(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            nonlocal calls
            calls += 1
            await release.wait()
            return httpx.Response(200, json=_band_response("2026-11-10"))

    clock = _Clock(NOW)
    probe = UpstreamProbe(_creds(), transport=Slow(), interval=600, clock=clock)
    # First scrape: nothing known yet, a refresh starts, and the scrape returns.
    assert probe.snapshot() is None
    await asyncio.sleep(0)
    assert calls == 1
    # Further scrapes while Zepp is slow neither wait nor pile up requests.
    assert probe.snapshot() is None
    await asyncio.sleep(0)
    assert calls == 1
    release.set()
    await probe._task
    assert probe.snapshot().newest_day == date(2026, 11, 10)
    # Not due again for ten minutes.
    clock.now += 599
    probe.snapshot()
    await asyncio.sleep(0)
    assert calls == 1
    clock.now += 1
    probe.snapshot()
    await probe._task
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
        _creds(), transport=httpx.MockTransport(lambda r: response), clock=_Clock(NOW)
    )
    result = await probe.refresh()
    assert result.ok is False
    assert result.http_status == status
    assert result.newest_day is None


async def test_a_network_error_is_not_ok_and_does_not_raise():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("boom", request=request)

    probe = UpstreamProbe(_creds(), transport=httpx.MockTransport(handler), clock=_Clock(NOW))
    assert await probe.refresh() == ProbeResult(NOW, False, None, None)
    assert attempts == 1


async def test_any_other_probe_error_is_contained():
    def handler(request: httpx.Request) -> httpx.Response:
        raise ValueError("a bug, or a malformed host")

    probe = UpstreamProbe(_creds(), transport=httpx.MockTransport(handler), clock=_Clock(NOW))
    assert (await probe.refresh()).ok is False

    def broken_credentials():
        raise UnicodeError("unreadable token file")

    probe = UpstreamProbe(broken_credentials, clock=_Clock(NOW))
    assert (await probe.refresh()).ok is False


async def test_no_token_means_no_request():
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError("probe sent a request without a token")

    probe = UpstreamProbe(
        _creds(token=None), transport=httpx.MockTransport(handler), clock=_Clock(NOW)
    )
    assert await probe.refresh() == ProbeResult(NOW, False, None, None)


async def test_base64_encoded_data_is_understood():
    encoded = base64.b64encode(
        json.dumps(_band_response("2026-11-08", "2026-11-10")["data"]).encode()
    ).decode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 1, "data": encoded})

    probe = UpstreamProbe(_creds(), transport=httpx.MockTransport(handler), clock=_Clock(NOW))
    assert (await probe.refresh()).newest_day == date(2026, 11, 10)


# -- render_metrics -----------------------------------------------------------


def test_render_reports_sync_state_archive_and_upstream(tmp_path):
    db = Database(tmp_path / "z.db")
    db.insert_daily_activity(_activity("2026-11-09"))
    db.insert_daily_activity(_activity("2026-11-10"))
    db.update_sync_state("cloud_session", "u1", "daily_activity", success=True)
    db.update_sync_state("cloud_session", "u1", "sleep", success=False, error="HTTP 401")

    probe = ProbeResult(NOW, True, 200, date(2026, 11, 10))
    text = render_metrics(db, "cloud_session", "u1", probe)

    import time as _time

    # Real times, not just present: CURRENT_TIMESTAMP is now, give or take.
    success = _metric(text, "zepp_sync_last_success_timestamp_seconds", data_type="daily_activity")
    assert success is not None and abs(success - _time.time()) < 120
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
    # The archive is there to be leaked: its newest day is reported...
    assert _metric(
        text, "zepp_archive_newest_day_timestamp_seconds", data_type="daily_activity"
    ) == day_seconds(date(2026, 11, 10))
    # ...but steps are 1000 and distance 900 in the fixture, and neither may appear.
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


def test_each_metric_family_is_one_block(tmp_path):
    # The text format wants a family's samples together under its HELP/TYPE;
    # a strict parser drops the type of a family that is split.
    db = Database(tmp_path / "z.db")
    for data_type in ("daily_activity", "sleep"):
        db.update_sync_state("cloud_session", "u1", data_type, success=True)
    text = render_metrics(db, "cloud_session", "u1", None)
    names = [line.split("{")[0].split(" ")[0] for line in text.splitlines() if not line.startswith("#")]
    blocks = [n for i, n in enumerate(names) if i == 0 or names[i - 1] != n]
    assert len(blocks) == len(set(blocks))
    assert text.count("# TYPE zepp_sync_last_attempt_failed gauge") == 1


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
    # No ZEPP_USER_ID: the chart's default. The id must still be found, from
    # the archive itself, the way the served tools find it.
    monkeypatch.delenv("ZEPP_USER_ID", raising=False)

    seeded = Database(tmp_path / "z.db")
    seeded.insert_daily_activity(_activity("2026-11-10", user_id="u1"))
    seeded.update_sync_state("cloud_session", "u1", "daily_activity", success=True)

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("the route must not wait on Zepp")

    probe = UpstreamProbe(lambda: ("tok", "u1"), transport=httpx.MockTransport(handler))
    probe._last = ProbeResult(datetime.now(UTC).timestamp(), True, 200, date(2026, 11, 10))
    monkeypatch.setattr(server, "_upstream_probe", probe)
    monkeypatch.setattr(server, "context", server.RuntimeContext())

    app = server.build_http_app("secret-bearer", read_only=True)
    with TestClient(app) as client:
        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert metrics.headers["content-type"].startswith("text/plain")
        assert _metric(metrics.text, "zepp_upstream_probe_ok") == 1
        assert (
            _metric(metrics.text, "zepp_sync_last_attempt_failed", data_type="daily_activity") == 0
        )

        for path in ("/mcp", "/mcp/"):
            assert client.post(path, json={}).status_code == 401
        assert client.get("/healthz").status_code == 200


# -- second review ------------------------------------------------------------


async def test_after_a_long_gap_the_old_answer_is_withheld_until_fresh():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_band_response("2026-11-10"))

    clock = _Clock(NOW)
    probe = UpstreamProbe(
        _creds(), transport=httpx.MockTransport(handler), interval=600, clock=clock
    )
    await probe.refresh()
    clock.now += 3 * 24 * 3600  # the prober was down for three days
    # A three-day-old "newest day" would read as a phone that stopped uploading.
    assert probe.snapshot() is None
    await probe._task
    assert probe.snapshot().checked_at == clock.now


async def test_a_failed_probe_keeps_the_last_good_day():
    replies = iter(
        [
            httpx.Response(200, json=_band_response("2026-11-10")),
            httpx.Response(502, text="bad gateway"),
        ]
    )
    clock = _Clock(NOW)
    probe = UpstreamProbe(
        _creds(), transport=httpx.MockTransport(lambda r: next(replies)), clock=clock
    )
    await probe.refresh()
    clock.now += 600
    failed = await probe.refresh()
    # One bad request must not make the day vanish: a check watching it with a
    # "for" would restart its timer and never fire.
    assert failed.ok is False and failed.http_status == 502
    assert failed.newest_day == date(2026, 11, 10)
    assert failed.newest_day_at == NOW


def test_sqlite_timestamps_are_utc_whatever_the_local_zone():
    from zepp_life_mcp.metrics import _sqlite_utc_seconds

    assert _sqlite_utc_seconds("2026-11-11 17:15:13") == datetime(
        2026, 11, 11, 17, 15, 13, tzinfo=UTC
    ).timestamp()


def test_workout_details_sync_is_reported(tmp_path):
    db = Database(tmp_path / "z.db")
    db.update_sync_state("cloud_session", "u1", "workout_details", success=False, error="HTTP 500")
    text = render_metrics(db, "cloud_session", "u1", None)
    assert _metric(text, "zepp_sync_last_attempt_failed", data_type="workout_details") == 1


def test_the_server_rereads_the_token_file_on_every_probe(tmp_path, monkeypatch):
    token_file = tmp_path / "apptoken"
    token_file.write_text("old\n")
    monkeypatch.delenv("ZEPP_APP_TOKEN", raising=False)
    monkeypatch.setenv("ZEPP_APP_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("ZEPP_USER_ID", "u1")
    monkeypatch.setattr(server, "_metrics_user", None)
    assert server._probe_credentials() == ("old", "u1")
    token_file.write_text("renewed\n")
    assert server._probe_credentials() == ("renewed", "u1")


def test_the_server_builds_the_probe_in_the_users_timezone(monkeypatch):
    from zepp_life_mcp.config import Config

    ctx = server.RuntimeContext()
    ctx.config = Config(mode="cloud_session", timezone="Asia/Kolkata")
    monkeypatch.setattr(server, "context", ctx)
    monkeypatch.setattr(server, "_upstream_probe", None)
    probe = server._metrics_probe()
    assert probe is not None and probe._tz.key == "Asia/Kolkata"


def test_a_due_refresh_never_delays_the_scrape(tmp_path, monkeypatch):
    import asyncio
    import time as _time

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("ZEPP_DATABASE_PATH", str(tmp_path / "z.db"))
    monkeypatch.setenv("ZEPP_MODE", "cloud_session")
    monkeypatch.setenv("ZEPP_APP_TOKEN", "tok")
    monkeypatch.setenv("ZEPP_USER_ID", "u1")
    monkeypatch.setattr(server, "_metrics_user", None)
    monkeypatch.setattr(server, "context", server.RuntimeContext())

    class Slow(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            await asyncio.sleep(2)
            return httpx.Response(200, json=_band_response("2026-11-10"))

    # Nothing cached: the first scrape is due a refresh, against a Zepp that
    # takes two seconds.
    monkeypatch.setattr(server, "_upstream_probe", UpstreamProbe(lambda: ("tok", "u1"), transport=Slow()))
    app = server.build_http_app("secret-bearer", read_only=True)
    with TestClient(app) as client:
        start = _time.monotonic()
        assert client.get("/metrics").status_code == 200
        assert _time.monotonic() - start < 1.0


def test_an_unreadable_token_file_does_not_break_the_scrape(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "_metrics_user", None)
    monkeypatch.setenv("ZEPP_USER_ID", "u1")
    bad = tmp_path / "apptoken"
    bad.write_bytes(b"\xff\xfe\xfa")
    monkeypatch.delenv("ZEPP_APP_TOKEN", raising=False)
    monkeypatch.setenv("ZEPP_APP_TOKEN_FILE", str(bad))
    # The route only needs the user id; the token is the probe's business, and
    # the probe contains its own errors.
    assert server._metrics_user_id() == "u1"
