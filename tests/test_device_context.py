"""Location, algorithm version, naps and the aggregated/raw escape hatches."""

from datetime import UTC, datetime
from typing import Any, cast

from zepp_life_mcp.adapters.cloud_session import CloudSessionAdapter
from zepp_life_mcp.services.query_service import QueryService
from zepp_life_mcp.storage import Database


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def band_day(date_str, *, tz, start, end, algo="4.0.17", stage=None):
    return {
        "date_time": date_str,
        "summary": {
            "tz": str(tz),
            "stp": {"ttl": 9000, "dis": 6000, "cal": 300},
            "slp": {
                "st": start,
                "ed": end,
                "sleepAlgoVersion": algo,
                "stage": stage
                if stage is not None
                else [
                    {"mode": 5, "start": 0, "stop": 59},
                    {"mode": 4, "start": 60, "stop": 299},
                    {"mode": 8, "start": 300, "stop": 359},
                ],
            },
        },
    }


class BandClient:
    def __init__(self, days):
        self.days = days

    async def get(self, url, params=None, **kwargs):
        return FakeResponse({"data": self.days})


def _adapter(days, timezone="UTC"):
    adapter = CloudSessionAdapter(app_token="t", user_id="u1", timezone=timezone)
    cast(Any, adapter)._client = BandClient(days)
    adapter._connected = True
    return adapter


NIGHT_START = int(datetime(2026, 7, 14, 19, 0, tzinfo=UTC).timestamp())   # 00:30 IST-ish
NIGHT_END = int(datetime(2026, 7, 15, 3, 0, tzinfo=UTC).timestamp())


async def test_timezone_offset_and_algorithm_version_are_captured():
    """Without these, 'where was I' and 'is this comparable' are unanswerable."""
    days = [band_day("2026-07-15", tz=-18000, start=NIGHT_START, end=NIGHT_END, algo="4.0.17")]
    adapter = _adapter(days)

    sleep = [s async for s in adapter.iter_sleep_sessions("2026-07-15", "2026-07-15")]
    assert sleep[0].tz_offset_seconds == -18000
    assert sleep[0].algo_version == "4.0.17"

    activity = [a async for a in adapter.iter_daily_activity("2026-07-15", "2026-07-15")]
    assert activity[0].tz_offset_seconds == -18000


async def test_deep_and_light_minutes_survive_to_the_model():
    """rem had a column and deep did not, so deep sleep needed JSON parsing to reach."""
    days = [band_day("2026-07-15", tz=19800, start=NIGHT_START, end=NIGHT_END)]
    sleep = [s async for s in _adapter(days).iter_sleep_sessions("2026-07-15", "2026-07-15")]
    assert sleep[0].deep_minutes == 60
    assert sleep[0].light_minutes == 240
    assert sleep[0].rem_minutes == 60


async def test_naps_are_inferred_from_local_hour_not_guessed_globally():
    """Zepp has no nap flag, so the rule is explicit: short AND starting in daylight."""
    # 08:00-09:40 local in UTC+5:30 -> a 100-minute daytime sleep
    nap_start = int(datetime(2026, 7, 15, 2, 30, tzinfo=UTC).timestamp())
    nap_end = int(datetime(2026, 7, 15, 4, 10, tzinfo=UTC).timestamp())
    nap = band_day("2026-07-15", tz=19800, start=nap_start, end=nap_end,
                   stage=[{"mode": 4, "start": 0, "stop": 99}])
    got = [s async for s in _adapter([nap]).iter_sleep_sessions("2026-07-15", "2026-07-15")]
    assert got[0].is_nap is True

    # the same duration overnight is not a nap
    night = band_day("2026-07-16", tz=19800,
                     start=int(datetime(2026, 7, 15, 19, 0, tzinfo=UTC).timestamp()),
                     end=int(datetime(2026, 7, 15, 20, 40, tzinfo=UTC).timestamp()),
                     stage=[{"mode": 4, "start": 0, "stop": 99}])
    got = [s async for s in _adapter([night]).iter_sleep_sessions("2026-07-16", "2026-07-16")]
    assert got[0].is_nap is False

    # a full night is never a nap
    full = band_day("2026-07-17", tz=19800, start=NIGHT_START, end=NIGHT_END)
    got = [s async for s in _adapter([full]).iter_sleep_sessions("2026-07-17", "2026-07-17")]
    assert got[0].is_nap is False


async def test_no_offset_means_no_nap_classification():
    """A local hour cannot be inferred without an offset, so nothing is claimed."""
    day = band_day("2026-07-15", tz="not-a-number",
                   start=int(datetime(2026, 7, 15, 2, 30, tzinfo=UTC).timestamp()),
                   end=int(datetime(2026, 7, 15, 4, 10, tzinfo=UTC).timestamp()),
                   stage=[{"mode": 4, "start": 0, "stop": 99}])
    got = [s async for s in _adapter([day]).iter_sleep_sessions("2026-07-15", "2026-07-15")]
    assert got[0].tz_offset_seconds is None
    assert got[0].is_nap is False


async def test_sleep_query_returns_the_date_and_place_it_already_stored(tmp_path):
    """Regression: local_date existed in the row and was never in the response."""
    db = Database(tmp_path / "q.db")
    days = [band_day("2026-07-15", tz=-18000, start=NIGHT_START, end=NIGHT_END)]
    async for sleep in _adapter(days).iter_sleep_sessions("2026-07-15", "2026-07-15"):
        db.upsert_sleep_session(sleep)

    sessions = QueryService(db, "u1").get_sleep_sessions("2026-07-01", "2026-07-31")
    assert sessions, "sleep should be queryable"
    s = sessions[0]
    assert s["local_date"] is not None
    assert s["tz_offset_seconds"] == -18000
    assert s["algo_version"] == "4.0.17"
    assert s["deep_minutes"] == 60


async def test_sleep_metrics_come_back_aggregated(tmp_path):
    """A year of nights as a few numbers, not a few hundred kilobytes of sessions."""
    db = Database(tmp_path / "m.db")
    days = [band_day("2026-07-15", tz=19800, start=NIGHT_START, end=NIGHT_END)]
    async for sleep in _adapter(days).iter_sleep_sessions("2026-07-15", "2026-07-15"):
        db.upsert_sleep_session(sleep)
    q = QueryService(db, "u1")

    for metric in ("sleep_minutes", "sleep_deep_minutes", "sleep_rem_minutes", "sleep_score"):
        series = q.get_metric_series(metric, "2026-07-01", "2026-07-31", "month", "avg")
        assert series, f"{metric} should produce a series"
    deep = q.get_metric_series("sleep_deep_minutes", "2026-07-01", "2026-07-31", "month", "avg")
    assert deep[0]["value"] == 60


async def test_raw_archive_is_reachable_without_opening_the_sqlite_file(tmp_path):
    db = Database(tmp_path / "r.db")
    db.record_raw_payload(
        source_type="cloud_session", user_id="u1", endpoint="sport.run.detail",
        payload={"data": {"longitude_latitude": "1,2;3,4"}}, record_id="track-9",
        window_start="2026-07-15", window_end="2026-07-15", http_status=200,
    )
    q = QueryService(db, "u1")

    rows = q.get_raw_payloads(endpoint="sport.run.detail")
    assert len(rows) == 1
    assert rows[0]["record_id"] == "track-9"
    assert "payload" not in rows[0], "bodies are large; omitted unless asked for"

    rows = q.get_raw_payloads(endpoint="sport.run.detail", include_payload=True)
    assert rows[0]["payload"]["data"]["longitude_latitude"] == "1,2;3,4"


class WorkoutHistoryClient:
    def __init__(self, items):
        self.items = items

    async def get(self, url, params=None, **kwargs):
        return FakeResponse({"data": {"summary": self.items}})


def _workout_adapter(items):
    adapter = CloudSessionAdapter(app_token="t", user_id="u1", timezone="Asia/Kolkata")
    cast(Any, adapter)._client = WorkoutHistoryClient(items)
    adapter._connected = True
    return adapter


def workout_item(**over):
    base = {
        "trackid": "t1",
        "type": "1",
        "end_time": str(int(datetime(2026, 8, 20, 2, 0, tzinfo=UTC).timestamp())),
        "run_time": "3600",
        "dis": "10000",
    }
    base.update(over)
    return base


async def test_workout_place_is_taken_from_the_workout_not_guessed():
    """syncedTimezone, city and geohash were all present upstream and all discarded."""
    items = [workout_item(syncedTimezone="America/Los_Angeles", city="San Francisco",
                          location="9q8yyk8ytpxr")]
    got = [w async for w in _workout_adapter(items).iter_workouts()]
    w = got[0]
    assert w.timezone == "America/Los_Angeles"
    assert w.city == "San Francisco"
    assert w.geohash == "9q8yyk8ytpxr"
    # offset derived from the zone AT THAT INSTANT, so DST is handled for free
    assert w.tz_offset_seconds == -25200


async def test_workout_without_a_zone_falls_back_to_the_configured_one():
    got = [w async for w in _workout_adapter([workout_item()]).iter_workouts()]
    assert got[0].timezone == "Asia/Kolkata"
    assert got[0].tz_offset_seconds is None
    assert got[0].city is None


async def test_unrecognised_workout_zone_does_not_break_the_sync():
    items = [workout_item(syncedTimezone="Mars/Olympus_Mons", city="Elysium")]
    got = [w async for w in _workout_adapter(items).iter_workouts()]
    assert got[0].timezone == "Asia/Kolkata"
    assert got[0].tz_offset_seconds is None
    assert got[0].city == "Elysium", "a bad zone must not discard the rest of the place"
