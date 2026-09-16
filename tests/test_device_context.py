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


def _hr_db(tmp_path, n_passive=500):
    """A database with enough passive samples to matter."""
    from datetime import timedelta

    from zepp_life_mcp.models import HeartRateSample

    db = Database(tmp_path / "hr.db")
    base = datetime(2026, 7, 1, tzinfo=UTC)
    for i in range(n_passive):
        ts = base + timedelta(minutes=i)
        db.upsert_heart_rate_sample(
            HeartRateSample(
                id=f"p{i}", provider="zepp_life", source_type="cloud_session", user_id="u1",
                timestamp=ts, local_date=ts.date().isoformat(), bpm=70, sample_type="passive",
            )
        )
    db.upsert_heart_rate_sample(
        HeartRateSample(
            id="r1", provider="zepp_life", source_type="cloud_session", user_id="u1",
            timestamp=base, local_date=base.date().isoformat(), bpm=48, sample_type="resting",
        )
    )
    return db


def test_heart_rate_filter_and_bound_happen_in_sql(tmp_path):
    """Regression: this query read every row before filtering, and OOM-killed the pod.

    Passive heart rate is one sample a minute, so a wide range is hundreds of
    thousands of rows. Selecting them all and filtering in Python materialises
    every one as a dict before the caller's limit applies -- a container with a
    memory limit simply dies, which is what happened in production.
    """
    db = _hr_db(tmp_path)

    # the filter reaches the database, not a Python loop over everything
    resting = db.query_heart_rate_samples("u1", "2026-07-01", "2026-07-31",
                                          sample_type="resting")
    assert len(resting) == 1
    assert resting[0]["bpm"] == 48

    # and the bound does too
    bounded = db.query_heart_rate_samples("u1", "2026-07-01", "2026-07-31", limit=25)
    assert len(bounded) == 25


def test_heart_rate_query_is_bounded_even_when_no_limit_is_asked_for(tmp_path, monkeypatch):
    from zepp_life_mcp.services import query_service as qs

    db = _hr_db(tmp_path)
    monkeypatch.setattr(qs, "DEFAULT_HEART_RATE_LIMIT", 40)
    samples = qs.QueryService(db, "u1").get_heart_rate_samples("2026-07-01", "2026-07-31")
    assert len(samples) == 40, "an unbounded request must still be bounded"


def test_an_absurd_limit_is_capped(tmp_path, monkeypatch):
    from zepp_life_mcp.services import query_service as qs

    db = _hr_db(tmp_path)
    monkeypatch.setattr(qs, "MAX_HEART_RATE_LIMIT", 10)
    samples = qs.QueryService(db, "u1").get_heart_rate_samples(
        "2026-07-01", "2026-07-31", limit=10_000_000
    )
    assert len(samples) == 10, "one query must not be able to kill the process"


class DeviceClient:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status

    async def get(self, url, params=None, **kwargs):
        r = FakeResponse(self.payload)
        r.status_code = self.status
        return r


async def test_get_profile_lists_real_devices():
    """It reported `devices: []` behind a TODO while the endpoint answered fine."""
    adapter = CloudSessionAdapter(app_token="t", user_id="u1")
    cast(Any, adapter)._client = DeviceClient({"items": [{
        "deviceId": "D8803CFFFEC173D4", "deviceType": 0, "deviceSource": 8716547,
        "macAddress": "D8:80:3C:C1:73:D4", "firmwareVersion": "6.3.25.7"}]})
    adapter._connected = True

    devices = await adapter.get_devices()
    assert len(devices) == 1
    assert devices[0]["device_id"] == "D8803CFFFEC173D4"
    assert devices[0]["firmware_version"] == "6.3.25.7"


async def test_device_listing_failure_is_not_fatal():
    """A profile without a device list is still a useful profile."""
    adapter = CloudSessionAdapter(app_token="t", user_id="u1")
    cast(Any, adapter)._client = DeviceClient({}, status=500)
    adapter._connected = True
    assert await adapter.get_devices() == []


async def test_workout_pace_and_effort_reach_the_model():
    """Regression: pace, steps, VO2max and training effect were all discarded.

    Upstream reports pace in seconds per METRE. Verified against real runs:
    avg_pace 0.3831 over 10.02 km in 64 min is 383 s/km, i.e. 6:23/km.
    """
    items = [workout_item(avg_pace="0.3831159", max_pace="0.246",
                          total_step="9629", VO2_max="54", te="45")]
    w = [x async for x in _workout_adapter(items).iter_workouts()][0]
    assert round(w.avg_pace_sec_per_km) == 383
    assert round(w.max_pace_sec_per_km) == 246, "upstream 'max' pace is the FASTEST"
    assert w.total_steps == 9629
    assert w.vo2max == 54
    assert w.training_effect == 45


async def test_unrecorded_effort_fields_are_absent_not_zero():
    """Upstream writes 0 and -1 for 'not recorded'; storing those poisons averages."""
    items = [workout_item(avg_pace="0", max_pace="-1", total_step="0", VO2_max="", te=None)]
    w = [x async for x in _workout_adapter(items).iter_workouts()][0]
    assert w.avg_pace_sec_per_km is None
    assert w.max_pace_sec_per_km is None
    assert w.total_steps is None
    assert w.vo2max is None
    assert w.training_effect is None


def test_api_host_is_the_real_knob_and_region_is_not():
    """`region` never selected a host; pretending it did would break working setups."""
    default = CloudSessionAdapter(app_token="t", user_id="u1", region="eu")
    assert default.api_host == CloudSessionAdapter.ZEPP_API_BASE

    # changing region alone changes nothing about where requests go
    other = CloudSessionAdapter(app_token="t", user_id="u1", region="us")
    assert other.api_host == default.api_host

    # api_host does
    override = CloudSessionAdapter(
        app_token="t", user_id="u1", api_host="https://api-mifit-de2.huami.com"
    )
    assert override.api_host == "https://api-mifit-de2.huami.com"


async def test_get_profile_answers_without_a_live_connection(tmp_path, monkeypatch):
    """Regression: the only tool that could not answer on a read-only instance.

    It demanded `adapter.is_connected()` before returning anything, even though
    the user id and timezone live in the local database -- so on the deployment's
    actual configuration it returned an error instead of a profile.
    """
    from zepp_life_mcp import server
    from zepp_life_mcp.models import DailyActivity

    db = Database(tmp_path / "prof.db")
    db.upsert_daily_activity(DailyActivity(
        id="a", provider="zepp_life", source_type="cloud_session", user_id="3308073311",
        date="2026-09-01", steps=1, distance_m=1, active_kcal=1))

    class Disconnected:
        def is_connected(self): return False
        def get_user_id(self): return None

    monkeypatch.setattr(server.context, "db", db)
    monkeypatch.setattr(server.context, "adapter", cast(Any, Disconnected()))
    monkeypatch.setattr(server.context, "read_only", True)

    out = await server._handle_get_profile({"include_devices": True})
    assert out["status"] == "ok"
    assert out["data"]["profile"]["user_id"] == "3308073311"
    assert out["data"]["profile"]["devices"] == [], "devices need upstream; the rest does not"
