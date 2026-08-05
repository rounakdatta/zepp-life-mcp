from datetime import UTC, datetime
from typing import Any, cast

import pytest

from zepp_life_mcp.adapters.cloud_session import SPORT_TYPE_MAP, CloudSessionAdapter
from zepp_life_mcp.storage import Database


def test_decode_band_summary_from_dict_and_base64():
    adapter = CloudSessionAdapter(app_token="t1", user_id="u1")
    assert adapter._decode_band_summary({"stp": {"ttl": 1}}) == {"stp": {"ttl": 1}}


def test_parse_heart_rate_bytes():
    adapter = CloudSessionAdapter(app_token="t1", user_id="u1")
    import base64

    samples = adapter._parse_heart_rate_data(
        base64.b64encode(bytes([0, 60, 61, 255, 254, 30])).decode()
    )
    assert samples == [(1, 60), (2, 61), (5, 30)]


@pytest.mark.asyncio
async def test_get_user_info_requires_client():
    adapter = CloudSessionAdapter(app_token="t1", user_id="u1")
    assert await adapter._get_user_info() is None


def test_unix_timestamp_is_utc_and_local_date_handles_dst_boundaries():
    adapter = CloudSessionAdapter(
        app_token="t1",
        user_id="u1",
        timezone="Europe/Berlin",
    )
    spring = datetime(2024, 3, 30, 23, 30, tzinfo=UTC)
    autumn = datetime(2024, 10, 26, 22, 30, tzinfo=UTC)

    parsed = adapter._utc_from_timestamp(spring.timestamp())

    assert parsed == spring
    assert parsed.tzinfo is UTC
    assert adapter._local_date(spring) == "2024-03-31"
    assert adapter._local_date(autumn) == "2024-10-27"


class WorkoutResponse:
    status_code = 200

    def __init__(self, start_time, sport_type=1):
        self.start_time = start_time
        self.sport_type = sport_type

    def json(self):
        return {
            "data": {
                "summary": [
                    {
                        "trackid": "workout-1",
                        "type": self.sport_type,
                        "start_time": self.start_time,
                        "run_time": 600,
                    }
                ]
            }
        }


class WorkoutClient:
    def __init__(self, start_time, sport_type=1):
        self.start_time = start_time
        self.sport_type = sport_type

    async def get(self, *args, **kwargs):
        return WorkoutResponse(self.start_time, self.sport_type)


async def test_workout_range_filter_uses_configured_local_date():
    adapter = CloudSessionAdapter(
        app_token="t1",
        user_id="u1",
        timezone="Europe/Berlin",
    )
    start_at = datetime(2024, 3, 30, 23, 30, tzinfo=UTC)
    cast(Any, adapter)._client = WorkoutClient(start_at.timestamp())
    adapter._connected = True

    workouts = [
        workout
        async for workout in adapter.iter_workouts(
            start_date="2024-03-31",
            end_date="2024-03-31",
        )
    ]

    assert len(workouts) == 1
    assert workouts[0].start_at == start_at
    assert workouts[0].start_at.tzinfo is UTC
    assert workouts[0].local_date == "2024-03-31"
    assert workouts[0].timezone == "Europe/Berlin"


@pytest.mark.parametrize(("sport_type", "expected"), SPORT_TYPE_MAP.items())
async def test_known_sport_types_map_to_readable_names(sport_type, expected):
    adapter = CloudSessionAdapter(app_token="t1", user_id="u1")
    start_at = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    cast(Any, adapter)._client = WorkoutClient(start_at.timestamp(), sport_type)
    adapter._connected = True

    workouts = [workout async for workout in adapter.iter_workouts()]

    assert workouts[0].activity_type == expected


async def test_unknown_sport_type_preserves_raw_numeric_string():
    adapter = CloudSessionAdapter(app_token="t1", user_id="u1")
    start_at = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    cast(Any, adapter)._client = WorkoutClient(start_at.timestamp(), 999)
    adapter._connected = True

    workouts = [workout async for workout in adapter.iter_workouts()]

    assert workouts[0].activity_type == "999"


class HeartRateResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class BandSummaryClient:
    async def get(self, *args, **kwargs):
        return HeartRateResponse(
            {
                "data": [
                    {
                        "date_time": "2024-01-02",
                        "summary": {"stp": {"ttl": 123, "dis": 456, "cal": 7}},
                    }
                ]
            }
        )


class FailingBandClient:
    async def get(self, *args, **kwargs):
        raise RuntimeError("network unavailable")


async def test_cloud_fetch_failure_raises_typed_adapter_error():
    adapter = CloudSessionAdapter(app_token="t1", user_id="user-a")
    cast(Any, adapter)._client = FailingBandClient()
    adapter._connected = True

    with pytest.raises(RuntimeError, match="daily activity") as error:
        _ = [
            activity
            async for activity in adapter.iter_daily_activity(
                start_date="2024-01-02",
                end_date="2024-01-02",
            )
        ]

    assert type(error.value).__name__ == "AdapterFetchError"


async def test_cloud_activity_ids_are_scoped_per_user_and_persist_isolated(tmp_path):
    activities = []
    for user_id in ("user-a", "user-b"):
        adapter = CloudSessionAdapter(app_token="t1", user_id=user_id)
        cast(Any, adapter)._client = BandSummaryClient()
        adapter._connected = True
        activities.extend(
            [
                activity
                async for activity in adapter.iter_daily_activity(
                    start_date="2024-01-02",
                    end_date="2024-01-02",
                )
            ]
        )

    assert [activity.id for activity in activities] == [
        "cloud_user-a_2024-01-02",
        "cloud_user-b_2024-01-02",
    ]

    database = Database(tmp_path / "zepp.db")
    for activity in activities:
        database.upsert_daily_activity(activity)

    with database._get_connection() as conn:
        rows = conn.execute(
            "SELECT id, user_id, date FROM daily_activity ORDER BY user_id"
        ).fetchall()

    assert [tuple(row) for row in rows] == [
        ("cloud_user-a_2024-01-02", "user-a", "2024-01-02"),
        ("cloud_user-b_2024-01-02", "user-b", "2024-01-02"),
    ]


class HeartRateClient:
    def __init__(self, sleep_end):
        self.sleep_end = sleep_end

    async def get(self, *args, **kwargs):
        if kwargs["params"]["query_type"] == "summary":
            return HeartRateResponse(
                {
                    "data": [
                        {
                            "date_time": "2024-03-31",
                            "summary": {"slp": {"ed": self.sleep_end, "rhr": 54}},
                        }
                    ]
                }
            )
        return HeartRateResponse({"data": []})


async def test_sleep_summary_emits_resting_heart_rate_sample():
    adapter = CloudSessionAdapter(
        app_token="t1",
        user_id="u1",
        timezone="Europe/Berlin",
    )
    sleep_end = datetime(2024, 3, 31, 5, 30, tzinfo=UTC)
    cast(Any, adapter)._client = HeartRateClient(sleep_end.timestamp())
    adapter._connected = True

    samples = [
        sample
        async for sample in adapter.iter_heart_rate(
            start_date="2024-03-31",
            end_date="2024-03-31",
        )
    ]

    assert len(samples) == 1
    assert samples[0].bpm == 54
    assert samples[0].sample_type == "resting"
    assert samples[0].timestamp == sleep_end
    assert samples[0].local_date == "2024-03-31"


class UserIdClient:
    def __init__(self):
        self.params = None

    async def get(self, *args, **kwargs):
        self.params = kwargs["params"]
        return HeartRateResponse({"data": [{"uid": 123456789}]})


async def test_user_id_discovery_uses_bounded_window_and_numeric_uid(monkeypatch):
    adapter = CloudSessionAdapter(app_token="t1")
    client = UserIdClient()
    cast(Any, adapter)._client = client

    user_id = await adapter._discover_user_id()

    assert user_id == "123456789"
    assert client.params is not None
    assert client.params["from_date"] != "2020-01-01"


async def test_connect_persists_discovered_user_id(monkeypatch):
    adapter = CloudSessionAdapter(app_token="t1")
    saved = []
    monkeypatch.setattr(adapter, "_get_user_info", lambda: _async_value({"valid": True}))
    monkeypatch.setattr(adapter, "_discover_user_id", lambda: _async_value("123456789"))
    monkeypatch.setattr(adapter, "_discover_data_types", lambda: _async_value([]))
    monkeypatch.setattr("zepp_life_mcp.adapters.cloud_session.save_user_id", saved.append)

    connected = await adapter.connect()
    await adapter.close()

    assert connected is True
    assert adapter.user_id == "123456789"
    assert saved == ["123456789"]


async def test_connect_fails_when_user_id_cannot_be_discovered(monkeypatch):
    adapter = CloudSessionAdapter(app_token="t1")
    monkeypatch.setattr(adapter, "_get_user_info", lambda: _async_value({"valid": True}))
    monkeypatch.setattr(adapter, "_discover_user_id", lambda: _async_value(None))

    connected = await adapter.connect()

    assert connected is False
    assert adapter.user_id is None
    assert adapter.is_connected() is False


async def _async_value(value):
    return value
