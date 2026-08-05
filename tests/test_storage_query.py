from datetime import UTC, datetime

import pytest

from zepp_life_mcp.adapters.export_file import ExportFileAdapter
from zepp_life_mcp.models import BodyMeasurement, DailyActivity, HeartRateSample, SleepSession
from zepp_life_mcp.services.query_service import QueryService
from zepp_life_mcp.storage import Database, UpsertOutcome


def test_storage_and_query_roundtrip(tmp_path):
    db = Database(tmp_path / "test.db")

    activity = DailyActivity(
        id="a1",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id="u1",
        device_id=None,
        collected_at=None,
        date="2022-02-13",
        steps=1000,
        distance_m=900,
        active_kcal=60,
        total_kcal=None,
        floors=None,
        active_minutes=None,
    )
    hr = HeartRateSample(
        id="hr1",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id="u1",
        device_id=None,
        collected_at=None,
        timestamp=datetime(2022, 2, 13, 12, 0, 0),
        local_date=None,
        bpm=72,
        sample_type="passive",
    )
    body = BodyMeasurement(
        id="w1",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id="u1",
        device_id=None,
        collected_at=None,
        timestamp=datetime(2022, 2, 13, 7, 0, 0),
        local_date=None,
        weight_kg=90.5,
        bmi=26.0,
        body_fat_pct=None,
        muscle_mass_kg=None,
        water_pct=None,
        bone_mass_kg=None,
        visceral_fat_score=None,
        basal_metabolism_kcal=None,
        metabolic_age=None,
    )

    db.insert_daily_activity(activity)
    db.insert_heart_rate_sample(hr)
    db.insert_body_measurement(body)

    query = QueryService(db, "u1")
    assert len(query.get_daily_summaries("2022-02-13", "2022-02-13")) == 1
    assert len(query.get_heart_rate_samples("2022-02-13", "2022-02-13")) == 1
    assert len(query.get_body_measurements("2022-02-13", "2022-02-13")) == 1


def test_null_device_identity_is_idempotent_and_downward_correction_applies(tmp_path):
    db = Database(tmp_path / "test.db")
    original = DailyActivity(
        id="a1",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id="u1",
        device_id=None,
        collected_at=None,
        date="2024-01-01",
        steps=1000,
        distance_m=900,
        active_kcal=60,
        total_kcal=None,
        floors=None,
        active_minutes=None,
    )
    corrected = original.model_copy(
        update={"id": "a2", "steps": 800, "distance_m": 700, "active_kcal": 50}
    )

    assert db.upsert_daily_activity(original) == UpsertOutcome.INSERTED
    assert db.upsert_daily_activity(original) == UpsertOutcome.UNCHANGED
    assert db.upsert_daily_activity(corrected) == UpsertOutcome.UPDATED
    assert db.insert_daily_activity(corrected) is False

    records = db.query_daily_activity("u1", "2024-01-01", "2024-01-01")
    assert len(records) == 1
    assert records[0]["steps"] == 800
    assert records[0]["distance_m"] == 700
    assert records[0]["active_kcal"] == 50


def test_timestamp_queries_use_stored_local_date(tmp_path):
    db = Database(tmp_path / "test.db")
    sample = HeartRateSample(
        id="hr-local-date",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id="u1",
        device_id=None,
        collected_at=None,
        timezone="Europe/Berlin",
        timestamp=datetime(2024, 3, 30, 23, 30, tzinfo=UTC),
        local_date="2024-03-31",
        bpm=65,
        sample_type="passive",
    )

    db.insert_heart_rate_sample(sample)

    assert db.query_heart_rate_samples("u1", "2024-03-30", "2024-03-30") == []
    records = db.query_heart_rate_samples("u1", "2024-03-31", "2024-03-31")
    assert len(records) == 1
    assert records[0]["local_date"] == "2024-03-31"


def test_sleep_metrics_roundtrip(tmp_path):
    db = Database(tmp_path / "test.db")
    sleep = SleepSession(
        id="sleep-metrics",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id="u1",
        device_id=None,
        collected_at=None,
        sleep_id="sleep-1",
        local_date="2024-03-31",
        start_at=datetime(2024, 3, 30, 22, 0, tzinfo=UTC),
        end_at=datetime(2024, 3, 31, 5, 0, tzinfo=UTC),
        duration_minutes=420,
        time_asleep_minutes=390,
        time_awake_minutes=30,
        rem_minutes=60,
        wake_count=2,
        sleep_score=85,
    )

    db.insert_sleep_session(sleep)
    record = db.query_sleep_sessions("u1", "2024-03-31", "2024-03-31")[0]

    assert record["rem_minutes"] == 60
    assert record["wake_count"] == 2


def test_export_parser_ids_are_deterministic(tmp_path):
    adapter = ExportFileAdapter(tmp_path)
    activity = {"date": "2024-01-01", "steps": 123, "distance": 1.2, "calories": 45}
    sleep = {"start": "2024-01-01T22:00:00", "end": "2024-01-02T06:00:00"}
    workout = {
        "type": "running",
        "start": "2024-01-01T12:00:00",
        "end": "2024-01-01T12:30:00",
    }
    body = {"timestamp": "2024-01-01T08:00:00", "weight": 80}

    pairs = [
        (adapter._dict_to_activity(activity), adapter._dict_to_activity(activity)),
        (adapter._dict_to_sleep(sleep), adapter._dict_to_sleep(sleep)),
        (adapter._dict_to_workout(workout), adapter._dict_to_workout(workout)),
        (
            adapter._dict_to_body_measurement(body),
            adapter._dict_to_body_measurement(body),
        ),
    ]
    for first, second in pairs:
        assert first is not None
        assert second is not None
        assert first.id == second.id


def test_export_identity_and_record_ids_do_not_change_when_file_is_added(tmp_path):
    activity_path = tmp_path / "activity.csv"
    activity_path.write_text("date,steps,distance,calories\n2024-01-01,123,1.2,45\n")

    first_adapter = ExportFileAdapter(tmp_path)
    assert first_adapter.connect() is True
    first_activity = next(first_adapter.iter_daily_activity())

    (tmp_path / "notes.txt").write_text("new export metadata")

    second_adapter = ExportFileAdapter(tmp_path)
    assert second_adapter.connect() is True
    second_activity = next(second_adapter.iter_daily_activity())

    assert second_adapter.get_user_id() == first_adapter.get_user_id()
    assert second_activity.id == first_activity.id


def test_export_unix_timestamp_is_utc_aware_and_uses_utc_local_date(tmp_path):
    adapter = ExportFileAdapter(tmp_path)
    expected = datetime(2024, 1, 1, 0, 30, tzinfo=UTC)

    measurement = adapter._dict_to_body_measurement(
        {"timestamp": str(expected.timestamp()), "weight": 80}
    )

    assert measurement is not None
    assert measurement.timestamp == expected
    assert measurement.timestamp.tzinfo is UTC
    assert measurement.local_date == "2024-01-01"


def test_export_z_timestamp_is_utc_aware(tmp_path):
    parsed = ExportFileAdapter(tmp_path)._parse_datetime("2024-01-01T00:30:00Z")

    assert parsed == datetime(2024, 1, 1, 0, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("metric", "values"),
    [
        ("steps", [100, 300]),
        ("distance_m", [1000, 2000]),
        ("active_kcal", [10, 30]),
        ("weight_kg", [80, 82]),
        ("sleep_minutes", [300, 420]),
    ],
)
@pytest.mark.parametrize(
    ("aggregation", "expected_index"),
    [("sum", 0), ("avg", 1), ("min", 2), ("max", 3), ("latest", 4)],
)
def test_metric_series_supports_every_metric_and_aggregation(
    tmp_path, metric, values, aggregation, expected_index
):
    db = Database(tmp_path / "metrics.db")
    for index, day in enumerate(("2024-01-01", "2024-01-02")):
        db.insert_daily_activity(
            DailyActivity(
                id=f"activity-{index}",
                provider="zepp_life",
                source_type="cloud_session",
                source_record_id=None,
                user_id="u1",
                device_id=None,
                collected_at=None,
                date=day,
                steps=[100, 300][index],
                distance_m=[1000, 2000][index],
                active_kcal=[10, 30][index],
                total_kcal=None,
                floors=None,
                active_minutes=None,
            )
        )
        timestamp = datetime.fromisoformat(f"{day}T08:00:00").replace(tzinfo=UTC)
        db.insert_body_measurement(
            BodyMeasurement(
                id=f"body-{index}",
                provider="zepp_life",
                source_type="cloud_session",
                source_record_id=None,
                user_id="u1",
                device_id=None,
                collected_at=None,
                timestamp=timestamp,
                local_date=day,
                weight_kg=[80, 82][index],
                bmi=None,
                body_fat_pct=None,
                muscle_mass_kg=None,
                water_pct=None,
                bone_mass_kg=None,
                visceral_fat_score=None,
                basal_metabolism_kcal=None,
                metabolic_age=None,
            )
        )
        db.insert_sleep_session(
            SleepSession(
                id=f"sleep-{index}",
                provider="zepp_life",
                source_type="cloud_session",
                source_record_id=None,
                user_id="u1",
                device_id=None,
                collected_at=None,
                sleep_id=f"sleep-{index}",
                local_date=day,
                start_at=timestamp,
                end_at=timestamp,
                duration_minutes=[300, 420][index],
                time_asleep_minutes=[300, 420][index],
                time_awake_minutes=0,
                sleep_score=None,
            )
        )

    expected_values = [sum(values), sum(values) / 2, min(values), max(values), values[-1]]
    series = QueryService(db, "u1").get_metric_series(
        metric,
        "2024-01-01",
        "2024-01-02",
        granularity="week",
        aggregation=aggregation,
    )

    assert series == [{"date": "2024-01-01", "value": expected_values[expected_index]}]


@pytest.mark.parametrize(
    ("start_date", "end_date", "message"),
    [
        ("bad-date", "2024-01-01", "YYYY-MM-DD"),
        ("2024-01-02", "2024-01-01", "on or before"),
    ],
)
def test_query_service_rejects_invalid_date_ranges(tmp_path, start_date, end_date, message):
    query = QueryService(Database(tmp_path / "query.db"), "u1")

    with pytest.raises(ValueError, match=message):
        query.get_metric_series("steps", start_date, end_date)
