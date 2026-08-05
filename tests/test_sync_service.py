import pytest

from zepp_life_mcp import server
from zepp_life_mcp.adapters.base import DataAdapter
from zepp_life_mcp.models import DailyActivity
from zepp_life_mcp.services.sync_service import SyncService
from zepp_life_mcp.storage import Database


class ActivityAdapter(DataAdapter):
    source_type = "cloud_session"

    def __init__(self, records, user_id="user-1", failure=None):
        self.records = records
        self.user_id = user_id
        self.failure = failure
        self.requested_ranges = []

    async def connect(self):
        return True

    async def disconnect(self):
        return None

    def is_connected(self):
        return True

    def get_user_id(self):
        return self.user_id

    def get_available_data_types(self):
        return ["daily_activity"]

    async def iter_daily_activity(self, start_date=None, end_date=None):
        self.requested_ranges.append((start_date, end_date))
        for record in self.records:
            yield record
        if self.failure:
            raise self.failure

    async def iter_sleep_sessions(self, start_date=None, end_date=None):
        if False:
            yield

    async def iter_workouts(self, start_date=None, end_date=None):
        if False:
            yield

    async def iter_body_measurements(self, start_date=None, end_date=None):
        if False:
            yield

    async def iter_heart_rate(self, start_date=None, end_date=None):
        if False:
            yield

async def test_repeated_sync_has_no_row_growth_and_correct_stats(tmp_path):
    activity = DailyActivity(
        id="daily-1",
        provider="zepp_life",
        source_type="export_file",
        source_record_id=None,
        user_id="user-1",
        device_id=None,
        collected_at=None,
        date="2024-01-01",
        steps=100,
        distance_m=80,
        active_kcal=10,
        total_kcal=None,
        floors=None,
        active_minutes=None,
    )
    database = Database(tmp_path / "test.db")
    service = SyncService(ActivityAdapter([activity]), database)

    first = await service.sync_data_type(
        "daily_activity", "2024-01-01", "2024-01-01", force_full=True
    )
    second = await service.sync_data_type(
        "daily_activity", "2024-01-01", "2024-01-01", force_full=True
    )

    assert first["added"] == 1
    assert first["updated"] == 0
    assert first["skipped"] == 0
    assert second["added"] == 0
    assert second["updated"] == 0
    assert second["skipped"] == 1
    assert len(database.query_daily_activity("user-1", "2024-01-01", "2024-01-01")) == 1


async def test_daily_activity_uses_scoped_logical_cursor_for_each_user(tmp_path):
    database = Database(tmp_path / "test.db")
    first_adapter = ActivityAdapter([], user_id="user-1")
    second_adapter = ActivityAdapter([], user_id="user-2")

    await SyncService(first_adapter, database).sync_data_type(
        "daily_activity", "2024-01-01", "2024-01-31"
    )
    await SyncService(second_adapter, database).sync_data_type(
        "daily_activity", "2024-02-01", "2024-02-29"
    )
    await SyncService(first_adapter, database).sync_data_type("daily_activity", end_date="2024-02-02")
    await SyncService(second_adapter, database).sync_data_type("daily_activity", end_date="2024-03-02")

    assert first_adapter.requested_ranges[-1] == ("2024-01-31", "2024-02-02")
    assert second_adapter.requested_ranges[-1] == ("2024-02-29", "2024-03-02")
    first_state = database.get_sync_state("cloud_session", "user-1", "daily_activity")
    second_state = database.get_sync_state("cloud_session", "user-2", "daily_activity")
    assert first_state is not None
    assert second_state is not None
    assert first_state["cursor_date"] == "2024-02-02"
    assert second_state["cursor_date"] == "2024-03-02"


async def test_empty_success_updates_attempt_and_advances_cursor(tmp_path):
    database = Database(tmp_path / "test.db")
    service = SyncService(ActivityAdapter([]), database)

    result = await service.sync_data_type("daily_activity", "2024-03-01", "2024-03-03")
    state = database.get_sync_state("cloud_session", "user-1", "daily_activity")

    assert state is not None
    assert result["added"] == result["updated"] == result["skipped"] == 0
    assert state["last_attempt_at"] is not None
    assert state["last_success_at"] is not None
    assert state["cursor_date"] == "2024-03-03"
    assert state["records_count"] == 0
    assert state["last_error"] is None


async def test_failed_pass_updates_attempt_without_advancing_cursor(tmp_path):
    database = Database(tmp_path / "test.db")
    successful = SyncService(ActivityAdapter([]), database)
    await successful.sync_data_type("daily_activity", "2024-03-01", "2024-03-03")
    previous = database.get_sync_state("cloud_session", "user-1", "daily_activity")
    assert previous is not None
    failing = SyncService(ActivityAdapter([], failure=RuntimeError("backend unavailable")), database)

    with pytest.raises(RuntimeError, match="backend unavailable"):
        await failing.sync_data_type("daily_activity", end_date="2024-03-05")

    state = database.get_sync_state("cloud_session", "user-1", "daily_activity")
    assert state is not None
    assert state["cursor_date"] == "2024-03-03"
    assert state["last_success_at"] == previous["last_success_at"]
    assert state["last_attempt_at"] is not None
    assert state["last_error"] == "backend unavailable"


async def test_midstream_adapter_fetch_error_preserves_cursor_and_reports_failure(
    tmp_path, monkeypatch
):
    database = Database(tmp_path / "test.db")
    await SyncService(ActivityAdapter([]), database).sync_data_type(
        "daily_activity", "2024-03-01", "2024-03-03"
    )
    activity = DailyActivity(
        id="daily-2",
        provider="zepp_life",
        source_type="cloud_session",
        source_record_id=None,
        user_id="user-1",
        device_id=None,
        collected_at=None,
        date="2024-03-04",
        steps=200,
        distance_m=160,
        active_kcal=20,
        total_kcal=None,
        floors=None,
        active_minutes=None,
    )
    service = SyncService(
        ActivityAdapter(
            [activity],
            failure=RuntimeError("daily activity fetch failed"),
        ),
        database,
    )
    monkeypatch.setattr(server.context, "sync_service", service)

    result = await server._handle_sync_data(
        {"data_types": ["daily_activity"], "end_date": "2024-03-05"}
    )
    state = database.get_sync_state("cloud_session", "user-1", "daily_activity")

    assert result["status"] == "error"
    assert result["data_types_synced"] == []
    assert result["failed_data_types"] == ["daily_activity"]
    assert state is not None
    assert state["cursor_date"] == "2024-03-03"
    assert state["last_attempt_at"] is not None
    assert state["last_error"] == "daily activity fetch failed"


@pytest.mark.parametrize(
    ("start_date", "end_date", "message"),
    [
        ("not-a-date", "2024-01-01", "YYYY-MM-DD"),
        ("2024-01-02", "2024-01-01", "on or before"),
    ],
)
async def test_sync_rejects_invalid_date_ranges(tmp_path, start_date, end_date, message):
    service = SyncService(ActivityAdapter([]), Database(tmp_path / "test.db"))

    with pytest.raises(ValueError, match=message):
        await service.sync_data_type("daily_activity", start_date, end_date)


class MultiTypeSyncService:
    def __init__(self, failures):
        self.adapter = ActivityAdapter([])
        self.failures = failures

    async def sync_data_type(self, data_type, **kwargs):
        if data_type in self.failures:
            raise RuntimeError(f"private backend detail for {data_type}")
        return {"added": 1, "updated": 0, "skipped": 0}


async def test_sync_handler_surfaces_partial_failures_without_backend_details(monkeypatch):
    monkeypatch.setattr(server.context, "sync_service", MultiTypeSyncService({"sleep"}))

    result = await server._handle_sync_data({"data_types": ["daily_activity", "sleep"]})

    assert result["status"] == "ok"
    assert result["data_types_synced"] == ["daily_activity"]
    assert result["failed_data_types"] == ["sleep"]
    assert "private backend detail" not in str(result)


async def test_sync_handler_reports_total_failure(monkeypatch):
    monkeypatch.setattr(server.context, "sync_service", MultiTypeSyncService({"sleep"}))

    result = await server._handle_sync_data({"data_types": ["sleep"]})

    assert result["status"] == "error"
    assert result["data_types_synced"] == []
    assert result["failed_data_types"] == ["sleep"]
    assert result["error"] == "No data types synced successfully"
