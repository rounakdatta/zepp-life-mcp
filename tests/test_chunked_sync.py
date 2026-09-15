"""Coverage guarantees for the band_data row cap, and the raw payload archive."""

from datetime import date, timedelta
from typing import Any, cast

from zepp_life_mcp.adapters import cloud_session
from zepp_life_mcp.adapters.cloud_session import CloudSessionAdapter
from zepp_life_mcp.services.sync_service import SyncService
from zepp_life_mcp.storage import Database


class FakeResponse:
    status_code = 200

    def __init__(self, payload: dict[str, Any]):
        self.payload = payload

    def json(self) -> dict[str, Any]:
        return self.payload


class CappedBandClient:
    """Stands in for band_data.json, including its truncation behaviour.

    The real endpoint answers HTTP 200 with at most `cap` rows and drops the
    MOST RECENT dates when it truncates, which is what makes the data loss
    invisible to a caller that only checks the status code.
    """

    def __init__(
        self,
        cap: int = 500,
        first_day: str = "2024-01-01",
        rows_per_day: int = 1,
    ):
        self.cap = cap
        self.first_day = date.fromisoformat(first_day)
        self.rows_per_day = rows_per_day
        self.windows: list[tuple[str, str]] = []

    async def get(self, url: str, params: dict[str, Any] | None = None, **kwargs: Any):
        params = params or {}
        start = max(date.fromisoformat(params["from_date"]), self.first_day)
        end = date.fromisoformat(params["to_date"])
        self.windows.append((params["from_date"], params["to_date"]))

        rows = []
        day = start
        while day <= end:
            for device in range(self.rows_per_day):
                rows.append(
                    {
                        "date_time": day.isoformat(),
                        "device_id": f"band-{device}",
                        "summary": {"stp": {"ttl": 100, "dis": 200, "cal": 30}},
                    }
                )
            day += timedelta(days=1)

        return FakeResponse({"data": rows[: self.cap]})


def _adapter(client: CappedBandClient) -> CloudSessionAdapter:
    adapter = CloudSessionAdapter(app_token="t", user_id="u1")
    cast(Any, adapter)._client = client
    adapter._connected = True
    return adapter


async def test_range_wider_than_the_row_cap_still_returns_the_most_recent_days():
    """Regression: one request for a wide range silently lost the newest dates.

    Upstream caps at 500 rows counted from the oldest end, so a single
    2020-01-01..today request returned data that stopped months before today
    while still reporting HTTP 200.
    """
    client = CappedBandClient(cap=500, first_day="2024-01-01")
    adapter = _adapter(client)

    start, end = "2024-01-01", "2026-09-16"
    activities = [a async for a in adapter.iter_daily_activity(start, end)]

    expected_days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    assert len(activities) == expected_days
    assert activities[0].date == start
    assert activities[-1].date == end

    # every window actually issued stayed inside the cap
    assert len(client.windows) > 1
    for window_start, window_end in client.windows:
        span = (date.fromisoformat(window_end) - date.fromisoformat(window_start)).days + 1
        assert span <= cloud_session.BAND_DATA_WINDOW_DAYS


async def test_windows_halve_when_a_response_comes_back_at_the_cap():
    """Rows are per (date, device), so the cap can be hit well inside a window.

    A two-device account emits two rows per date, which puts a 400-day window at
    800 rows and back over the cap. The window has to shrink itself.
    """
    client = CappedBandClient(
        cap=cloud_session.BAND_DATA_ROW_CAP, first_day="2024-01-01", rows_per_day=2
    )
    adapter = _adapter(client)

    start, end = "2024-01-01", "2024-12-31"
    activities = [a async for a in adapter.iter_daily_activity(start, end)]

    expected_days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    dates = [a.date for a in activities]
    assert dates == sorted(dates)
    assert len(set(dates)) == expected_days, "chunking must not drop or duplicate days"

    # it shrank below the starting window rather than accepting a capped response
    spans = [
        (date.fromisoformat(b) - date.fromisoformat(a)).days + 1 for a, b in client.windows
    ]
    assert min(spans) < cloud_session.BAND_DATA_WINDOW_DAYS


async def test_windows_are_contiguous_and_non_overlapping():
    client = CappedBandClient(cap=500, first_day="2024-01-01")
    adapter = _adapter(client)

    _ = [a async for a in adapter.iter_daily_activity("2024-01-01", "2026-09-16")]

    covered: list[tuple[date, date]] = []
    for window_start, window_end in client.windows:
        covered.append((date.fromisoformat(window_start), date.fromisoformat(window_end)))

    # windows that were retried at a smaller size share a start; keep the last try
    accepted: dict[date, date] = {}
    for window_start, window_end in covered:
        accepted[window_start] = window_end

    cursor = date.fromisoformat("2024-01-01")
    for window_start in sorted(accepted):
        if window_start < cursor:
            continue
        assert window_start == cursor, f"gap before {window_start}"
        cursor = accepted[window_start] + timedelta(days=1)
    assert cursor == date.fromisoformat("2026-09-17")


async def test_sync_archives_raw_payloads_and_replays_them(tmp_path):
    db = Database(tmp_path / "raw.db")
    client = CappedBandClient(cap=500, first_day="2026-01-01")
    adapter = _adapter(client)

    service = SyncService(adapter, db)
    result = await service.sync_data_type(
        "daily_activity", start_date="2026-01-01", end_date="2026-03-01"
    )
    assert result["added"] == 60

    stats = db.raw_payload_stats(user_id="u1")
    assert [row["endpoint"] for row in stats] == ["band_data.summary"]
    assert stats[0]["payloads"] == 1
    assert stats[0]["stored_bytes"] < stats[0]["uncompressed_bytes"], "archive is compressed"

    replayed = list(db.read_raw_payloads(user_id="u1", endpoint="band_data.summary"))
    assert len(replayed) == 1
    rows = replayed[0]["payload"]["data"]
    assert len(rows) == 60
    # the archive keeps fields the typed schema has no column for
    assert rows[0]["summary"]["stp"]["cal"] == 30
    assert replayed[0]["window_start"] == "2026-01-01"
    assert replayed[0]["window_end"] == "2026-03-01"


async def test_archiving_can_be_disabled(tmp_path):
    db = Database(tmp_path / "noraw.db")
    adapter = _adapter(CappedBandClient(cap=500, first_day="2026-01-01"))

    service = SyncService(adapter, db, archive_raw=False)
    await service.sync_data_type("daily_activity", start_date="2026-01-01", end_date="2026-01-31")

    assert db.raw_payload_stats() == []


async def test_identical_refetch_dedupes_but_changed_content_is_versioned(tmp_path):
    db = Database(tmp_path / "dedupe.db")
    adapter = _adapter(CappedBandClient(cap=500, first_day="2026-01-01"))
    service = SyncService(adapter, db)

    for _ in range(3):
        await service.sync_data_type(
            "daily_activity", start_date="2026-01-01", end_date="2026-01-31", force_full=True
        )
    assert db.raw_payload_stats(user_id="u1")[0]["payloads"] == 1

    # a later fetch that sees different upstream content appends a version
    cast(Any, adapter)._client = CappedBandClient(cap=500, first_day="2026-01-02")
    await service.sync_data_type(
        "daily_activity", start_date="2026-01-01", end_date="2026-01-31", force_full=True
    )
    assert db.raw_payload_stats(user_id="u1")[0]["payloads"] == 2
