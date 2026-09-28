"""Sync service for importing data from adapters to database."""

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from zepp_life_mcp.adapters.base import DataAdapter
from zepp_life_mcp.storage import Database, UpsertOutcome

logger = logging.getLogger(__name__)

# How far before the stored cursor a resumed sync starts reading. The band
# uploads on the phone's schedule, so the last hours of a day can reach Zepp
# after a run has already moved the cursor past that day; resuming exactly at
# the cursor skips them for good. Re-reading is cheap: upserts are idempotent
# and an identical payload is never archived twice.
CURSOR_LOOKBACK_DAYS = 2

# The cursor records when a sync last ran, not how far the band's data had got.
# While the phone is offline, passes keep running and the cursor keeps moving,
# so once it reconnects and uploads the missed days in one go, they sit behind
# any fixed lookback. A pass therefore also resumes from the latest day the
# archive has band data for, which stays put while nothing arrives. Capped, so a
# band left in a drawer for months doesn't make every pass read months.
MAX_CATCHUP_DAYS = 30


class SyncService:
    """Service for synchronizing data from adapters to local database."""

    def __init__(self, adapter: DataAdapter, db: Database, archive_raw: bool = True):
        """Initialize sync service.

        Args:
            adapter: Data source adapter
            db: Database instance
            archive_raw: Persist every upstream response verbatim alongside the
                mapped records, so fields the typed schema drops stay recoverable
        """
        self.adapter = adapter
        self.db = db
        self.archive_raw = archive_raw

        set_raw_sink = getattr(adapter, "set_raw_sink", None)
        if callable(set_raw_sink):
            set_raw_sink(db.record_raw_payload if archive_raw else None)

    def _today(self) -> date:
        """Today in the account's timezone rather than the host's.

        A container clock runs in UTC, where it is still yesterday for the first
        hours of an IST day. Zepp files data under the band's local date, so a
        run in that gap could not see the morning that had just been uploaded.
        """
        zone = getattr(self.adapter, "timezone", None)
        if isinstance(zone, str) and zone:
            return datetime.now(ZoneInfo(zone)).date()
        return date.today()

    async def _iterate_records(self, records: Any) -> AsyncIterator[Any]:
        if hasattr(records, "__aiter__"):
            async for record in records:
                yield record
            return

        for record in records:
            yield record

    async def sync_data_type(
        self,
        data_type: str,
        start_date: str | None = None,
        end_date: str | None = None,
        force_full: bool = False,
        lookback_days: int | None = None,
    ) -> dict[str, Any]:
        """Synchronize a specific data type.

        Args:
            data_type: Type of data to sync (daily_activity, sleep, workouts, body_measurements)
            start_date: Start date (YYYY-MM-DD), defaults to the earlier of
                CURSOR_LOOKBACK_DAYS before the stored cursor and the day before
                the latest one with band data; without a cursor, all history for
                the cloud and 30 days for exports
            end_date: End date (YYYY-MM-DD), defaults to today in the adapter's
                timezone
            force_full: Force full sync ignoring last sync state
            lookback_days: Days before the cursor a resumed pass starts, when
                wider than the default is wanted (a daily deep pass)

        Returns:
            Dict with sync statistics
        """
        if not self.adapter.is_connected():
            raise RuntimeError("Adapter not connected")

        source_type = getattr(self.adapter, "source_type", None)
        if not source_type:
            adapter_name = self.adapter.__class__.__name__.removesuffix("Adapter")
            source_type = {
                "CloudSession": "cloud_session",
                "ExportFile": "export_file",
            }.get(adapter_name, adapter_name.lower())
        user_id = self.adapter.get_user_id() or "unknown"

        cursor_date = None
        if not force_full:
            state = self.db.get_sync_state(source_type, user_id, data_type)
            if state:
                cursor_date = state.get("cursor_date")

        if not end_date:
            end_date = self._today().isoformat()
        if not start_date:
            if cursor_date:
                days = CURSOR_LOOKBACK_DAYS if lookback_days is None else lookback_days
                if days < 0:
                    raise ValueError("lookback_days must not be negative")
                end = date.fromisoformat(end_date)
                resume = date.fromisoformat(cursor_date) - timedelta(days=days)
                arrived = self.db.latest_daily_activity_date(user_id)
                if arrived:
                    # The day data last arrived for is usually partial; re-read
                    # from the day before it.
                    catch_up = date.fromisoformat(arrived) - timedelta(days=1)
                    resume = min(resume, max(catch_up, end - timedelta(days=MAX_CATCHUP_DAYS)))
                # A cursor can sit past end_date (an earlier run given a later
                # --end-date); clamping keeps that from failing every run after.
                start_date = min(resume, end).isoformat()
            elif self.adapter.__class__.__name__ == "CloudSessionAdapter":
                start_date = "2020-01-01"
            else:
                start = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)
                start_date = start.strftime("%Y-%m-%d")

        assert start_date is not None
        assert end_date is not None
        try:
            parsed_start = date.fromisoformat(start_date)
            parsed_end = date.fromisoformat(end_date)
        except ValueError as exc:
            raise ValueError("start_date and end_date must use YYYY-MM-DD format") from exc
        if parsed_start > parsed_end:
            raise ValueError("start_date must be on or before end_date")

        added = 0
        updated = 0
        skipped = 0

        def count_outcome(outcome: UpsertOutcome) -> None:
            nonlocal added, updated, skipped
            if outcome == UpsertOutcome.INSERTED:
                added += 1
            elif outcome == UpsertOutcome.UPDATED:
                updated += 1
            else:
                skipped += 1

        try:
            if data_type == "daily_activity":
                records = self.adapter.iter_daily_activity(start_date, end_date)
                async for activity in self._iterate_records(records):
                    count_outcome(self.db.upsert_daily_activity(activity))
            elif data_type == "sleep":
                records = self.adapter.iter_sleep_sessions(start_date, end_date)
                async for sleep in self._iterate_records(records):
                    count_outcome(self.db.upsert_sleep_session(sleep))
            elif data_type == "workouts":
                records = self.adapter.iter_workouts(start_date, end_date)
                async for workout in self._iterate_records(records):
                    count_outcome(self.db.upsert_workout(workout))
            elif data_type == "body_measurements":
                records = self.adapter.iter_body_measurements(start_date, end_date)
                async for measurement in self._iterate_records(records):
                    count_outcome(self.db.upsert_body_measurement(measurement))
            elif data_type == "workout_details":
                iter_details = getattr(self.adapter, "iter_workout_details", None)
                if iter_details is None:
                    # Export mode has no track source; a default sync that lists
                    # every type must not fail because one source cannot serve one.
                    logger.info(
                        "%s exposes no workout detail source; skipping",
                        type(self.adapter).__name__,
                    )
                else:
                    known = self.db.archived_record_ids("sport.run.detail", user_id)
                    records = iter_details(start_date, end_date, known)
                    async for _trackid, newly_archived in self._iterate_records(records):
                        if newly_archived:
                            added += 1
                        else:
                            skipped += 1
            elif data_type == "heart_rate":
                records = self.adapter.iter_heart_rate(start_date, end_date)
                async for sample in self._iterate_records(records):
                    count_outcome(self.db.upsert_heart_rate_sample(sample))
            else:
                raise ValueError(f"Unknown data type: {data_type}")
        except Exception as exc:
            self.db.update_sync_state(
                source_type,
                user_id,
                data_type,
                records_count=added + updated + skipped,
                success=False,
                error=str(exc),
            )
            raise

        self.db.update_sync_state(
            source_type,
            user_id,
            data_type,
            cursor_date=end_date,
            records_count=added + updated + skipped,
            success=True,
        )

        logger.info(
            f"Synced {data_type}: {added} added, {updated} updated, "
            f"range {start_date} to {end_date}"
        )

        return {
            "data_type": data_type,
            "added": added,
            "updated": updated,
            "skipped": skipped,
            "start_date": start_date,
            "end_date": end_date,
        }

    def sync_data_type_sync(
        self,
        data_type: str,
        start_date: str | None = None,
        end_date: str | None = None,
        force_full: bool = False,
    ) -> dict[str, Any]:
        """Synchronous wrapper for sync_data_type.

        Use this when calling from synchronous code.
        """
        return asyncio.run(self.sync_data_type(data_type, start_date, end_date, force_full))
