"""Sync service for importing data from adapters to database."""

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import date, datetime, timedelta
from typing import Any

from zepp_life_mcp.adapters.base import DataAdapter
from zepp_life_mcp.storage import Database, UpsertOutcome

logger = logging.getLogger(__name__)


class SyncService:
    """Service for synchronizing data from adapters to local database."""

    def __init__(self, adapter: DataAdapter, db: Database):
        """Initialize sync service.

        Args:
            adapter: Data source adapter
            db: Database instance
        """
        self.adapter = adapter
        self.db = db

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
    ) -> dict[str, Any]:
        """Synchronize a specific data type.

        Args:
            data_type: Type of data to sync (daily_activity, sleep, workouts, body_measurements)
            start_date: Start date (YYYY-MM-DD), defaults to 30 days ago
            end_date: End date (YYYY-MM-DD), defaults to today
            force_full: Force full sync ignoring last sync state

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
            end_date = date.today().isoformat()
        if not start_date:
            if cursor_date:
                start_date = cursor_date
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
