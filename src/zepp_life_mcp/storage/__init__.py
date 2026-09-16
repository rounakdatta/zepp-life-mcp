"""SQLite storage layer for Zepp MCP."""

import gzip
import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Any

from zepp_life_mcp.models import (
    BodyMeasurement,
    DailyActivity,
    HeartRateSample,
    SleepSession,
    Workout,
)
from zepp_life_mcp.storage.migrations import run_migrations


class UpsertOutcome(StrEnum):
    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"


class Database:
    """SQLite database manager."""

    def __init__(self, db_path: Path | str):
        """Initialize database.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        run_migrations(self.db_path)
        # A deployment runs sync in one process and serving in another against
        # the same file. WAL lets the reader keep working through a write
        # instead of failing on SQLITE_BUSY; it is stored in the file header, so
        # setting it once here is enough.
        with self._get_connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")

    @contextmanager
    def _get_connection(self):
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        # Wait out a concurrent writer rather than raising immediately.
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _device_key(device_id: str | None) -> str:
        return device_id or ""

    @staticmethod
    def _upsert(
        conn: sqlite3.Connection,
        table: str,
        key_columns: tuple[str, ...],
        values: dict[str, Any],
    ) -> UpsertOutcome:
        key_values = tuple(values[column] for column in key_columns)
        predicate = " AND ".join(f"{column} = ?" for column in key_columns)
        existing = conn.execute(
            f"SELECT * FROM {table} WHERE {predicate}",
            key_values,
        ).fetchone()

        if existing is None:
            columns = tuple(values)
            placeholders = ", ".join("?" for _ in columns)
            conn.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                tuple(values[column] for column in columns),
            )
            return UpsertOutcome.INSERTED

        changed_values = {
            column: value
            for column, value in values.items()
            if column != "id" and existing[column] != value
        }
        if not changed_values:
            return UpsertOutcome.UNCHANGED

        assignments = ", ".join(f"{column} = ?" for column in changed_values)
        conn.execute(
            f"UPDATE {table} SET {assignments}, updated_at = CURRENT_TIMESTAMP "
            f"WHERE {predicate}",
            (*changed_values.values(), *key_values),
        )
        return UpsertOutcome.UPDATED

    def upsert_daily_activity(self, activity: DailyActivity) -> UpsertOutcome:
        with self._get_connection() as conn:
            outcome = self._upsert(
                conn,
                "daily_activity",
                ("user_id", "date", "device_key"),
                {
                    "id": activity.id,
                    "provider": activity.provider,
                    "source_type": activity.source_type,
                    "source_record_id": activity.source_record_id,
                    "user_id": activity.user_id,
                    "device_id": activity.device_id,
                    "device_key": self._device_key(activity.device_id),
                    "timezone": activity.timezone,
                    "collected_at": (
                        activity.collected_at.isoformat() if activity.collected_at else None
                    ),
                    "date": activity.date,
                    "steps": activity.steps,
                    "distance_m": activity.distance_m,
                    "active_kcal": activity.active_kcal,
                    "total_kcal": activity.total_kcal,
                    "floors": activity.floors,
                    "active_minutes": activity.active_minutes,
                    "tz_offset_seconds": activity.tz_offset_seconds,
                },
            )
            conn.commit()
            return outcome

    def insert_daily_activity(self, activity: DailyActivity) -> bool:
        return self.upsert_daily_activity(activity) == UpsertOutcome.INSERTED

    def upsert_sleep_session(self, sleep: SleepSession) -> UpsertOutcome:
        with self._get_connection() as conn:
            outcome = self._upsert(
                conn,
                "sleep_sessions",
                ("user_id", "sleep_id"),
                {
                    "id": sleep.id,
                    "provider": sleep.provider,
                    "source_type": sleep.source_type,
                    "source_record_id": sleep.source_record_id,
                    "user_id": sleep.user_id,
                    "device_id": sleep.device_id,
                    "timezone": sleep.timezone,
                    "collected_at": sleep.collected_at.isoformat() if sleep.collected_at else None,
                    "sleep_id": sleep.sleep_id,
                    "local_date": sleep.local_date or sleep.start_at.date().isoformat(),
                    "start_at": sleep.start_at.isoformat(),
                    "end_at": sleep.end_at.isoformat(),
                    "duration_minutes": sleep.duration_minutes,
                    "time_asleep_minutes": sleep.time_asleep_minutes,
                    "time_awake_minutes": sleep.time_awake_minutes,
                    "rem_minutes": sleep.rem_minutes,
                    "deep_minutes": sleep.deep_minutes,
                    "light_minutes": sleep.light_minutes,
                    "wake_count": sleep.wake_count,
                    "sleep_score": sleep.sleep_score,
                    "is_nap": sleep.is_nap,
                    "tz_offset_seconds": sleep.tz_offset_seconds,
                    "algo_version": sleep.algo_version,
                    "stages": json.dumps([stage.model_dump() for stage in sleep.stages]),
                },
            )
            conn.commit()
            return outcome

    def insert_sleep_session(self, sleep: SleepSession) -> bool:
        return self.upsert_sleep_session(sleep) == UpsertOutcome.INSERTED

    def upsert_workout(self, workout: Workout) -> UpsertOutcome:
        with self._get_connection() as conn:
            outcome = self._upsert(
                conn,
                "workouts",
                ("user_id", "workout_id"),
                {
                    "id": workout.id,
                    "provider": workout.provider,
                    "source_type": workout.source_type,
                    "source_record_id": workout.source_record_id,
                    "user_id": workout.user_id,
                    "device_id": workout.device_id,
                    "timezone": workout.timezone,
                    "collected_at": (
                        workout.collected_at.isoformat() if workout.collected_at else None
                    ),
                    "workout_id": workout.workout_id,
                    "local_date": workout.local_date or workout.start_at.date().isoformat(),
                    "activity_type": workout.activity_type,
                    "start_at": workout.start_at.isoformat(),
                    "end_at": workout.end_at.isoformat(),
                    "duration_minutes": workout.duration_minutes,
                    "distance_m": workout.distance_m,
                    "calories_kcal": workout.calories_kcal,
                    "avg_heart_rate_bpm": workout.avg_heart_rate_bpm,
                    "max_heart_rate_bpm": workout.max_heart_rate_bpm,
                    "avg_pace_sec_per_km": workout.avg_pace_sec_per_km,
                    "max_pace_sec_per_km": workout.max_pace_sec_per_km,
                    "total_steps": workout.total_steps,
                    "tz_offset_seconds": workout.tz_offset_seconds,
                },
            )
            conn.commit()
            return outcome

    def insert_workout(self, workout: Workout) -> bool:
        return self.upsert_workout(workout) == UpsertOutcome.INSERTED

    def upsert_body_measurement(self, measurement: BodyMeasurement) -> UpsertOutcome:
        with self._get_connection() as conn:
            outcome = self._upsert(
                conn,
                "body_measurements",
                ("user_id", "timestamp", "device_key"),
                {
                    "id": measurement.id,
                    "provider": measurement.provider,
                    "source_type": measurement.source_type,
                    "source_record_id": measurement.source_record_id,
                    "user_id": measurement.user_id,
                    "device_id": measurement.device_id,
                    "device_key": self._device_key(measurement.device_id),
                    "timezone": measurement.timezone,
                    "collected_at": (
                        measurement.collected_at.isoformat() if measurement.collected_at else None
                    ),
                    "timestamp": measurement.timestamp.isoformat(),
                    "local_date": measurement.local_date
                    or measurement.timestamp.date().isoformat(),
                    "weight_kg": measurement.weight_kg,
                    "bmi": measurement.bmi,
                    "body_fat_pct": measurement.body_fat_pct,
                    "muscle_mass_kg": measurement.muscle_mass_kg,
                    "water_pct": measurement.water_pct,
                    "bone_mass_kg": measurement.bone_mass_kg,
                    "visceral_fat_score": measurement.visceral_fat_score,
                    "basal_metabolism_kcal": measurement.basal_metabolism_kcal,
                    "metabolic_age": measurement.metabolic_age,
                },
            )
            conn.commit()
            return outcome

    def insert_body_measurement(self, measurement: BodyMeasurement) -> bool:
        return self.upsert_body_measurement(measurement) == UpsertOutcome.INSERTED

    def upsert_heart_rate_sample(self, sample: HeartRateSample) -> UpsertOutcome:
        with self._get_connection() as conn:
            outcome = self._upsert(
                conn,
                "heart_rate_samples",
                ("user_id", "timestamp", "sample_type"),
                {
                    "id": sample.id,
                    "provider": sample.provider,
                    "source_type": sample.source_type,
                    "source_record_id": sample.source_record_id,
                    "user_id": sample.user_id,
                    "device_id": sample.device_id,
                    "timezone": sample.timezone,
                    "collected_at": sample.collected_at.isoformat() if sample.collected_at else None,
                    "timestamp": sample.timestamp.isoformat(),
                    "local_date": sample.local_date or sample.timestamp.date().isoformat(),
                    "bpm": sample.bpm,
                    "sample_type": sample.sample_type,
                },
            )
            conn.commit()
            return outcome

    def insert_heart_rate_sample(self, sample: HeartRateSample) -> bool:
        return self.upsert_heart_rate_sample(sample) == UpsertOutcome.INSERTED

    def record_raw_payload(
        self,
        *,
        source_type: str,
        user_id: str,
        endpoint: str,
        payload: Any,
        request_params: dict[str, Any] | None = None,
        window_start: str | None = None,
        window_end: str | None = None,
        http_status: int | None = None,
        record_id: str | None = None,
        provider: str = "zepp_life",
    ) -> bool:
        """Archive one upstream response verbatim, gzipped.

        Returns True when a new row was written, False when an identical payload
        for the same endpoint and window is already archived. Re-fetching a
        window whose content has changed appends a new row rather than
        overwriting, so the archive is append-only and never loses a version.
        """
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(body).hexdigest()
        blob = gzip.compress(body)

        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO raw_payloads (
                    provider, source_type, user_id, endpoint, request_params,
                    window_start, window_end, http_status, record_id, content_encoding,
                    payload_sha256, payload_bytes, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'gzip', ?, ?, ?)
                """,
                (
                    provider,
                    source_type,
                    user_id,
                    endpoint,
                    json.dumps(request_params or {}, sort_keys=True),
                    window_start,
                    window_end,
                    http_status,
                    record_id,
                    digest,
                    len(body),
                    blob,
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

    def sole_user_id(self) -> str | None:
        """The one user_id present in this database, or None if 0 or many.

        A served instance must be able to answer from the archive without a
        working upstream connection, which is the only other way to learn whose
        data it holds.
        """
        tables = (
            "daily_activity",
            "sleep_sessions",
            "workouts",
            "body_measurements",
            "heart_rate_samples",
        )
        union = " UNION ".join(f"SELECT DISTINCT user_id FROM {table}" for table in tables)
        with self._get_connection() as conn:
            rows = conn.execute(f"SELECT DISTINCT user_id FROM ({union}) LIMIT 2").fetchall()
        return str(rows[0]["user_id"]) if len(rows) == 1 else None

    def archived_record_ids(self, endpoint: str, user_id: str) -> set[str]:
        """Record IDs already archived for a per-record endpoint.

        Lets an incremental backfill skip what it has, so the expensive first
        pass over a full workout history happens exactly once.
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT record_id FROM raw_payloads "
                "WHERE endpoint = ? AND user_id = ? AND record_id IS NOT NULL",
                (endpoint, user_id),
            ).fetchall()
        return {str(row["record_id"]) for row in rows}

    def read_raw_payloads(
        self,
        source_type: str | None = None,
        user_id: str | None = None,
        endpoint: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Replay archived payloads, newest fetch first, with JSON decoded.

        This is the entry point for re-deriving metrics the typed tables do not
        carry: iterate the raw responses and map them however you like.
        """
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("source_type", source_type),
            ("user_id", user_id),
            ("endpoint", endpoint),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if start_date is not None:
            clauses.append("(window_end IS NULL OR window_end >= ?)")
            params.append(start_date)
        if end_date is not None:
            clauses.append("(window_start IS NULL OR window_start <= ?)")
            params.append(end_date)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM raw_payloads {where} ORDER BY fetched_at DESC, id DESC",
                tuple(params),
            ).fetchall()

        for row in rows:
            record = dict(row)
            record["payload"] = json.loads(gzip.decompress(row["payload"]).decode("utf-8"))
            record["request_params"] = json.loads(row["request_params"])
            yield record

    def raw_payload_stats(self, user_id: str | None = None) -> list[dict[str, Any]]:
        """Summarize the raw archive by endpoint."""
        predicate = "WHERE user_id = ?" if user_id else ""
        params = (user_id,) if user_id else ()
        with self._get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT endpoint,
                       COUNT(*) AS payloads,
                       SUM(payload_bytes) AS uncompressed_bytes,
                       SUM(LENGTH(payload)) AS stored_bytes,
                       MIN(window_start) AS first_window,
                       MAX(window_end) AS last_window,
                       MAX(fetched_at) AS last_fetched_at
                FROM raw_payloads {predicate}
                GROUP BY endpoint
                ORDER BY endpoint
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def update_sync_state(
        self,
        source_type: str,
        user_id: str,
        data_type: str,
        *,
        cursor_date: str | None = None,
        records_count: int = 0,
        success: bool,
        error: str | None = None,
    ) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (
                    source_type,
                    user_id,
                    data_type,
                    last_attempt_at,
                    last_success_at,
                    cursor_date,
                    records_count,
                    last_error
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    CURRENT_TIMESTAMP,
                    CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,
                    ?,
                    ?,
                    ?
                )
                ON CONFLICT(source_type, user_id, data_type) DO UPDATE SET
                    last_attempt_at = CURRENT_TIMESTAMP,
                    last_success_at = CASE
                        WHEN ? THEN CURRENT_TIMESTAMP
                        ELSE sync_state.last_success_at
                    END,
                    cursor_date = CASE
                        WHEN ? THEN excluded.cursor_date
                        ELSE sync_state.cursor_date
                    END,
                    records_count = excluded.records_count,
                    last_error = excluded.last_error
                """,
                (
                    source_type,
                    user_id,
                    data_type,
                    success,
                    cursor_date,
                    records_count,
                    error,
                    success,
                    success,
                ),
            )
            conn.commit()

    def get_sync_state(
        self,
        source_type: str,
        user_id: str,
        data_type: str,
    ) -> dict[str, Any] | None:
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM sync_state
                WHERE source_type = ? AND user_id = ? AND data_type = ?
                """,
                (source_type, user_id, data_type),
            ).fetchone()
            return dict(row) if row else None

    def query_daily_activity(
        self,
        user_id: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        """Query daily activity records."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM daily_activity
                WHERE user_id = ? AND date >= ? AND date <= ?
                ORDER BY date
                """,
                (user_id, start_date, end_date),
            ).fetchall()
            return [dict(row) for row in rows]

    def query_sleep_sessions(
        self,
        user_id: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        """Query sleep session records."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM sleep_sessions
                WHERE user_id = ?
                AND local_date >= ? AND local_date <= ?
                ORDER BY start_at
                """,
                (user_id, start_date, end_date),
            ).fetchall()
            return [dict(row) for row in rows]

    def query_workouts(
        self,
        user_id: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        """Query workout records."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workouts
                WHERE user_id = ?
                AND local_date >= ? AND local_date <= ?
                ORDER BY start_at
                """,
                (user_id, start_date, end_date),
            ).fetchall()
            return [dict(row) for row in rows]

    def query_body_measurements(
        self,
        user_id: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        """Query body measurement records."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM body_measurements
                WHERE user_id = ?
                AND local_date >= ? AND local_date <= ?
                ORDER BY timestamp
                """,
                (user_id, start_date, end_date),
            ).fetchall()
            return [dict(row) for row in rows]

    def query_heart_rate_samples(
        self,
        user_id: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM heart_rate_samples
                WHERE user_id = ?
                AND local_date >= ? AND local_date <= ?
                ORDER BY timestamp
                """,
                (user_id, start_date, end_date),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_data_coverage(self, user_id: str) -> list[dict[str, Any]]:
        """Get data coverage statistics."""
        with self._get_connection() as conn:
            results = []

            # Activity coverage
            row = conn.execute(
                """
                SELECT
                    MIN(date) as first_date,
                    MAX(date) as last_date,
                    COUNT(DISTINCT date) as days_with_data
                FROM daily_activity
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row and row["first_date"]:
                results.append(
                    {
                        "data_type": "daily_activity",
                        **dict(row),
                    }
                )

            # Sleep coverage
            row = conn.execute(
                """
                SELECT
                    MIN(local_date) as first_date,
                    MAX(local_date) as last_date,
                    COUNT(DISTINCT local_date) as days_with_data
                FROM sleep_sessions
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row and row["first_date"]:
                results.append(
                    {
                        "data_type": "sleep",
                        **dict(row),
                    }
                )

            # Workouts coverage
            row = conn.execute(
                """
                SELECT
                    MIN(local_date) as first_date,
                    MAX(local_date) as last_date,
                    COUNT(DISTINCT local_date) as days_with_data
                FROM workouts
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row and row["first_date"]:
                results.append(
                    {
                        "data_type": "workouts",
                        **dict(row),
                    }
                )

            # Body measurements coverage
            row = conn.execute(
                """
                SELECT
                    MIN(local_date) as first_date,
                    MAX(local_date) as last_date,
                    COUNT(DISTINCT local_date) as days_with_data
                FROM body_measurements
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row and row["first_date"]:
                results.append(
                    {
                        "data_type": "body_measurements",
                        **dict(row),
                    }
                )

            row = conn.execute(
                """
                SELECT
                    MIN(local_date) as first_date,
                    MAX(local_date) as last_date,
                    COUNT(DISTINCT local_date) as days_with_data
                FROM heart_rate_samples
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row and row["first_date"]:
                results.append(
                    {
                        "data_type": "heart_rate",
                        **dict(row),
                    }
                )

            return results
