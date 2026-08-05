"""SQLite storage layer for Zepp MCP."""

import json
import sqlite3
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

    @contextmanager
    def _get_connection(self):
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
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
                    "wake_count": sleep.wake_count,
                    "sleep_score": sleep.sleep_score,
                    "is_nap": sleep.is_nap,
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
