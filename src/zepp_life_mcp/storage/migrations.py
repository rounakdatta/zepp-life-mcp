import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

MigrationCallback = Callable[[sqlite3.Connection], None]


@dataclass(frozen=True)
class Migration:
    version: int
    apply: MigrationCallback
    destructive: bool = False


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS daily_activity (
        id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_record_id TEXT,
        user_id TEXT NOT NULL,
        device_id TEXT,
        timezone TEXT DEFAULT 'UTC',
        collected_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        date TEXT NOT NULL,
        steps INTEGER NOT NULL DEFAULT 0,
        distance_m REAL NOT NULL DEFAULT 0,
        active_kcal REAL NOT NULL DEFAULT 0,
        total_kcal REAL,
        floors INTEGER,
        active_minutes INTEGER,
        UNIQUE(user_id, date, device_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sleep_sessions (
        id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_record_id TEXT,
        user_id TEXT NOT NULL,
        device_id TEXT,
        timezone TEXT DEFAULT 'UTC',
        collected_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sleep_id TEXT NOT NULL,
        start_at TIMESTAMP NOT NULL,
        end_at TIMESTAMP NOT NULL,
        duration_minutes INTEGER NOT NULL,
        time_asleep_minutes INTEGER NOT NULL,
        time_awake_minutes INTEGER NOT NULL,
        sleep_score INTEGER,
        is_nap BOOLEAN DEFAULT FALSE,
        stages TEXT,
        UNIQUE(user_id, sleep_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workouts (
        id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_record_id TEXT,
        user_id TEXT NOT NULL,
        device_id TEXT,
        timezone TEXT DEFAULT 'UTC',
        collected_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        workout_id TEXT NOT NULL,
        activity_type TEXT NOT NULL,
        start_at TIMESTAMP NOT NULL,
        end_at TIMESTAMP NOT NULL,
        duration_minutes INTEGER NOT NULL,
        distance_m REAL,
        calories_kcal REAL,
        avg_heart_rate_bpm INTEGER,
        max_heart_rate_bpm INTEGER,
        avg_pace_sec_per_km REAL,
        max_pace_sec_per_km REAL,
        total_steps INTEGER,
        UNIQUE(user_id, workout_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS body_measurements (
        id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_record_id TEXT,
        user_id TEXT NOT NULL,
        device_id TEXT,
        timezone TEXT DEFAULT 'UTC',
        collected_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        timestamp TIMESTAMP NOT NULL,
        weight_kg REAL NOT NULL,
        bmi REAL,
        body_fat_pct REAL,
        muscle_mass_kg REAL,
        water_pct REAL,
        bone_mass_kg REAL,
        visceral_fat_score INTEGER,
        basal_metabolism_kcal INTEGER,
        metabolic_age INTEGER,
        UNIQUE(user_id, timestamp, device_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS heart_rate_samples (
        id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_record_id TEXT,
        user_id TEXT NOT NULL,
        device_id TEXT,
        timezone TEXT DEFAULT 'UTC',
        collected_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        timestamp TIMESTAMP NOT NULL,
        bpm INTEGER NOT NULL,
        sample_type TEXT NOT NULL,
        UNIQUE(user_id, timestamp, sample_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_type TEXT NOT NULL UNIQUE,
        last_sync_at TIMESTAMP,
        last_record_timestamp TIMESTAMP,
        records_count INTEGER DEFAULT 0
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_activity_user_date
    ON daily_activity(user_id, date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sleep_user_start
    ON sleep_sessions(user_id, start_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_workouts_user_start
    ON workouts(user_id, start_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_measurements_user_ts
    ON body_measurements(user_id, timestamp)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_heart_rate_user_ts
    ON heart_rate_samples(user_id, timestamp)
    """,
)


def _create_baseline_schema(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _add_device_keys(conn: sqlite3.Connection) -> None:
    for table in ("daily_activity", "body_measurements"):
        if "device_key" not in _column_names(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN device_key TEXT NOT NULL DEFAULT ''")
        conn.execute(f"UPDATE {table} SET device_key = COALESCE(device_id, '')")

    conn.execute("""
        DELETE FROM daily_activity
        WHERE rowid NOT IN (
            SELECT MAX(rowid)
            FROM daily_activity
            GROUP BY user_id, date, device_key
        )
    """)
    conn.execute("""
        DELETE FROM body_measurements
        WHERE rowid NOT IN (
            SELECT MAX(rowid)
            FROM body_measurements
            GROUP BY user_id, timestamp, device_key
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_activity_business_key
        ON daily_activity(user_id, date, device_key)
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_measurements_business_key
        ON body_measurements(user_id, timestamp, device_key)
    """)


def _scope_sync_state(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE sync_state_v3 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            user_id TEXT NOT NULL,
            data_type TEXT NOT NULL,
            last_attempt_at TIMESTAMP,
            last_success_at TIMESTAMP,
            cursor_date TEXT,
            records_count INTEGER DEFAULT 0,
            last_error TEXT,
            UNIQUE(source_type, user_id, data_type)
        )
    """)
    conn.execute("""
        INSERT INTO sync_state_v3 (
            source_type,
            user_id,
            data_type,
            last_attempt_at,
            last_success_at,
            cursor_date,
            records_count
        )
        SELECT
            'legacy',
            'legacy',
            data_type,
            last_sync_at,
            last_sync_at,
            date(last_record_timestamp),
            records_count
        FROM sync_state
    """)
    conn.execute("DROP TABLE sync_state")
    conn.execute("ALTER TABLE sync_state_v3 RENAME TO sync_state")


def _add_local_dates(conn: sqlite3.Connection) -> None:
    timestamp_columns = {
        "sleep_sessions": "start_at",
        "workouts": "start_at",
        "body_measurements": "timestamp",
        "heart_rate_samples": "timestamp",
    }
    for table, timestamp_column in timestamp_columns.items():
        if "local_date" not in _column_names(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN local_date TEXT")
        conn.execute(
            f"UPDATE {table} SET local_date = substr({timestamp_column}, 1, 10) "
            "WHERE local_date IS NULL"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_user_local_date "
            f"ON {table}(user_id, local_date)"
        )


def _add_sleep_metrics(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "sleep_sessions")
    if "rem_minutes" not in columns:
        conn.execute("ALTER TABLE sleep_sessions ADD COLUMN rem_minutes INTEGER NOT NULL DEFAULT 0")
    if "wake_count" not in columns:
        conn.execute("ALTER TABLE sleep_sessions ADD COLUMN wake_count INTEGER NOT NULL DEFAULT 0")


def _scope_cloud_record_ids(conn: sqlite3.Connection) -> None:
    prefixes = {
        "daily_activity": "cloud_",
        "sleep_sessions": "cloud_sleep_",
        "workouts": "cloud_",
        "body_measurements": "cloud_weight_",
    }
    for table, prefix in prefixes.items():
        conn.execute(
            f"UPDATE {table} SET id = ? || user_id || '_' || substr(id, ?) "
            "WHERE source_type = 'cloud_session' AND id LIKE ?",
            (prefix, len(prefix) + 1, f"{prefix}%"),
        )

    for prefix in ("cloud_rhr_", "cloud_hr_"):
        conn.execute(
            "UPDATE heart_rate_samples SET id = ? || user_id || '_' || substr(id, ?) "
            "WHERE source_type = 'cloud_session' AND id LIKE ?",
            (prefix, len(prefix) + 1, f"{prefix}%"),
        )

def _add_raw_payloads(conn: sqlite3.Connection) -> None:
    """Archive every upstream response verbatim so mapping stays replayable.

    The mapped tables are lossy by construction: Zepp returns ~193 fields per
    workout and the typed schema keeps a handful. Keeping the compressed source
    payload means a future mapping change can be backfilled from local data
    instead of re-fetching from an account that may no longer serve it.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_payloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL DEFAULT 'zepp_life',
            source_type TEXT NOT NULL,
            user_id TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            request_params TEXT NOT NULL,
            window_start TEXT,
            window_end TEXT,
            fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            http_status INTEGER,
            content_encoding TEXT NOT NULL DEFAULT 'gzip',
            payload_sha256 TEXT NOT NULL,
            payload_bytes INTEGER NOT NULL,
            payload BLOB NOT NULL,
            UNIQUE(source_type, user_id, endpoint, window_start, window_end, payload_sha256)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_payloads_lookup
        ON raw_payloads(source_type, user_id, endpoint, window_start)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_payloads_fetched
        ON raw_payloads(fetched_at)
    """)

def _add_raw_payload_record_ids(conn: sqlite3.Connection) -> None:
    """Identify payloads that are fetched one per record, not one per window.

    Workout tracks come from run/detail.json keyed by trackid, so "have I
    already archived this one?" has to be answerable without decompressing
    every stored blob. Window columns cannot express that: two workouts on the
    same day share a date.
    """
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(raw_payloads)")}
    if "record_id" not in columns:
        conn.execute("ALTER TABLE raw_payloads ADD COLUMN record_id TEXT")
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_payloads_record
        ON raw_payloads(endpoint, user_id, record_id)
    """)

def _add_device_context(conn: sqlite3.Connection) -> None:
    """Record where and with which algorithm each record was captured.

    Two questions could not be answered from the typed tables at all. *Where was
    the user* -- the band reports a timezone offset per day, which is the only
    location signal available, and without it a trip is invisible. And *is this
    row comparable to that one* -- the sleep algorithm changed version mid-2026
    and silently redistributed time between stages, so a longitudinal query that
    spans the change invents an effect. Both were sitting in the raw payloads
    and nowhere else.
    """
    for table in ("sleep_sessions", "daily_activity", "workouts"):
        columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        if "tz_offset_seconds" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN tz_offset_seconds INTEGER")
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(sleep_sessions)")}
    if "algo_version" not in columns:
        conn.execute("ALTER TABLE sleep_sessions ADD COLUMN algo_version TEXT")

    # rem_minutes had a column and deep did not, so deep sleep -- the stage people
    # actually ask about -- could only be got by parsing the stages JSON of every
    # row. Both are already computed during parsing; they just were not kept.
    for column in ("deep_minutes", "light_minutes"):
        if column not in columns:
            conn.execute(
                f"ALTER TABLE sleep_sessions ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
            )


MIGRATIONS = (
    Migration(version=1, apply=_create_baseline_schema),
    Migration(version=2, apply=_add_device_keys, destructive=True),
    Migration(version=3, apply=_scope_sync_state, destructive=True),
    Migration(version=4, apply=_add_local_dates),
    Migration(version=5, apply=_add_sleep_metrics),
    Migration(version=6, apply=_scope_cloud_record_ids),
    Migration(version=7, apply=_add_raw_payloads),
    Migration(version=8, apply=_add_raw_payload_record_ids),
    Migration(version=9, apply=_add_device_context),
)
LATEST_SCHEMA_VERSION = MIGRATIONS[-1].version


def _check_integrity(conn: sqlite3.Connection) -> None:
    result = conn.execute("PRAGMA integrity_check").fetchone()
    if result is None or result[0] != "ok":
        detail = result[0] if result else "no result"
        raise sqlite3.DatabaseError(f"SQLite integrity check failed: {detail}")


def _backup_database(db_path: Path, from_version: int, to_version: int) -> Path | None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    backup_path = db_path.with_name(
        f"{db_path.name}.v{from_version}-to-v{to_version}.{timestamp}.bak"
    )
    try:
        with sqlite3.connect(db_path) as source, sqlite3.connect(backup_path) as backup:
            source.backup(backup)
            _check_integrity(backup)
            backup_version = int(backup.execute("PRAGMA user_version").fetchone()[0])
            if backup_version != from_version:
                raise sqlite3.DatabaseError(
                    f"SQLite backup schema version {backup_version} does not match "
                    f"source version {from_version}"
                )
    except Exception:
        backup_path.unlink(missing_ok=True)
        raise
    return backup_path


def run_migrations(db_path: Path | str) -> int:
    """Upgrade a database to the latest schema and return its version."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(path, isolation_level=None, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            current_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if current_version > LATEST_SCHEMA_VERSION:
                raise sqlite3.DatabaseError(
                    f"Database schema version {current_version} is newer than supported "
                    f"version {LATEST_SCHEMA_VERSION}"
                )

            _check_integrity(conn)
            pending = [
                migration for migration in MIGRATIONS if migration.version > current_version
            ]
            if any(migration.destructive for migration in pending):
                _backup_database(path, current_version, LATEST_SCHEMA_VERSION)

            for migration in pending:
                migration.apply(conn)
                conn.execute(f"PRAGMA user_version = {migration.version}")
                current_version = migration.version

            _check_integrity(conn)
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
            return current_version
