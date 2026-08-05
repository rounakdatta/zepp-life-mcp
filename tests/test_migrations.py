import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from zepp_life_mcp.storage import Database
from zepp_life_mcp.storage.migrations import LATEST_SCHEMA_VERSION, MIGRATIONS, run_migrations


def _create_v010_database(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE daily_activity (
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
        """)
        conn.execute(
            """
            INSERT INTO daily_activity (
                id, provider, source_type, user_id, date, steps, distance_m, active_kcal
            ) VALUES ('legacy-1', 'zepp_life', 'export_file', 'user-1', '2024-01-02', 321, 250, 12)
            """
        )
        conn.execute(
            """
            INSERT INTO daily_activity (
                id, provider, source_type, user_id, date, steps, distance_m, active_kcal
            ) VALUES ('legacy-2', 'zepp_life', 'export_file', 'user-1', '2024-01-02', 654, 500, 24)
            """
        )


def test_upgrade_v010_database_preserves_data(tmp_path):
    db_path = tmp_path / "zepp.db"
    _create_v010_database(db_path)

    database = Database(db_path)

    with database._get_connection() as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        record = conn.execute(
            "SELECT id, steps FROM daily_activity WHERE id = 'legacy-2'"
        ).fetchone()
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert version == LATEST_SCHEMA_VERSION
    assert tuple(record) == ("legacy-2", 654)
    assert {
        "daily_activity",
        "sleep_sessions",
        "workouts",
        "body_measurements",
        "heart_rate_samples",
        "sync_state",
    } <= tables


def test_migration_rerun_is_idempotent(tmp_path):
    db_path = tmp_path / "zepp.db"
    _create_v010_database(db_path)

    first_version = run_migrations(db_path)
    second_version = run_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM daily_activity").fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert first_version == second_version == LATEST_SCHEMA_VERSION
    assert count == 1
    assert integrity == "ok"


def test_device_key_migration_deduplicates_null_identity_and_creates_backup(tmp_path):
    db_path = tmp_path / "zepp.db"
    _create_v010_database(db_path)

    run_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, device_key FROM daily_activity ORDER BY id"
        ).fetchall()
    backups = list(tmp_path.glob("zepp.db.v*-to-v*.*.bak"))

    assert rows == [("legacy-2", "")]
    assert len(backups) == 1


def test_migration_backup_includes_committed_wal_data(tmp_path):
    db_path = tmp_path / "zepp.db"
    _create_v010_database(db_path)

    with sqlite3.connect(db_path) as writer:
        assert writer.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
        writer.execute("UPDATE daily_activity SET steps = 777 WHERE id = 'legacy-2'")
        writer.commit()

        run_migrations(db_path)

        backup_path = next(tmp_path.glob("zepp.db.v*-to-v*.*.bak"))
        with sqlite3.connect(backup_path) as backup:
            integrity = backup.execute("PRAGMA integrity_check").fetchone()[0]
            steps = backup.execute(
                "SELECT steps FROM daily_activity WHERE id = 'legacy-2'"
            ).fetchone()[0]

    assert integrity == "ok"
    assert steps == 777


def test_concurrent_database_opens_migrate_once_and_preserve_data(tmp_path):
    db_path = tmp_path / "zepp.db"
    _create_v010_database(db_path)
    barrier = Barrier(2)

    def open_database():
        barrier.wait()
        return Database(db_path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(open_database) for _ in range(2)]
        databases = [future.result(timeout=10) for future in futures]

    with sqlite3.connect(db_path) as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        rows = conn.execute("SELECT id, steps FROM daily_activity ORDER BY id").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert all(database.db_path == db_path for database in databases)
    assert version == LATEST_SCHEMA_VERSION
    assert rows == [("legacy-2", 654)]
    assert integrity == "ok"


def test_local_date_migration_preserves_legacy_calendar_day(tmp_path):
    db_path = tmp_path / "zepp.db"
    with sqlite3.connect(db_path) as conn:
        for migration in MIGRATIONS[:3]:
            migration.apply(conn)
            conn.execute(f"PRAGMA user_version = {migration.version}")
        conn.execute(
            """
            INSERT INTO sleep_sessions (
                id,
                provider,
                source_type,
                user_id,
                sleep_id,
                start_at,
                end_at,
                duration_minutes,
                time_asleep_minutes,
                time_awake_minutes
            ) VALUES (
                'legacy-sleep',
                'zepp_life',
                'export_file',
                'user-1',
                'sleep-1',
                '2024-03-31T23:30:00',
                '2024-04-01T07:00:00',
                450,
                420,
                30
            )
            """
        )

    run_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        local_date, rem_minutes, wake_count = conn.execute(
            """
            SELECT local_date, rem_minutes, wake_count
            FROM sleep_sessions
            WHERE id = 'legacy-sleep'
            """
        ).fetchone()

    assert local_date == "2024-03-31"
    assert rem_minutes == 0
    assert wake_count == 0


def test_cloud_identity_migration_rekeys_existing_rows_without_data_loss(tmp_path):
    db_path = tmp_path / "zepp.db"
    with sqlite3.connect(db_path) as conn:
        for migration in MIGRATIONS[:5]:
            migration.apply(conn)
            conn.execute(f"PRAGMA user_version = {migration.version}")
        conn.execute(
            """
            INSERT INTO daily_activity (
                id, provider, source_type, user_id, date, steps, device_key
            ) VALUES (
                'cloud_2024-01-02', 'zepp_life', 'cloud_session', 'user-1',
                '2024-01-02', 321, ''
            )
            """
        )
        conn.execute(
            """
            INSERT INTO sleep_sessions (
                id, provider, source_type, user_id, sleep_id, local_date,
                start_at, end_at, duration_minutes, time_asleep_minutes,
                time_awake_minutes
            ) VALUES (
                'cloud_sleep_2024-01-02', 'zepp_life', 'cloud_session', 'user-1',
                'sleep_2024-01-02', '2024-01-02', '2024-01-02T22:00:00+00:00',
                '2024-01-03T06:00:00+00:00', 480, 450, 30
            )
            """
        )
        conn.execute(
            """
            INSERT INTO workouts (
                id, provider, source_type, user_id, workout_id, local_date,
                activity_type, start_at, end_at, duration_minutes
            ) VALUES (
                'cloud_track-1', 'zepp_life', 'cloud_session', 'user-1', 'track-1',
                '2024-01-02', 'running', '2024-01-02T12:00:00+00:00',
                '2024-01-02T12:30:00+00:00', 30
            )
            """
        )
        conn.execute(
            """
            INSERT INTO body_measurements (
                id, provider, source_type, user_id, device_key, local_date,
                timestamp, weight_kg
            ) VALUES (
                'cloud_weight_weight-1', 'zepp_life', 'cloud_session', 'user-1', '',
                '2024-01-02', '2024-01-02T08:00:00+00:00', 70
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO heart_rate_samples (
                id, provider, source_type, source_record_id, user_id, local_date,
                timestamp, bpm, sample_type
            ) VALUES (?, 'zepp_life', 'cloud_session', ?, 'user-1', '2024-01-02', ?, ?, ?)
            """,
            [
                (
                    "cloud_rhr_2024-01-02",
                    "sleep_2024-01-02",
                    "2024-01-02T06:00:00+00:00",
                    54,
                    "resting",
                ),
                (
                    "cloud_hr_2024-01-02_1",
                    None,
                    "2024-01-02T00:01:00+00:00",
                    60,
                    "passive",
                ),
            ],
        )

    run_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        ids = {
            table: [row[0] for row in conn.execute(f"SELECT id FROM {table} ORDER BY id")]
            for table in (
                "daily_activity",
                "sleep_sessions",
                "workouts",
                "body_measurements",
                "heart_rate_samples",
            )
        }
        steps = conn.execute("SELECT steps FROM daily_activity").fetchone()[0]

    assert ids == {
        "daily_activity": ["cloud_user-1_2024-01-02"],
        "sleep_sessions": ["cloud_sleep_user-1_2024-01-02"],
        "workouts": ["cloud_user-1_track-1"],
        "body_measurements": ["cloud_weight_user-1_weight-1"],
        "heart_rate_samples": [
            "cloud_hr_user-1_2024-01-02_1",
            "cloud_rhr_user-1_2024-01-02",
        ],
    }
    assert steps == 321
