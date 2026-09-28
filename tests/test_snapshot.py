import sqlite3
from types import SimpleNamespace

import pytest

from zepp_life_mcp import main
from zepp_life_mcp.models import DailyActivity
from zepp_life_mcp.storage import Database, snapshot_database


def _live_archive(tmp_path):
    path = tmp_path / "zepp_life.db"
    Database(path).upsert_daily_activity(
        DailyActivity(
            id="daily-1",
            provider="zepp_life",
            source_type="cloud_session",
            source_record_id=None,
            user_id="user-1",
            device_id=None,
            collected_at=None,
            date="2024-03-01",
            steps=4321,
            distance_m=0,
            active_kcal=0,
            total_kcal=None,
            floors=None,
            active_minutes=None,
        )
    )
    return path


def test_snapshot_is_one_consistent_file(tmp_path):
    live = _live_archive(tmp_path)
    target = tmp_path / "snapshot" / "zepp_life.db"

    snapshot_database(live, target)

    assert sorted(p.name for p in target.parent.iterdir()) == ["zepp_life.db"]
    with sqlite3.connect(target) as copy:
        assert copy.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert copy.execute("SELECT steps FROM daily_activity").fetchone()[0] == 4321


def test_snapshot_replaces_the_previous_one(tmp_path):
    live = _live_archive(tmp_path)
    target = tmp_path / "zepp_life.snapshot.db"
    target.write_bytes(b"yesterday's copy")

    snapshot_database(live, target)

    with sqlite3.connect(target) as copy:
        assert copy.execute("SELECT COUNT(*) FROM daily_activity").fetchone()[0] == 1


def test_snapshot_of_a_missing_database_fails_without_writing_anything(tmp_path):
    target = tmp_path / "snapshot" / "zepp_life.db"

    with pytest.raises(FileNotFoundError):
        snapshot_database(tmp_path / "absent.db", target)

    assert not (tmp_path / "absent.db").exists()
    assert not target.exists()
    assert not target.with_name("zepp_life.db.partial").exists()


def test_snapshot_command_writes_the_configured_database(monkeypatch, tmp_path, capsys):
    live = _live_archive(tmp_path)
    monkeypatch.setattr(main, "load_config", lambda: SimpleNamespace(database_path=live))
    target = tmp_path / "out.db"

    main.cmd_snapshot(SimpleNamespace(to=str(target)))

    assert target.is_file()
    assert str(target) in capsys.readouterr().out
