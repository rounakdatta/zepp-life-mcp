import json
import logging

import pytest
from pydantic import ValidationError

from zepp_life_mcp.config import Config, load_config, save_config


def test_config_roundtrip(tmp_path, monkeypatch):
    config_file = tmp_path / "config.json"
    monkeypatch.setattr("zepp_life_mcp.config.get_config_path", lambda: config_file)

    config = Config(mode="cloud_session", region="eu")
    save_config(config)
    loaded = load_config()

    assert loaded.mode == "cloud_session"
    assert loaded.region == "eu"
    assert loaded.database_path.name == "zepp_life.db"


def test_config_rejects_unknown_timezone():
    with pytest.raises(ValidationError, match="Unknown timezone"):
        Config(timezone="Mars/Olympus_Mons")


def test_load_config_marks_legacy_no_op_keys_as_deprecated(tmp_path, monkeypatch, caplog):
    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "mode": "cloud_session",
                "auto_sync_on_start": False,
                "store_raw_payloads": False,
                "default_lookback_days": 7,
                "logs_path": str(tmp_path / "legacy.log"),
            }
        )
    )
    monkeypatch.setattr("zepp_life_mcp.config.get_config_path", lambda: config_file)

    with caplog.at_level(logging.WARNING, logger="zepp_life_mcp.config"):
        loaded = load_config()

    assert loaded.auto_sync_on_start is False
    assert loaded.logs_path == tmp_path / "legacy.log"
    assert "retained but ignored" in caplog.text
    schema = Config.model_json_schema()["properties"]
    assert all(
        schema[key]["deprecated"] is True and schema[key]["x-no-op"] is True
        for key in (
            "auto_sync_on_start",
            "store_raw_payloads",
            "default_lookback_days",
            "logs_path",
        )
    )
