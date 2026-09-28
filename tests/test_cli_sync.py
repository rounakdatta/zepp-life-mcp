from argparse import Namespace
from types import SimpleNamespace

import pytest

from zepp_life_mcp import main


class FakeAdapter:
    def __init__(self, *args, **kwargs):
        pass

    async def connect(self):
        return True

    def get_available_data_types(self):
        return ["daily_activity", "sleep"]


class RecordingSyncService:
    failing: frozenset[str] = frozenset()
    last_kwargs: dict = {}

    def __init__(self, *args, **kwargs):
        pass

    async def sync_data_type(self, data_type, **kwargs):
        RecordingSyncService.last_kwargs = kwargs
        if data_type in self.failing:
            raise RuntimeError("upstream 502")
        return {"added": 0, "updated": 0}


class SleepFailingSyncService(RecordingSyncService):
    failing = frozenset({"sleep"})


def _patch(monkeypatch, tmp_path, service_cls):
    config = SimpleNamespace(
        mode="cloud_session",
        database_path=tmp_path / "zepp.db",
        region="us",
        timezone="Asia/Kolkata",
        api_host=None,
        store_raw_payloads=True,
    )
    monkeypatch.setattr(main, "load_config", lambda: config)
    monkeypatch.setattr(main, "load_token", lambda: ("token", "user-1"))
    monkeypatch.setattr(main, "CloudSessionAdapter", FakeAdapter)
    monkeypatch.setattr(main, "SyncService", service_cls)


def _args(**overrides):
    return Namespace(
        **{"type": None, "start_date": None, "end_date": None, "lookback_days": None, **overrides}
    )


async def test_sync_exits_nonzero_when_any_type_fails(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path, SleepFailingSyncService)

    with pytest.raises(SystemExit) as exc_info:
        await main.cmd_sync_async(_args())

    assert exc_info.value.code == 1


async def test_sync_passes_lookback_days_through(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path, RecordingSyncService)

    await main.cmd_sync_async(_args(lookback_days=14))

    assert RecordingSyncService.last_kwargs["lookback_days"] == 14
