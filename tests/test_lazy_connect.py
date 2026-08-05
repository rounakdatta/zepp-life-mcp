import asyncio
import json
from typing import Any, cast

from mcp.types import CallToolResult, TextContent

from zepp_life_mcp import server
from zepp_life_mcp.config import Config
from zepp_life_mcp.storage import Database


class LazyAdapter:
    def __init__(self):
        self.connected = False
        self.connect_calls = 0

    async def connect(self):
        self.connect_calls += 1
        await asyncio.sleep(0)
        self.connected = True
        return True

    def is_connected(self):
        return self.connected

    def get_user_id(self):
        return "123456"


class ClosableAdapter(LazyAdapter):
    def __init__(self):
        super().__init__()
        self.closed = False

    async def close(self):
        self.closed = True


def _payload(result: CallToolResult):
    content = result.content[0]
    assert isinstance(content, TextContent)
    return json.loads(content.text)


async def test_list_tools_and_connection_status_do_not_connect(monkeypatch, tmp_path):
    adapter = LazyAdapter()
    monkeypatch.setattr(server.context, "adapter", cast(Any, adapter))
    monkeypatch.setattr(server.context, "config", Config(mode="cloud_session"))
    monkeypatch.setattr(server.context, "db", Database(tmp_path / "test.db"))

    await cast(Any, server.list_tools)()
    result = cast(CallToolResult, await server.call_tool("get_connection_status", {}))

    assert adapter.connect_calls == 0
    assert result.isError is False
    assert _payload(result)["connected"] is False

    unknown = cast(CallToolResult, await server.call_tool("does_not_exist", {}))
    assert unknown.isError is True
    assert adapter.connect_calls == 0


async def test_concurrent_first_use_connects_once_and_rebuilds_services(monkeypatch, tmp_path):
    adapter = LazyAdapter()
    database = Database(tmp_path / "test.db")
    monkeypatch.setattr(server.context, "adapter", cast(Any, adapter))
    monkeypatch.setattr(server.context, "db", database)
    monkeypatch.setattr(server.context, "sync_service", None)
    monkeypatch.setattr(server.context, "query_service", None)
    monkeypatch.setattr(server.context, "connect_lock", asyncio.Lock())

    results = await asyncio.gather(server.ensure_connected(), server.ensure_connected())

    assert results == [True, True]
    assert adapter.connect_calls == 1
    assert server.context.sync_service is not None
    assert server.context.query_service is not None
    assert server.context.query_service.user_id == "123456"


async def test_runtime_context_closes_adapter(monkeypatch):
    adapter = ClosableAdapter()
    monkeypatch.setattr(server.context, "adapter", cast(Any, adapter))

    await server.close_runtime_context()

    assert adapter.closed is True
