import json
import sys
from argparse import Namespace
from datetime import datetime
from typing import cast

import pytest
from mcp.types import CallToolResult, TextContent

from zepp_life_mcp import server
from zepp_life_mcp.main import cmd_setup
from zepp_life_mcp.main import main as cli_main
from zepp_life_mcp.models import QueryResponse
from zepp_life_mcp.server import call_tool, list_tools

EXPECTED_TOOL_SPECS = [
    {
        "name": "get_connection_status",
        "description": "Check connection status to data source and last sync time",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "sync_data",
        "description": "Synchronize data from source to local cache",
        "inputSchema": {
            "type": "object",
            "properties": {
                "data_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Types of data to sync (daily_activity, sleep, heart_rate, workouts, "
                        "body_measurements)"
                    ),
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)",
                },
                "force_full_sync": {
                    "type": "boolean",
                    "description": "Force full sync instead of incremental",
                },
            },
        },
    },
    {
        "name": "get_profile",
        "description": "Get user profile information",
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_devices": {
                    "type": "boolean",
                    "description": "Include connected devices information",
                }
            },
        },
    },
    {
        "name": "get_daily_summary",
        "description": "Get daily activity summary for a date or date range",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Single date (YYYY-MM-DD)",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date for range (YYYY-MM-DD)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date for range (YYYY-MM-DD)",
                },
                "timezone": {
                    "type": "string",
                    "description": "Timezone (default from config)",
                },
            },
        },
    },
    {
        "name": "query_metric_series",
        "description": "Query time series data for a specific metric",
        "inputSchema": {
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "enum": [
                        "steps",
                        "distance_m",
                        "active_kcal",
                        "weight_kg",
                        "sleep_minutes",
                    ],
                    "description": "Metric to query",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)",
                },
                "granularity": {
                    "type": "string",
                    "enum": ["day", "week", "month"],
                    "description": "Aggregation granularity",
                },
                "aggregation": {
                    "type": "string",
                    "enum": ["sum", "avg", "min", "max", "latest"],
                    "description": "Aggregation method",
                },
            },
            "required": ["metric", "start_date", "end_date"],
        },
    },
    {
        "name": "query_sleep",
        "description": "Query sleep sessions for a date range",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)",
                },
                "include_naps": {
                    "type": "boolean",
                    "description": "Include nap sessions",
                },
                "include_stages": {
                    "type": "boolean",
                    "description": "Include sleep stage breakdown",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "query_workouts",
        "description": "Query workouts for a date range",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)",
                },
                "activity_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by activity types (running, cycling, etc.)",
                },
                "min_duration_minutes": {
                    "type": "integer",
                    "description": "Minimum duration in minutes",
                },
                "min_distance_km": {
                    "type": "number",
                    "description": "Minimum distance in kilometers",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "query_heart_rate",
        "description": "Query heart rate samples for a date range",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)",
                },
                "sample_type": {
                    "type": "string",
                    "enum": ["resting", "active", "passive", "workout"],
                    "description": "Filter by sample type",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of samples to return",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "query_body_measurements",
        "description": "Query body measurements (weight, body composition) for a date range",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)",
                },
                "metrics": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": [
                            "weight_kg",
                            "bmi",
                            "body_fat_pct",
                            "muscle_mass_kg",
                            "water_pct",
                        ],
                    },
                    "description": "Specific metrics to include",
                },
                "latest_only": {
                    "type": "boolean",
                    "description": "Return only the latest measurement",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_data_coverage",
        "description": "Get data coverage information - which dates have data for each type",
        "inputSchema": {
            "type": "object",
            "properties": {
                "data_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific data types to check (default: all)",
                }
            },
        },
    },
]


async def test_list_tools_schema_snapshot():
    tools = await list_tools()  # pyright: ignore[reportCallIssue]

    assert len(tools) == 10
    assert [
        {
            "name": tool.name,
            "description": tool.description,
            "inputSchema": tool.inputSchema,
        }
        for tool in tools
    ] == EXPECTED_TOOL_SPECS


@pytest.mark.parametrize("status", ["ok", "error"])
def test_query_response_contract(status):
    response = QueryResponse(
        status=status,
        source="cache",
        data={"records": []},
        error="backend unavailable" if status == "error" else None,
    ).model_dump()

    assert set(response) == {"status", "source", "generated_at", "timezone", "data", "error"}
    assert response["status"] == status
    assert response["source"] == "cache"
    assert isinstance(response["generated_at"], datetime)
    assert response["timezone"] == "UTC"
    assert response["data"] == {"records": []}
    assert response["error"] == ("backend unavailable" if status == "error" else None)
    assert "success" not in response


@pytest.mark.parametrize(
    ("arguments", "expected_fragments"),
    [
        (
            ["--help"],
            [
                "usage: zepp-life-mcp [-h] {serve,setup,doctor,sync} ...",
                "MCP server for Zepp Life data",
                "serve",
                "setup",
                "doctor",
                "sync",
            ],
        ),
        (
            ["setup", "--help"],
            [
                "--mode {export_file,cloud_session}",
                "--export-path EXPORT_PATH",
                "--token TOKEN",
                "--user-id USER_ID",
                "--region REGION",
            ],
        ),
        (["doctor", "--help"], ["usage: zepp-life-mcp doctor [-h]"]),
        (
            ["sync", "--help"],
            [
                "--type {daily_activity,sleep,heart_rate,workouts,workout_details,body_measurements}",
                "--start-date START_DATE",
                "--end-date END_DATE",
            ],
        ),
        (["serve", "--help"], ["usage: zepp-life-mcp serve [-h]"]),
    ],
)
def test_cli_help_contract(monkeypatch, capsys, arguments, expected_fragments):
    monkeypatch.setattr(sys, "argv", ["zepp-life-mcp", *arguments])

    with pytest.raises(SystemExit) as exc_info:
        cli_main()

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    for fragment in expected_fragments:
        assert fragment in output


def test_cloud_setup_masks_token_output(monkeypatch, capsys):
    token = "abcdefghijklmnop"
    saved_tokens = []
    monkeypatch.setattr("zepp_life_mcp.main.save_token", lambda *args: saved_tokens.append(args))
    monkeypatch.setattr("zepp_life_mcp.main.save_config", lambda config: None)

    cmd_setup(
        Namespace(
            mode="cloud_session",
            token=token,
            user_id=None,
            region="eu",
            export_path=None,
        )
    )

    output = capsys.readouterr().out
    assert "abcd***" in output
    assert token not in output
    assert saved_tokens == [(token, None)]


def test_cloud_setup_masks_user_id_output(monkeypatch, capsys):
    user_id = "account-12345678"
    monkeypatch.setattr("zepp_life_mcp.main.save_token", lambda *args: None)
    monkeypatch.setattr("zepp_life_mcp.main.save_config", lambda config: None)

    cmd_setup(
        Namespace(
            mode="cloud_session",
            token="abcdefghijklmnop",
            user_id=user_id,
            region="eu",
            export_path=None,
        )
    )

    output = capsys.readouterr().out
    assert user_id not in output
    assert "************5678" in output


def _tool_payload(result: CallToolResult):
    assert len(result.content) == 1
    content = result.content[0]
    assert isinstance(content, TextContent)
    return json.loads(content.text)


async def _call_tool(name, arguments):
    return cast(CallToolResult, await call_tool(name, arguments))


async def test_unknown_tool_is_protocol_error():
    result = await _call_tool("does_not_exist", {})

    assert result.isError is True
    assert _tool_payload(result) == {
        "status": "error",
        "error": "Unknown tool: does_not_exist",
    }


async def test_validation_failure_is_protocol_error(monkeypatch):
    monkeypatch.setattr(server.context, "query_service", object())

    result = await _call_tool("query_metric_series", {})

    assert result.isError is True
    assert _tool_payload(result) == {"status": "error", "error": "Request failed"}


async def test_uninitialized_service_is_protocol_error(monkeypatch):
    monkeypatch.setattr(server.context, "query_service", None)

    result = await _call_tool(
        "query_sleep",
        {"start_date": "2024-01-01", "end_date": "2024-01-01"},
    )

    assert result.isError is True
    assert _tool_payload(result) == {
        "status": "error",
        "error": "Query service not initialized",
    }


class FailingQueryService:
    def get_sleep_sessions(self, **kwargs):
        raise RuntimeError("secret=/tmp/private-token.txt")


async def test_backend_failure_is_protocol_error_without_sensitive_details(monkeypatch):
    monkeypatch.setattr(server.context, "query_service", FailingQueryService())

    result = await _call_tool(
        "query_sleep",
        {"start_date": "2024-01-01", "end_date": "2024-01-01"},
    )
    payload = _tool_payload(result)

    assert result.isError is True
    assert payload == {"status": "error", "error": "Request failed"}
    assert "/tmp" not in str(payload)


async def test_total_sync_failure_is_protocol_error(monkeypatch):
    class FailingSyncService:
        adapter = type("Adapter", (), {"get_available_data_types": lambda self: ["sleep"]})()

        async def sync_data_type(self, **kwargs):
            raise RuntimeError("backend secret")

    monkeypatch.setattr(server.context, "sync_service", FailingSyncService())
    result = await _call_tool("sync_data", {"data_types": ["sleep"]})

    assert result.isError is True
    assert _tool_payload(result)["failed_data_types"] == ["sleep"]
