"""MCP server implementation for Zepp Life."""

import asyncio
import contextlib
import inspect
import json
import logging
import secrets
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import CallToolResult, TextContent, Tool

from zepp_life_mcp.adapters.base import DataAdapter
from zepp_life_mcp.adapters.cloud_session import CloudSessionAdapter
from zepp_life_mcp.adapters.export_file import ExportFileAdapter
from zepp_life_mcp.auth import load_token
from zepp_life_mcp.config import Config, load_config
from zepp_life_mcp.models import ConnectionStatus, QueryResponse
from zepp_life_mcp.services.query_service import QueryService
from zepp_life_mcp.services.sync_service import SyncService
from zepp_life_mcp.storage import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Server("zepp-life-mcp")


@dataclass
class RuntimeContext:
    config: Config | None = None
    db: Database | None = None
    adapter: DataAdapter | None = None
    sync_service: SyncService | None = None
    query_service: QueryService | None = None
    connect_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Hosted deployments run sync as a separate CronJob against the same SQLite
    # file, so the served process must not also write to it.
    read_only: bool = False


context = RuntimeContext()
CONNECTION_REQUIRED_TOOLS = {
    "sync_data",
    "get_profile",
    "get_daily_summary",
    "query_metric_series",
    "query_sleep",
    "query_workouts",
    "query_heart_rate",
    "query_body_measurements",
    "query_raw_payloads",
    "get_data_coverage",
}


async def ensure_connected() -> bool:
    if context.adapter is None:
        return True
    if context.db is None:
        return False
    if context.adapter.is_connected():
        return True

    async with context.connect_lock:
        if context.adapter.is_connected():
            return True
        result = context.adapter.connect()
        if inspect.isawaitable(result):
            result = await result
        if not result:
            logger.warning("Failed to connect to data source")
            return False

        user_id = context.adapter.get_user_id() or "unknown"
        context.sync_service = SyncService(
            context.adapter,
            context.db,
            archive_raw=context.config.store_raw_payloads if context.config else True,
        )
        context.query_service = QueryService(context.db, user_id)
        logger.info("Connected to data source")
        return True


TOOL_SPECS = (
        Tool(
            name="get_connection_status",
            description="Check connection status to data source and last sync time",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="sync_data",
            description="Synchronize data from source to local cache",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Types of data to sync (daily_activity, sleep, heart_rate, workouts, body_measurements)",
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
        ),
        Tool(
            name="get_profile",
            description="Get user profile information",
            inputSchema={
                "type": "object",
                "properties": {
                    "include_devices": {
                        "type": "boolean",
                        "description": "Include connected devices information",
                    },
                },
            },
        ),
        Tool(
            name="get_daily_summary",
            description="Get daily activity summary for a date or date range",
            inputSchema={
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
        ),
        Tool(
            name="query_metric_series",
            description="Query time series data for a specific metric",
            inputSchema={
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
                            "sleep_deep_minutes",
                            "sleep_rem_minutes",
                            "sleep_awake_minutes",
                            "sleep_wake_count",
                            "sleep_score",
                        ],
                        "description": (
                            "Metric to query. The sleep_* metrics come back aggregated, which is "
                            "how to ask a question about a year of nights without pulling every "
                            "session."
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
        ),
        Tool(
            name="query_sleep",
            description="Query sleep sessions for a date range",
            inputSchema={
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
        ),
        Tool(
            name="query_workouts",
            description="Query workouts for a date range",
            inputSchema={
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
        ),
        Tool(
            name="query_heart_rate",
            description="Query heart rate samples for a date range",
            inputSchema={
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
        ),
        Tool(
            name="query_body_measurements",
            description="Query body measurements (weight, body composition) for a date range",
            inputSchema={
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
        ),
        Tool(
            name="query_raw_payloads",
            description=(
                "List verbatim upstream responses held in the local archive. The typed tables "
                "keep a subset of what Zepp returns (193 fields per workout become a dozen); "
                "this reaches the rest. Bodies are omitted unless include_payload is set, "
                "because one band_data window can be several megabytes."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "endpoint": {
                        "type": "string",
                        "description": (
                            "e.g. band_data.summary, band_data.detail, sport.run.history, "
                            "sport.run.detail, weight.records"
                        ),
                    },
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                    "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"},
                    "include_payload": {
                        "type": "boolean",
                        "description": "Include the decoded body. Large; defaults to false.",
                    },
                    "limit": {"type": "integer", "description": "Max rows (default 20)"},
                },
            },
        ),
        Tool(
            name="get_data_coverage",
            description="Get data coverage information - which dates have data for each type",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific data types to check (default: all)",
                    },
                },
            },
        ),
)


WRITE_TOOLS = frozenset({"sync_data"})


@app.list_tools()
async def list_tools() -> list[Tool]:
    if context.read_only:
        return [tool for tool in TOOL_SPECS if tool.name not in WRITE_TOOLS]
    return list(TOOL_SPECS)


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> CallToolResult:
    try:
        if context.read_only and name in WRITE_TOOLS:
            result = {
                "status": "error",
                "error": f"{name} is disabled: this instance serves the local datastore read-only",
            }
            return CallToolResult(
                content=[TextContent(type="text", text=json.dumps(result))],
                isError=True,
            )
        # Read-only instances answer from the local database and never need the
        # upstream. Requiring a connection here would let an expired apptoken --
        # which should only ever break sync -- take down queries whose data is
        # already on disk.
        needs_connection = name in CONNECTION_REQUIRED_TOOLS and not context.read_only
        if needs_connection and not await ensure_connected():
            result = {
                "status": "error",
                "error": "Data source connection failed",
            }
            return CallToolResult(
                content=[TextContent(type="text", text=json.dumps(result))],
                isError=True,
            )
        handler = TOOL_HANDLERS.get(name)
        if handler is None:
            result = {
                "status": "error",
                "error": f"Unknown tool: {name}",
            }
        else:
            result = await handler(arguments)

        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, default=str))],
            isError=result.get("status") == "error",
        )

    except Exception:
        logger.exception(f"Error handling tool {name}")
        result = {
            "status": "error",
            "error": "Request failed",
        }
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result))],
            isError=True,
        )


async def _handle_get_connection_status(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.config or context.config.mode == "not_configured":
        return ConnectionStatus(
            mode="not_configured",
            connected=False,
            message="Server not configured. Run 'zepp-mcp setup' first.",
        ).model_dump()

    connected = context.adapter is not None and context.adapter.is_connected()

    # Get sync state from database
    last_sync = None
    available_types = []

    if context.db and context.adapter:
        source_type = context.config.mode
        user_id = context.adapter.get_user_id() or "unknown"
        for data_type in ["daily_activity", "sleep", "heart_rate", "workouts", "body_measurements"]:
            state = context.db.get_sync_state(source_type, user_id, data_type)
            if state and state.get("last_success_at"):
                available_types.append(data_type)
                sync_time = datetime.fromisoformat(state["last_success_at"])
                if last_sync is None or sync_time > last_sync:
                    last_sync = sync_time

    # Determine sync health
    sync_health = "unknown"
    if last_sync:
        if last_sync.tzinfo is None:
            last_sync = last_sync.replace(tzinfo=UTC)
        age_minutes = (datetime.now(UTC) - last_sync).total_seconds() / 60
        sync_health = (
            "healthy" if age_minutes < context.config.stale_after_minutes else "stale"
        )

    next_action = None
    if not connected:
        next_action = "Check export path configuration"
    elif sync_health == "stale":
        next_action = "Run sync_data to update cache"
    elif not available_types:
        next_action = "Run sync_data to import data"

    return ConnectionStatus(
        mode=context.config.mode,
        connected=connected,
        last_sync_at=last_sync,
        available_data_types=available_types,
        sync_health=sync_health,
        next_action=next_action,
        message="Connection is deferred until the first data request" if not connected else None,
    ).model_dump()


async def _handle_sync_data(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.sync_service:
        return {
            "status": "error",
            "error": "Sync service not initialized",
        }

    data_types = (
        arguments.get("data_types") or context.sync_service.adapter.get_available_data_types()
    )
    start_date = arguments.get("start_date")
    end_date = arguments.get("end_date")
    force_full = arguments.get("force_full_sync", False)

    sync_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    total_added = 0
    total_updated = 0
    total_skipped = 0
    types_synced = []
    failed_data_types = []

    for data_type in data_types:
        try:
            result = await context.sync_service.sync_data_type(
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                force_full=force_full,
            )
            total_added += result.get("added", 0)
            total_updated += result.get("updated", 0)
            total_skipped += result.get("skipped", 0)
            types_synced.append(data_type)
        except Exception as e:
            logger.error(f"Failed to sync {data_type}: {e}")
            failed_data_types.append(data_type)

    finished_at = datetime.now(UTC)

    return {
        "status": "ok" if types_synced else "error",
        "sync_id": sync_id,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "records_added": total_added,
        "records_updated": total_updated,
        "records_skipped": total_skipped,
        "data_types_synced": types_synced,
        "failed_data_types": failed_data_types,
        "error": "No data types synced successfully" if not types_synced else None,
    }


async def _handle_get_profile(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.adapter or not context.adapter.is_connected():
        return {
            "status": "error",
            "error": "Not connected to data source",
        }

    user_id = context.adapter.get_user_id() or "unknown"

    profile = {
        "user_id": user_id,
        "display_name": None,
        "timezone": context.config.timezone if context.config else "UTC",
        "devices": [],
    }

    if arguments.get("include_devices"):
        # TODO: Get devices from adapter
        pass

    return QueryResponse(
        status="ok",
        source=(
            context.config.mode
            if context.config and context.config.mode != "not_configured"
            else "unknown"
        ),
        data={"profile": profile},
    ).model_dump()


async def _handle_get_daily_summary(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {
            "status": "error",
            "error": "Query service not initialized",
        }

    # Handle single date or date range
    if "date" in arguments:
        start_date = arguments["date"]
        end_date = arguments["date"]
    else:
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")

    if not start_date or not end_date:
        return {
            "status": "error",
            "error": "Either 'date' or 'start_date' and 'end_date' required",
        }

    try:
        summaries = context.query_service.get_daily_summaries(start_date, end_date)
        return QueryResponse(
            status="ok",
            source="cache",
            timezone=arguments.get(
                "timezone", context.config.timezone if context.config else "UTC"
            ),
            data={"summaries": summaries},
        ).model_dump()
    except Exception:
        logger.exception("Daily summary query failed")
        return {
            "status": "error",
            "error": "Request failed",
        }


async def _handle_query_metric_series(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {
            "status": "error",
            "error": "Query service not initialized",
        }

    metric = arguments["metric"]
    start_date = arguments["start_date"]
    end_date = arguments["end_date"]
    granularity = arguments.get("granularity", "day")
    aggregation = arguments.get("aggregation", "sum")

    try:
        series = context.query_service.get_metric_series(
            metric=metric,
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
            aggregation=aggregation,
        )
        return QueryResponse(
            status="ok",
            source="cache",
            data={
                "metric": metric,
                "granularity": granularity,
                "aggregation": aggregation,
                "series": series,
            },
        ).model_dump()
    except Exception:
        logger.exception("Metric series query failed")
        return {
            "status": "error",
            "error": "Request failed",
        }


async def _handle_query_sleep(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {
            "status": "error",
            "error": "Query service not initialized",
        }

    start_date = arguments["start_date"]
    end_date = arguments["end_date"]
    include_naps = arguments.get("include_naps", True)
    include_stages = arguments.get("include_stages", True)

    try:
        sessions = context.query_service.get_sleep_sessions(
            start_date=start_date,
            end_date=end_date,
            include_naps=include_naps,
        )

        if not include_stages:
            for session in sessions:
                session.pop("stages", None)

        return QueryResponse(
            status="ok",
            source="cache",
            data={
                "sessions": sessions,
                "total_sessions": len(sessions),
            },
        ).model_dump()
    except Exception:
        logger.exception("Sleep query failed")
        return {
            "status": "error",
            "error": "Request failed",
        }


async def _handle_query_workouts(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {
            "status": "error",
            "error": "Query service not initialized",
        }

    start_date = arguments["start_date"]
    end_date = arguments["end_date"]
    activity_types = arguments.get("activity_types")
    min_duration = arguments.get("min_duration_minutes")
    min_distance_km = arguments.get("min_distance_km")

    try:
        workouts = context.query_service.get_workouts(
            start_date=start_date,
            end_date=end_date,
            activity_types=activity_types,
            min_duration=min_duration,
            min_distance_km=min_distance_km,
        )

        # Calculate summary
        total_duration = sum(w.get("duration_minutes", 0) for w in workouts)
        total_distance = sum(w.get("distance_m", 0) or 0 for w in workouts)
        total_calories = sum(w.get("calories_kcal", 0) or 0 for w in workouts)

        return QueryResponse(
            status="ok",
            source="cache",
            data={
                "workouts": workouts,
                "summary": {
                    "count": len(workouts),
                    "total_duration_minutes": total_duration,
                    "total_distance_m": total_distance,
                    "total_kcal": total_calories,
                },
            },
        ).model_dump()
    except Exception:
        logger.exception("Workout query failed")
        return {
            "status": "error",
            "error": "Request failed",
        }


async def _handle_query_heart_rate(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {
            "status": "error",
            "error": "Query service not initialized",
        }

    start_date = arguments["start_date"]
    end_date = arguments["end_date"]
    sample_type = arguments.get("sample_type")
    limit = arguments.get("limit")

    try:
        samples = context.query_service.get_heart_rate_samples(
            start_date=start_date,
            end_date=end_date,
            sample_type=sample_type,
            limit=limit,
        )
        return QueryResponse(
            status="ok",
            source="cache",
            data={
                "samples": samples,
                "count": len(samples),
            },
        ).model_dump()
    except Exception:
        logger.exception("Heart rate query failed")
        return {
            "status": "error",
            "error": "Request failed",
        }


async def _handle_query_body_measurements(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {
            "status": "error",
            "error": "Query service not initialized",
        }

    start_date = arguments["start_date"]
    end_date = arguments["end_date"]
    metrics = arguments.get("metrics")
    latest_only = arguments.get("latest_only", False)

    try:
        measurements = context.query_service.get_body_measurements(
            start_date=start_date,
            end_date=end_date,
            metrics=metrics,
        )

        if latest_only and measurements:
            measurements = [measurements[-1]]

        return QueryResponse(
            status="ok",
            source="cache",
            data={
                "measurements": measurements,
                "count": len(measurements),
            },
        ).model_dump()
    except Exception:
        logger.exception("Body measurement query failed")
        return {
            "status": "error",
            "error": "Request failed",
        }


async def _handle_query_raw_payloads(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {"status": "error", "error": "Query service not initialized"}
    try:
        rows = context.query_service.get_raw_payloads(
            endpoint=arguments.get("endpoint"),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
            include_payload=bool(arguments.get("include_payload")),
            limit=int(arguments.get("limit") or 20),
        )
        return QueryResponse(
            status="ok",
            source="cache",
            data={"payloads": rows, "total_payloads": len(rows)},
        ).model_dump()
    except Exception:
        logger.exception("Raw payload query failed")
        return {"status": "error", "error": "Raw payload query failed"}


async def _handle_get_data_coverage(arguments: dict[str, Any]) -> dict[str, Any]:
    if not context.query_service:
        return {
            "status": "error",
            "error": "Query service not initialized",
        }

    data_types = arguments.get("data_types")

    try:
        coverage = context.query_service.get_data_coverage(data_types)
        return QueryResponse(
            status="ok",
            source="cache",
            data={"coverage": coverage},
        ).model_dump()
    except Exception:
        logger.exception("Data coverage query failed")
        return {
            "status": "error",
            "error": "Request failed",
        }


ToolHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

TOOL_HANDLERS: dict[str, ToolHandler] = {
    "get_connection_status": _handle_get_connection_status,
    "sync_data": _handle_sync_data,
    "get_profile": _handle_get_profile,
    "get_daily_summary": _handle_get_daily_summary,
    "query_metric_series": _handle_query_metric_series,
    "query_sleep": _handle_query_sleep,
    "query_workouts": _handle_query_workouts,
    "query_heart_rate": _handle_query_heart_rate,
    "query_body_measurements": _handle_query_body_measurements,
    "query_raw_payloads": _handle_query_raw_payloads,
    "get_data_coverage": _handle_get_data_coverage,
}


async def close_runtime_context() -> None:
    if context.adapter is None:
        return
    close = getattr(context.adapter, "close", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        await result


async def _configure_runtime() -> None:
    context.config = load_config()
    context.db = Database(context.config.database_path)

    if context.config.mode == "export_file" and context.config.export_path:
        context.adapter = ExportFileAdapter(context.config.export_path)
        if context.adapter.connect():
            logger.info(f"Connected to export files at {context.config.export_path}")
        else:
            logger.warning(f"Failed to connect to export files at {context.config.export_path}")
    elif context.config.mode == "cloud_session":
        token, user_id = load_token()
        if token:
            context.adapter = CloudSessionAdapter(
                token,
                user_id,
                context.config.region,
                context.config.timezone,
            )
    if context.adapter and context.adapter.is_connected():
        context.sync_service = SyncService(
            context.adapter,
            context.db,
            archive_raw=context.config.store_raw_payloads if context.config else True,
        )
        context.query_service = QueryService(
            context.db, context.adapter.get_user_id() or "unknown"
        )
    else:
        # No live connection (commonly a lapsed apptoken). The archive is still
        # on disk, so resolve who it belongs to from the configured adapter or
        # from the database itself -- otherwise every query filters on "unknown"
        # and returns empty with status "ok", which reads as "no data" rather
        # than "misconfigured".
        user_id = context.adapter.get_user_id() if context.adapter else None
        if not user_id:
            user_id = context.db.sole_user_id()
        if not user_id:
            logger.warning("No user id available; queries will return nothing")
        context.query_service = QueryService(context.db, user_id or "unknown")


def build_http_app(auth_token: str | None, read_only: bool = False):
    """ASGI app exposing the MCP streamable-HTTP transport at /mcp.

    /healthz is deliberately outside the auth check so kubelet probes work
    without handing the cluster a credential.
    """
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Mount, Route

    context.read_only = read_only
    session_manager = StreamableHTTPSessionManager(app=app, json_response=False)

    async def handle_mcp(scope, receive, send):
        await session_manager.handle_request(scope, receive, send)

    async def healthz(_request):
        return PlainTextResponse("ok")

    @contextlib.asynccontextmanager
    async def lifespan(_app):
        async with session_manager.run():
            await _configure_runtime()
            try:
                yield
            finally:
                await close_runtime_context()

    http_app: Any = McpPathNormalizer(
        Starlette(
            routes=[Route("/healthz", healthz, methods=["GET"]), Mount("/mcp", app=handle_mcp)],
            lifespan=lifespan,
        )
    )

    if not auth_token:
        logger.warning(
            "Serving HTTP without an auth token: every caller reaches the full archive"
        )
        return http_app
    return BearerAuthMiddleware(http_app, auth_token, exempt_paths=frozenset({"/healthz"}))


class McpPathNormalizer:
    """Serve /mcp and /mcp/ identically.

    Starlette's Mount answers the un-slashed form with a 307 to the slashed one.
    Redirect handling on POST is not universal across HTTP clients, and the
    endpoint URL is something a person types into a client config by hand, so
    both spellings have to work rather than one of them working by luck.
    """

    def __init__(self, app, mount_path: str = "/mcp"):
        self.app = app
        self.mount_path = mount_path

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") == self.mount_path:
            scope = dict(scope)
            scope["path"] = f"{self.mount_path}/"
            raw_path = scope.get("raw_path")
            if raw_path:
                scope["raw_path"] = raw_path + b"/"
        await self.app(scope, receive, send)


class BearerAuthMiddleware:
    """Require `Authorization: Bearer <token>` on everything but the exempt paths."""

    def __init__(self, app, token: str, exempt_paths: frozenset[str]):
        self.app = app
        self.token = token
        self.exempt_paths = exempt_paths

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") in self.exempt_paths:
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers") or [])
        provided = headers.get(b"authorization", b"").decode("utf-8", "replace")
        # compare_digest keeps the check constant-time; both sides are str.
        if not secrets.compare_digest(provided, f"Bearer {self.token}"):
            from starlette.responses import JSONResponse

            response = JSONResponse(
                {"status": "error", "error": "unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


async def main(
    transport: str = "stdio",
    host: str = "0.0.0.0",
    port: int = 8080,
    auth_token: str | None = None,
    read_only: bool = False,
) -> None:
    if transport == "http":
        import uvicorn

        http_app = build_http_app(auth_token, read_only=read_only)
        logger.info("Serving MCP over HTTP at http://%s:%d/mcp", host, port)
        server = uvicorn.Server(
            uvicorn.Config(http_app, host=host, port=port, log_level="info", lifespan="on")
        )
        await server.serve()
        return

    context.read_only = read_only
    await _configure_runtime()
    try:
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options(),
            )
    finally:
        await close_runtime_context()
