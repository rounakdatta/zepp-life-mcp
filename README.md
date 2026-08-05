# Zepp Life MCP

[![CI](https://github.com/kubulashvili/zepp-life-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/kubulashvili/zepp-life-mcp/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/kubulashvili/zepp-life-mcp)](https://github.com/kubulashvili/zepp-life-mcp/releases)
[![License](https://img.shields.io/github/license/kubulashvili/zepp-life-mcp)](https://github.com/kubulashvili/zepp-life-mcp/blob/main/LICENSE)

MCP server for Zepp Life data.

This project provides local caching, sync, and MCP tools for Zepp Life data from either exported files or the Zepp cloud session flow.

## Supported sources

- `export_file` for local Zepp exports
- `cloud_session` for `apptoken`-based cloud access

## Current data coverage

The current implementation supports:

- daily steps, distance, and active calories
- sleep sessions with light, deep, REM, awake time, and wake count
- passive and resting heart rate (`slp.rhr`)
- workouts with readable sport names for known Zepp sport codes
- weight and body-composition measurements

Cloud coverage can vary by account, region, and upstream endpoint stability. Export mode is the safest option when you need predictable full-history access.

Cloud connections are lazy: server startup and `tools/list` do not wait for Zepp login. The first data tool establishes one shared connection. If `user_id` is omitted, the adapter attempts to discover the numeric UID from the last 30 days of band summary data and stores it in the system keyring.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

## Setup

### Cloud session

You need an `apptoken`.

Typical flow:

1. Open `https://user.huami.com/privacy2/index.html`
2. Sign in to the Zepp Life account
3. Open browser DevTools
4. Find the `apptoken` cookie

Then configure the server:

```bash
zepp-life-mcp setup --mode cloud_session --token "<apptoken>" --region eu
zepp-life-mcp doctor
```

`--user-id` remains supported and can be supplied explicitly if automatic discovery is unavailable.

### Export file mode

```bash
zepp-life-mcp setup --mode export_file --export-path ~/Downloads/ZeppExport
zepp-life-mcp doctor
```

## Use

```bash
zepp-life-mcp sync --start-date 2022-01-01 --end-date 2022-12-31
zepp-life-mcp serve
```

## MCP client config

Example `Claude Desktop` config:

```json
{
  "mcpServers": {
    "zepp-life": {
      "command": "zepp-life-mcp",
      "args": ["serve"]
    }
  }
}
```

## MCP tools

All tool responses keep the backward-compatible JSON envelope with `status: "ok"` or `status: "error"`. Failed calls also use MCP `isError: true`.

| Tool | Purpose |
|---|---|
| `get_connection_status` | Report configuration, lazy connection state, and sync health |
| `sync_data` | Sync selected data types with optional `start_date`, `end_date`, and `force_full_sync` |
| `get_profile` | Return the connected user ID/timezone and accept the compatible `include_devices` option |
| `get_daily_summary` | Query one date or a date range of daily activity |
| `query_metric_series` | Query `steps`, `distance_m`, `active_kcal`, `weight_kg`, or `sleep_minutes` by day/week/month with sum/avg/min/max/latest |
| `query_sleep` | Query sleep sessions by sleep start date with optional naps/stages |
| `query_workouts` | Query workouts with activity, duration, and distance filters |
| `query_heart_rate` | Query resting/active/passive/workout heart-rate samples |
| `query_body_measurements` | Query weight/body metrics with optional latest-only output |
| `get_data_coverage` | Report first/last dates and days with data by type |

## Sync and storage behavior

- SQLite schema upgrades run automatically through `PRAGMA user_version` migrations.
- Destructive rebuild migrations create a timestamped database backup first.
- Sync cursors are scoped by source, user, and data type.
- Empty successful syncs update the attempt/success state and advance the logical date cursor.
- Failed or partial syncs report `failed_data_types`; a failed pass never advances its cursor.
- Existing 0.1.0 databases are upgraded in place without reinterpreting legacy calendar dates.

## Example prompts

- `Show my workouts from the last 30 days`
- `How has my weight changed this year?`
- `Summarize my sleep for the past week`
- `Sync my latest Zepp Life data`

## Commands

```bash
zepp-life-mcp --help
zepp-life-mcp setup --help
zepp-life-mcp doctor
zepp-life-mcp sync --help
zepp-life-mcp serve
```

## Development

```bash
uv run ruff check src tests scripts
uv run pytest -q
uv run python -m build
```

## Troubleshooting

- `Connection: failed`
  - verify `apptoken`
  - provide `--user-id` if automatic UID discovery is unavailable
- `No export data found`
  - verify the extracted archive path
  - verify that CSV or JSON export files are present
- `sync` returns no data
  - try another date range
  - try export mode if cloud coverage is incomplete

## Security

- `apptoken` is stored via the system keyring
- do not commit `.env`, exported health data, or local SQLite files
- prefer interactive setup over pasting secrets into shell history

## Disclaimer

This is an unofficial project and is not affiliated with Xiaomi or Zepp Health.
