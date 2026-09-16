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
- for workouts: the reported `city`, a `geohash` and the recording `timezone`
- the device's UTC offset per day (`tz_offset_seconds`) -- the only location
  signal the band provides, and what makes a multi-timezone series readable
- the device's sleep-algorithm version per night (`algo_version`). Stage splits
  are **not comparable across a change in this value**: a version bump can move
  time between deep, light and REM without any change in the sleep itself.

Cloud coverage can vary by account, region, and upstream endpoint stability.

### Raw payload archive

Every upstream response is also stored verbatim in the `raw_payloads` table (gzipped,
deduplicated by content hash). The typed tables are deliberately narrow — Zepp returns
193 fields per workout and the schema keeps 8 — so the archive is what makes a later
mapping change backfillable from local data rather than requiring a refetch from an
account that may no longer serve the history.

```python
from zepp_life_mcp.storage import Database

db = Database("~/.local/share/zepp-life-mcp/zepp_life.db")
db.raw_payload_stats()                                   # what is archived
for row in db.read_raw_payloads(endpoint="sport.run.history"):
    row["payload"]                                       # the original JSON
```

Set `store_raw_payloads` to `false` in the config to turn this off.

### Upstream row cap

`band_data.json` truncates any response to 500 rows and drops the **most recent** dates
when it does, while still answering HTTP 200. Sync therefore splits wide ranges into
windows that stay under the cap; a window that still returns at the cap (a multi-device
account emits one row per date *per device*) halves itself and retries. A single
unwindowed request for `2020-01-01`..today was measured returning history that stopped
two months early.

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

## Serving over HTTP

`serve` speaks stdio by default, which is what a local MCP client wants. For a
hosted instance it can speak the MCP streamable-HTTP transport instead:

```bash
export ZEPP_MCP_AUTH_TOKEN='<a long random string>'
zepp-life-mcp serve --transport http --port 8080 --read-only
```

- the endpoint is `POST /mcp`; clients send `Authorization: Bearer $ZEPP_MCP_AUTH_TOKEN`
- `/healthz` is outside the auth check so container probes need no credential
- `--read-only` removes `sync_data`, so a separate scheduled `sync` can own the
  database and there is exactly one writer
- the token is read from the environment, never a flag, because an argv secret is
  visible in `ps` and in a pod spec

Starting without `ZEPP_MCP_AUTH_TOKEN` serves unauthenticated and logs a warning.

### Configuration by environment

Everything needed to run without a writable config directory:

| Variable | Purpose |
|---|---|
| `ZEPP_APP_TOKEN` / `ZEPP_APP_TOKEN_FILE` | the Zepp session cookie, inline or from a mounted file |
| `ZEPP_USER_ID` | optional; saves a discovery request |
| `ZEPP_MODE`, `ZEPP_REGION`, `ZEPP_TIMEZONE` | config overrides |
| `ZEPP_DATABASE_PATH`, `ZEPP_EXPORT_PATH` | where the data lives |
| `ZEPP_STORE_RAW_PAYLOADS` | archive upstream responses verbatim |
| `ZEPP_MCP_AUTH_TOKEN`, `ZEPP_MCP_TRANSPORT`, `ZEPP_MCP_HOST`, `ZEPP_MCP_PORT`, `ZEPP_MCP_READ_ONLY` | serving |

Environment wins over `config.json`, and the keyring is only consulted when no
token is supplied — a container has no Secret Service, and that is not an error.

## Container and Helm chart

```bash
docker build -t zepp-life-mcp .
docker run --rm -p 8080:8080 -v zepp-data:/data \
  -e ZEPP_MODE=cloud_session -e ZEPP_APP_TOKEN=... -e ZEPP_MCP_AUTH_TOKEN=... \
  zepp-life-mcp serve --read-only
```

`charts/zepp-life-mcp` deploys a read-only HTTP server plus a nightly sync
CronJob sharing one `ReadWriteOnce` volume. Because that volume is node-local,
both workloads carry the same `nodeSelector` and the Deployment uses the
`Recreate` strategy — a second pod cannot attach the same claim. SQLite runs in
WAL mode so the server keeps reading while the job writes. The PVC is annotated
`helm.sh/resource-policy: keep`: the archive cannot be re-fetched once Zepp stops
serving the history.

Credentials come from one Secret (`apptoken`, `bearerToken`); nothing sensitive
belongs in `values.yaml`.

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
| `query_raw_payloads` | List verbatim upstream responses from the local archive |
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
