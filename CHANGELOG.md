# Changelog

## 0.4.0

Everything here came out of actually using the server for an analysis and hitting
the wall each time.

### Added

- `tz_offset_seconds` on sleep, daily activity and workouts. The band reports a
  UTC offset per day and it is the only location signal available, so without it
  a trip is invisible and "where was I" cannot be asked at all.
- `algo_version` on sleep sessions. The device's sleep algorithm changed version
  mid-2026 and silently redistributed time between stages; a longitudinal query
  spanning that change invents an effect. The version is now attached to every
  row so the discontinuity is visible instead of inferred.
- `deep_minutes` and `light_minutes` as real columns. `rem_minutes` already had
  one, so deep sleep -- the stage people actually ask about -- could previously
  only be reached by parsing the stages JSON of every row.
- Sleep metrics in `query_metric_series`: `sleep_deep_minutes`,
  `sleep_rem_minutes`, `sleep_awake_minutes`, `sleep_wake_count`, `sleep_score`.
  A year of nights is now a few hundred bytes instead of the ~400 KB a full
  session dump costs.
- `query_raw_payloads` tool. The archive held 193 fields per workout, GPS tracks
  and per-second heart rate, and was reachable only by opening the SQLite file.
  Bodies are omitted unless asked for.

### Fixed

- `query_sleep` and `query_workouts` now return `local_date` and `timezone`.
  Both were stored and neither was ever serialized, so every session came back
  without a date on it.
- `is_nap` is now inferred rather than always false: under three hours AND
  starting between 06:00 and 20:00 local. Zepp exposes no nap flag (`supNap`
  only reports device capability), so this is explicitly a heuristic, and
  nothing is classified when the day has no timezone offset.

### Changed

- An unmapped workout code is now `sport_204` rather than a bare `204`, which
  was indistinguishable from a real activity label. **Breaking** for any caller
  filtering `activity_types` on a raw numeric code.

## 0.3.3

### Fixed

- The sync CronJob no longer passes `--start-date` unless `sync.startDate` is
  set, and the default is now empty. Passing it unconditionally overrode the
  stored sync cursor, so every scheduled run re-fetched the whole history and
  re-upserted every heart-rate sample instead of doing an incremental pass.

## 0.3.2

### Fixed

- The sync CronJob's schedule is now interpreted in a configurable timezone via
  `sync.timeZone`. Kubernetes reads a cron schedule in UTC unless `spec.timeZone`
  is set, so a schedule written as a local time ran hours away from where it was
  meant to, with nothing in the manifest to show the discrepancy.

## 0.3.1

### Fixed

- `/mcp` answered with a 307 redirect to `/mcp/`, because Starlette's `Mount`
  redirects the un-slashed path. Redirect-following on POST is not universal
  across HTTP clients, and the endpoint URL is typed into a client config by
  hand, so relying on one spelling working by luck was wrong. Both now serve
  directly.

## 0.3.0

### Fixed

- **`band_data.json` truncation silently dropped the newest data.** The endpoint caps
  every response at 500 rows and discards the most recent dates when it does, while
  still returning HTTP 200. A single request for the default full-sync range
  (`2020-01-01`..today) therefore returned history that stopped months early, and the
  sync cursor then advanced past the missing days so they were never refetched.
  Requests are now split into windows that stay under the cap, and a window that
  still comes back at the cap halves itself and retries. Verified against a live
  account: the same range went from 500 days ending 2026-07-11 to 566 days ending
  2026-09-15, recovering 66 days.

### Added

- `raw_payloads` table archiving every upstream response verbatim (gzipped, deduplicated
  by content hash, append-only so a changed refetch is versioned rather than overwritten),
  with `Database.read_raw_payloads()` to replay them and `Database.raw_payload_stats()`
  to summarize. The typed tables are lossy by construction — Zepp returns 193 fields per
  workout and the schema keeps 8 — so the archive keeps a mapping change backfillable
  from local data instead of requiring a refetch.
- `store_raw_payloads` config flag is honoured again; it was previously documented as a
  no-op and gated nothing.
- Python 3.13 CI coverage and post-build wheel/CLI smoke testing
- resting heart-rate samples from sleep summaries
- REM minutes, wake counts, readable workout sport names, and all documented metric aggregations
- lazy cloud connection with bounded numeric user-ID discovery stored in the system keyring

### Changed

- preserved all 0.1.0 CLI commands, MCP tool names/arguments, and `status: "ok"|"error"` response envelopes
- upgraded SQLite automatically with versioned, transactional, idempotent migrations and backups before destructive rebuilds
- scoped sync state by source, user, and data type with logical date watermarks and explicit partial/total failure reporting
- used timezone-aware UTC timestamps and stored local calendar dates without shifting legacy rows
- returned failed MCP tool calls with protocol-level `isError: true` while hiding backend paths and secrets
- generated tool definitions and dispatch from registries under a lifecycle-managed runtime context
- replaced nondeterministic export IDs with stable IDs and moved adapter warnings from stdout to logging

### Removed

- unused autosync code with an unawaited synchronization call
- obsolete root-level API/export smoke scripts; supported CLI commands replace them

## 0.1.0

- initial standalone `zepp-life-mcp` repository
- Zepp cloud sync for steps, sleep, heart rate, workouts, body measurements
- export-file mode for local data import
- MCP tools for sync, summaries, workouts, heart rate, body measurements, coverage
