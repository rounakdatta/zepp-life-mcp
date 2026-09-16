# Changelog

## Unreleased

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
