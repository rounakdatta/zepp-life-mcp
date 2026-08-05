# Changelog

## Unreleased

### Added

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
