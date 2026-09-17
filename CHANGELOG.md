# Changelog

## 0.5.3

### Fixed

- **Resting heart rate could be filed a year outside the window it came from.**
  Upstream occasionally records a sleep block under one day while its timestamps
  point somewhere else entirely -- three such days exist in a real archive, with
  2024 timestamps on 2025 records. `iter_sleep_sessions` rejects them, but the
  heart-rate path needs only `rhr` and `ed` and so accepted them, putting
  resting samples in the archive before the device existed and making
  `get_data_coverage` report `heart_rate` starting a year before every other
  data type. A sample derived from a window must now fall inside it.
- A migration removes samples already stored outside the span of that user's own
  daily activity. Conservative by design: only users who have daily activity at
  all are touched, and the verbatim payloads remain in `raw_payloads`, so this
  drops a derived row that was known to be wrong rather than losing anything.
- **`dt` is REM and the no-stage fallback ignored it**, reporting REM as zero and
  understating those nights by however long REM was. On a night that does carry
  stages, `dp + lt + dt + wk` reconstructs the session length exactly, which is
  what identifies the field. Latent rather than active on the archives seen so
  far -- every stage-less night there is empty -- but silent when it does bite.

## 0.5.2

### Fixed

- `get_profile` returned `Not connected to data source` on a **read-only**
  instance -- the configuration the deployment actually runs -- even though the
  user id and timezone it reports live in the local database. It was the one
  tool that could not answer at all once the endpoint stopped holding a live
  cloud session. It now answers from local data, and only the device list, which
  genuinely needs the upstream, is omitted when there is no connection.

## 0.5.1

### Fixed

- **Startup could crash-loop forever against a busy database.** Every process
  runs the migration check when it opens the database, including the server on
  each boot, and it opened an *exclusive* transaction unconditionally -- even
  when the schema was already current and there was nothing to do. A sync job
  holding the write lock therefore made the server fail to start with
  `database is locked` after the 30-second timeout, and with a frequent sync
  schedule it never got a clean window: an unbreakable CrashLoopBackOff.
  Observed in production while backfilling on a 10-minute schedule.

  The common case is now answered by a plain reader, which a writer cannot
  block, and returns in microseconds. A genuine migration retries with backoff
  and re-checks whether another process completed it in the meantime. A
  non-lock error is still fatal on the first attempt -- a corrupt database is
  not a busy one, and retrying would only hide it.

## 0.5.0

Clears the backlog of things flagged in review and never actually fixed.

### Fixed

- **Workout pace and step count were hardcoded `None`** while the columns existed
  and the values sat in the same response that was already being parsed. 387 of
  425 workouts had pace data and all of it was discarded. Upstream reports pace
  in seconds per *metre*; the conversion is verified against distance over
  duration on real runs. Upstream's `max_pace` is the **fastest** pace, so it is
  numerically smaller than the average -- documented on the field, because the
  name invites the opposite assumption.
- **`get_profile` returned `devices: []` behind a `# TODO`** while the devices
  endpoint answered fine. It now lists the bound devices with their firmware;
  a failure there is logged and leaves the rest of the profile intact.
- **`region` never selected an API host.** It is still accepted, and now says so
  in the code and the README rather than looking functional. `api_host` /
  `ZEPP_API_HOST` is the real knob -- Zepp's regional hostnames are not reliably
  derivable from a region string, and guessing one would break a working account.
- **Export mode's empty heart rate is documented as final**, not as a stub
  awaiting a parser: Zepp's export archives contain no heart-rate series, so
  cloud is the only source. The README no longer implies export mode is the more
  complete of the two.

### Added

- `vo2max` and `training_effect` columns on workouts, present on 120 and 388 of
  425 records respectively. VO2max is the device's **estimate** and can step when
  firmware recalibrates, which is noted on the field.
- Values upstream writes as `0` or `-1` for "not recorded" are stored as absent
  rather than as zero, so they cannot quietly drag an average down.

## 0.4.2

### Fixed

- `query_heart_rate` could kill the server. It selected every sample in the date
  range and then filtered by type and applied the caller's limit **in Python**,
  so a wide range materialised hundreds of thousands of dicts before returning a
  handful. Passive heart rate is one sample per minute -- about 800,000 rows
  across the archive -- and a container with a memory limit is simply killed.
  Observed in production: one 18-month query took the endpoint down until the
  pod restarted. The filter and the bound now happen in SQL, an unbounded
  request is capped at 10,000 samples, and an explicit limit is capped at
  100,000, so no single query can end the process.

## 0.4.1

### Added

- Workouts now carry the place they happened: `city`, `geohash` and a real
  `timezone`, all read from the upstream record's own `syncedTimezone`, `city`
  and `location` fields. All three were present on every sync and none reached
  the database. `tz_offset_seconds` is derived from the zone at the workout's
  own instant, so daylight saving resolves correctly instead of being assumed.
  This is strictly better than the daily summary's numeric offset: it names the
  city rather than leaving a UTC offset to be guessed at.

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

- Setting `journal_mode=WAL` at open is now best-effort. It needs a brief
  exclusive lock, so two processes opening the same database at once made one of
  them raise `database is locked` -- introduced in 0.3.2 and only reproducible
  under real contention. The mode lives in the file header, so whichever opener
  wins has already set it for the others.

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
