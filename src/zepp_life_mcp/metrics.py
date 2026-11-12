"""Prometheus metrics for watching the sync from outside this process.

Served at ``/metrics`` on the HTTP transport, outside the bearer check like
``/healthz``, so a prober can read them without holding the credential that
opens the archive. That is only safe because everything here is a timestamp or a
flag: **nothing added to this module may reveal a value from the archive.**

Two independent views of "how fresh is the data", so a stale archive can be told
apart from a broken sync:

* what the archive holds (``zepp_archive_newest_day_timestamp_seconds``), and
* what Zepp itself holds (``zepp_upstream_newest_day_timestamp_seconds``),
  asked by :class:`UpstreamProbe`. It shares nothing with the sync's code path
  (not the adapter, the cursor or the parser) beyond the definition of a filed
  day, so a sync bug that silently drops data shows up as the archive falling
  behind it. Its 30-day window is far below band_data's 500-row cap (the very
  cap that once hid the newest days from the sync), and wide enough that a phone
  silent for days still has a newest day to report.

When the phone has not uploaded, both views stop at the same day: the archive is
stale but the sync is fine.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from zepp_life_mcp.storage import Database

logger = logging.getLogger(__name__)

DEFAULT_API_HOST = "https://api-mifit.huami.com"
PROBE_INTERVAL_SECONDS = 600
PROBE_WINDOW_DAYS = 30
PROBE_TIMEOUT_SECONDS = 15.0

SYNC_DATA_TYPES = (
    "daily_activity",
    "sleep",
    "heart_rate",
    "workouts",
    "workout_details",
    "body_measurements",
)

# The day columns that are cheap to MAX() over. heart_rate_samples is left out on
# purpose: it is by far the largest table, and its sync_state already says whether
# heart-rate passes succeed.
ARCHIVE_DAY_QUERIES = {
    "daily_activity": "SELECT MAX(date) FROM daily_activity WHERE user_id = ?",
    "sleep": "SELECT MAX(local_date) FROM sleep_sessions WHERE user_id = ?",
    "workouts": "SELECT MAX(local_date) FROM workouts WHERE user_id = ?",
}


@dataclass(frozen=True)
class ProbeResult:
    checked_at: float
    ok: bool
    http_status: int | None
    newest_day: date | None


def _decode_summary(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(base64.b64decode(value))
    except (ValueError, TypeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def newest_filed_day(rows: Iterable[Any]) -> date | None:
    """The latest day Zepp has filed data for.

    A day counts once it has a date and a non-empty summary, which is the same
    test the sync uses to decide that a day exists at all. Anything else in a row
    is ignored, so this cannot inherit the sync's parsing bugs.
    """
    newest: date | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_day = str(row.get("date_time") or row.get("date") or "")
        if not raw_day or not _decode_summary(row.get("summary")):
            continue
        try:
            day = date.fromisoformat(raw_day[:10])
        except ValueError:
            continue
        if newest is None or day > newest:
            newest = day
    return newest


def _band_rows(payload: Any) -> list[Any] | None:
    """The day rows of a band_data response, or None when it is not one."""
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(base64.b64decode(data))
        except (ValueError, TypeError):
            return None
    if isinstance(data, list):
        return data
    return None


class UpstreamProbe:
    """Asks Zepp for its newest filed day, at most once per ``interval`` seconds.

    A scrape never waits on Zepp: :meth:`snapshot` returns the last answer at
    once and, when it is due, starts a refresh in the background. A slow Zepp
    therefore delays the number, never the scrape (a prober times scrapes out
    and would read that as a failure).

    ``credentials`` is called on every probe, so a renewed token (mounted as a
    file) or a user id learnt after start-up is picked up without a restart.
    """

    def __init__(
        self,
        credentials: Callable[[], tuple[str | None, str | None]],
        timezone: str = "UTC",
        api_host: str | None = None,
        interval: float = PROBE_INTERVAL_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self._credentials = credentials
        self._tz = ZoneInfo(timezone)
        self._host = api_host or DEFAULT_API_HOST
        self._interval = interval
        self._transport = transport
        self._clock = clock
        self._last: ProbeResult | None = None
        self._task: asyncio.Task[ProbeResult] | None = None

    def snapshot(self) -> ProbeResult | None:
        """The latest answer, starting a background refresh when one is due."""
        now = self._clock()
        due = self._last is None or now - self._last.checked_at >= self._interval
        if due and (self._task is None or self._task.done()):
            self._task = asyncio.get_running_loop().create_task(self.refresh())
        return self._last

    async def refresh(self) -> ProbeResult:
        now = self._clock()
        try:
            result = await self._probe(now)
        except Exception as exc:  # a probe bug must never take /metrics down
            logger.warning("Upstream probe raised %s", type(exc).__name__)
            result = ProbeResult(now, False, None, None)
        self._last = result
        return result

    async def _probe(self, now: float) -> ProbeResult:
        token, user_id = self._credentials()
        if not token or not user_id:
            return ProbeResult(now, False, None, None)
        today = datetime.fromtimestamp(now, self._tz).date()
        params = {
            "query_type": "summary",
            "device_type": "android_phone",
            "userid": user_id,
            "from_date": (today - timedelta(days=PROBE_WINDOW_DAYS)).isoformat(),
            "to_date": today.isoformat(),
        }
        headers = {
            "apptoken": token,
            "appPlatform": "web",
            "appname": "com.xiaomi.hm.health",
        }
        try:
            async with httpx.AsyncClient(
                base_url=self._host,
                headers=headers,
                timeout=PROBE_TIMEOUT_SECONDS,
                transport=self._transport,
            ) as client:
                response = await client.get("/v1/data/band_data.json", params=params)
        except httpx.HTTPError as exc:
            # The exception text can carry the request URL; its type is enough.
            logger.warning("Upstream probe failed: %s", type(exc).__name__)
            return ProbeResult(now, False, None, None)

        if response.status_code != 200:
            return ProbeResult(now, False, response.status_code, None)
        try:
            payload = response.json()
        except ValueError:
            return ProbeResult(now, False, response.status_code, None)
        # Zepp answers a rejected token with HTTP 200 and a code other than 1.
        if isinstance(payload, dict) and payload.get("code") not in (None, 1):
            return ProbeResult(now, False, response.status_code, None)
        rows = _band_rows(payload)
        if rows is None:
            return ProbeResult(now, False, response.status_code, None)
        return ProbeResult(now, True, response.status_code, newest_filed_day(rows))


def _sqlite_utc_seconds(value: Any) -> float | None:
    """CURRENT_TIMESTAMP text ("2026-11-11 17:15:13", UTC) as epoch seconds."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def day_seconds(day: date) -> float:
    """A calendar day as its UTC-midnight timestamp.

    Both newest-day metrics use this, so they subtract to a whole number of days.
    """
    return datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()


def _parse_day(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _fmt(value: float) -> str:
    return repr(float(value)) if value != int(value) else str(int(value))


class _Exposition:
    """Collects samples per metric family and writes each family as one block,
    as the text format requires (a strict parser drops the type otherwise)."""

    def __init__(self) -> None:
        self._families: dict[str, tuple[str, list[str]]] = {}

    def add(
        self, name: str, help_text: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        label_text = ""
        if labels:
            label_text = "{" + ",".join(f'{k}="{v}"' for k, v in sorted(labels.items())) + "}"
        family = self._families.setdefault(name, (help_text, []))
        family[1].append(f"{name}{label_text} {_fmt(value)}")

    def text(self) -> str:
        lines: list[str] = []
        for name, (help_text, samples) in self._families.items():
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} gauge")
            lines.extend(samples)
        return "\n".join(lines) + "\n"


def render_metrics(
    db: Database,
    source_type: str,
    user_id: str | None,
    probe: ProbeResult | None,
) -> str:
    out = _Exposition()

    for data_type in SYNC_DATA_TYPES:
        state = db.get_sync_state(source_type, user_id, data_type) if user_id else None
        if not state:
            continue
        labels = {"data_type": data_type}
        attempt = _sqlite_utc_seconds(state.get("last_attempt_at"))
        success = _sqlite_utc_seconds(state.get("last_success_at"))
        if attempt is not None:
            out.add(
                "zepp_sync_last_attempt_timestamp_seconds",
                "When a sync pass last tried this data type.",
                attempt,
                labels,
            )
        if success is not None:
            out.add(
                "zepp_sync_last_success_timestamp_seconds",
                "When a sync pass last succeeded for this data type.",
                success,
                labels,
            )
        out.add(
            "zepp_sync_last_attempt_failed",
            "1 when the most recent sync pass for this data type ended in an error.",
            1 if state.get("last_error") else 0,
            labels,
        )

    if user_id:
        newest_days = db.newest_archive_days(user_id, ARCHIVE_DAY_QUERIES)
        for data_type, raw_day in newest_days.items():
            day = _parse_day(raw_day)
            if day is None:
                continue
            out.add(
                "zepp_archive_newest_day_timestamp_seconds",
                "The newest day the archive holds, as its UTC-midnight timestamp.",
                day_seconds(day),
                {"data_type": data_type},
            )

    if probe is not None:
        out.add(
            "zepp_upstream_probe_timestamp_seconds",
            "When Zepp was last asked for its newest day.",
            probe.checked_at,
        )
        out.add(
            "zepp_upstream_probe_ok",
            "1 when Zepp answered the last probe with day data; 0 for a rejected token or an error.",
            1 if probe.ok else 0,
        )
        if probe.http_status is not None:
            out.add(
                "zepp_upstream_probe_http_status",
                "HTTP status of the last probe.",
                probe.http_status,
            )
        if probe.ok and probe.newest_day is not None:
            out.add(
                "zepp_upstream_newest_day_timestamp_seconds",
                "The newest day Zepp has filed band data for, as its UTC-midnight timestamp.",
                day_seconds(probe.newest_day),
            )

    return out.text()
