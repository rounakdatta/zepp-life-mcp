"""Cloud session adapter for direct API access to Zepp Life."""

import base64
import json
import logging
from collections.abc import AsyncIterator, Callable, Container
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from zepp_life_mcp.adapters.base import DataAdapter, parse_sleep_summary
from zepp_life_mcp.auth import save_user_id
from zepp_life_mcp.models import (
    BodyMeasurement,
    DailyActivity,
    HeartRateSample,
    SleepSession,
    Workout,
)

logger = logging.getLogger(__name__)


class AdapterFetchError(RuntimeError):
    pass

SPORT_TYPE_MAP = {
    "1": "running",
    "6": "walking",
    "8": "treadmill",
    "9": "cycling",
    "10": "indoor_cycling",
    "11": "treadmill",
    "12": "elliptical",
    "13": "rowing",
    "14": "pool_swimming",
    "16": "freestyle",
    "17": "jump_rope",
}

# band_data.json truncates any response to this many rows, and it drops the MOST
# RECENT dates when it does. Measured live: a 600-day request returns 500 rows
# ending two months early, a 500-day request returns the correct tail. Requesting
# a wide range therefore loses the newest data silently, with HTTP 200.
BAND_DATA_ROW_CAP = 500
# Rows are per (date, device), so a multi-device account can exceed the cap well
# inside 500 calendar days. The window halves itself whenever a response comes
# back at the cap, so the starting value only affects request count, not coverage.
BAND_DATA_WINDOW_DAYS = 400

# What a connected cloud session can sync, in a fixed order. This is a capability
# list, not a data-presence list: whether a type has rows is a question for sync,
# not for connect. It used to be probed and returned via list(set(...)), which was
# both nondeterministic and able to drop a type from a default sync because one
# probe happened to fail.
CLOUD_DATA_TYPES = (
    "daily_activity",
    "sleep",
    "heart_rate",
    "workouts",
    "workout_details",
    "body_measurements",
)

RawSink = Callable[..., Any]


def _scaled(value: Any, factor: float) -> float | None:
    """A positive numeric field, scaled; anything else is absent rather than zero.

    Upstream uses 0 and -1 interchangeably for "not recorded", and storing those
    as real measurements would quietly poison any average taken over them.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number * factor if number > 0 else None


def _positive_int(value: Any) -> int | None:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


class CloudSessionAdapter(DataAdapter):
    """Adapter for accessing Zepp Life cloud APIs."""

    ZEPP_API_BASE = "https://api-mifit.huami.com"
    ZEPP_AUTH_BASE = "https://account.huami.com"
    ZEPP_USER_API = "https://api-user.huami.com"
    ZEPP_WEIGHT_API = "https://api-mifit.zepp.com"

    def __init__(
        self,
        app_token: str | None = None,
        user_id: str | None = None,
        region: str = "eu",
        timezone: str = "UTC",
        api_host: str | None = None,
    ):
        self.app_token = app_token
        self.user_id = user_id
        # `region` does NOT select a host and never has -- it is kept because the
        # CLI and config have always accepted it. Zepp's regional hostnames are
        # not reliably derivable from it, so guessing one would break a working
        # account for no gain. `api_host` is the real knob for anyone who needs a
        # different endpoint.
        self.region = region
        self.api_host = api_host or self.ZEPP_API_BASE
        self.timezone = timezone
        self._timezone = ZoneInfo(timezone)
        self._connected = False
        self._client: httpx.AsyncClient | None = None
        self._available_types: list[str] = []
        self._raw_sink: RawSink | None = None

    def set_raw_sink(self, sink: RawSink | None) -> None:
        """Register a callback that archives every upstream response verbatim."""
        self._raw_sink = sink

    def _archive(
        self,
        endpoint: str,
        params: dict[str, Any],
        payload: Any,
        window_start: str | None = None,
        window_end: str | None = None,
        http_status: int | None = None,
        record_id: str | None = None,
    ) -> None:
        if self._raw_sink is None:
            return
        try:
            self._raw_sink(
                source_type="cloud_session",
                user_id=self.user_id or "unknown",
                endpoint=endpoint,
                payload=payload,
                request_params=params,
                window_start=window_start,
                window_end=window_end,
                http_status=http_status,
                record_id=record_id,
            )
        except Exception as exc:
            # The mapped record still lands; surface loudly so a silently empty
            # archive cannot be mistaken for a complete one.
            logger.error("Failed to archive raw payload for %s: %s", endpoint, exc)

    def _resolve_range(
        self,
        start_date: str | None,
        end_date: str | None,
        lookback_days: int,
    ) -> tuple[str, str]:
        if not end_date:
            end_date = datetime.now(self._timezone).date().isoformat()
        if not start_date:
            start_date = (date.fromisoformat(end_date) - timedelta(days=lookback_days)).isoformat()
        return start_date, end_date

    async def _fetch_band_window(
        self,
        query_type: str,
        window_start: str,
        window_end: str,
        context: str,
    ) -> tuple[Any, int]:
        client = self._client
        if client is None:
            raise AdapterFetchError(f"Failed to fetch {context}: no client")

        params = {
            "query_type": query_type,
            "device_type": "android_phone",
            "userid": self.user_id,
            "from_date": window_start,
            "to_date": window_end,
        }
        response = await client.get("/v1/data/band_data.json", params=params)
        if response.status_code != 200:
            raise AdapterFetchError(f"Failed to fetch {context}: HTTP {response.status_code}")

        payload = response.json()
        self._archive(
            f"band_data.{query_type}",
            params,
            payload,
            window_start=window_start,
            window_end=window_end,
            http_status=response.status_code,
        )

        parsed = self._parse_band_data(payload)
        row_count = len(parsed) if isinstance(parsed, (list, dict)) else 0

        # Truncation is detected by the known row cap. If upstream ever lowers it,
        # the newest dates would go missing quietly again, so make the shortfall
        # visible instead of trusting the constant.
        newest = max(
            (date_str for date_str, _ in self._iter_band_summary_entries(parsed)),
            default=None,
        )
        if newest and row_count < BAND_DATA_ROW_CAP:
            shortfall = (date.fromisoformat(window_end) - date.fromisoformat(newest)).days
            if shortfall > 7:
                logger.warning(
                    "band_data %s for %s..%s returned %d rows ending %s, %d days short of the "
                    "window; expected if the band simply did not sync, but also what an "
                    "unknown upstream row cap would look like",
                    query_type,
                    window_start,
                    window_end,
                    row_count,
                    newest,
                    shortfall,
                )

        return parsed, row_count

    async def _iter_band_windows(
        self,
        query_type: str,
        start_date: str,
        end_date: str,
        context: str,
    ) -> AsyncIterator[Any]:
        """Cover [start_date, end_date] in windows that stay under the row cap.

        A single request for the whole range would come back capped at
        BAND_DATA_ROW_CAP rows with the newest dates missing, and the caller
        would have no way to tell that from a genuinely short history.
        """
        cursor = date.fromisoformat(start_date)
        final = date.fromisoformat(end_date)
        window = BAND_DATA_WINDOW_DAYS

        while cursor <= final:
            window_end = min(cursor + timedelta(days=window - 1), final)
            parsed, row_count = await self._fetch_band_window(
                query_type, cursor.isoformat(), window_end.isoformat(), context
            )

            if row_count >= BAND_DATA_ROW_CAP and window > 1:
                window = max(1, window // 2)
                logger.warning(
                    "band_data %s returned %d rows (cap %d) for %s..%s; "
                    "retrying that window at %d days",
                    query_type,
                    row_count,
                    BAND_DATA_ROW_CAP,
                    cursor.isoformat(),
                    window_end.isoformat(),
                    window,
                )
                continue

            if row_count >= BAND_DATA_ROW_CAP:
                logger.error(
                    "band_data %s hit the %d-row cap on the single day %s; "
                    "that day may be truncated upstream",
                    query_type,
                    BAND_DATA_ROW_CAP,
                    cursor.isoformat(),
                )

            yield parsed
            cursor = window_end + timedelta(days=1)

    async def _iter_band_summary_days(
        self,
        start_date: str,
        end_date: str,
        context: str,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        async for parsed in self._iter_band_windows("summary", start_date, end_date, context):
            for date_str, day_data in self._iter_band_summary_entries(parsed):
                yield date_str, day_data

    async def _iter_band_detail_rows(
        self,
        start_date: str,
        end_date: str,
        context: str,
    ) -> AsyncIterator[dict[str, Any]]:
        async for parsed in self._iter_band_windows("detail", start_date, end_date, context):
            rows = parsed if isinstance(parsed, list) else []
            for row in rows:
                if isinstance(row, dict):
                    yield row

    @staticmethod
    def _tz_offset(day_data: dict[str, Any]) -> int | None:
        """The device's UTC offset for that day, in seconds.

        This is the only location signal the band provides -- there is no GPS in
        the daily summary -- so it is what makes "where was I" answerable at all.
        """
        try:
            return int(day_data.get("tz"))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _looks_like_a_nap(start_at: datetime, offset: int | None, asleep_minutes: int) -> bool:
        """Heuristic: a short sleep that starts in daylight hours.

        Zepp exposes no nap flag -- `supNap` only says the device supports the
        feature -- so this is inferred, not reported. Kept deliberately
        conservative: under three hours AND beginning between 06:00 and 20:00
        local. Without an offset the local hour is unknowable and nothing is
        classified.
        """
        if offset is None or asleep_minutes >= 180:
            return False
        local_hour = (start_at + timedelta(seconds=offset)).hour
        return 6 <= local_hour < 20

    def _workout_local_date(self, item: dict[str, Any]) -> str | None:
        start_time = item.get("start_time")
        end_time = item.get("end_time")
        run_time = item.get("run_time", 0)
        end_ts = int(float(end_time)) if end_time else None
        duration_sec = int(float(run_time)) if run_time else 0
        start_ts = (
            int(float(start_time)) if start_time else (end_ts - duration_sec if end_ts else None)
        )
        if not start_ts:
            return None
        return self._local_date(self._utc_from_timestamp(start_ts))

    async def get_devices(self) -> list[dict[str, Any]]:
        """The bands and scales bound to this account.

        get_profile has reported `devices: []` behind a TODO since the first
        release even though this endpoint answers fine. Failure is non-fatal:
        a profile without a device list is still a useful profile.
        """
        client = self._client
        if client is None or not self.is_connected():
            return []
        try:
            response = await client.get(
                f"/users/{self.user_id}/devices", params={"deviceType": "all"}
            )
            if response.status_code != 200:
                logger.warning("Device listing returned HTTP %s", response.status_code)
                return []
            payload = response.json()
        except Exception as exc:
            logger.warning("Could not list devices: %s", exc)
            return []

        self._archive(
            "users.devices",
            {"deviceType": "all"},
            payload,
            http_status=200,
        )
        devices = []
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            devices.append(
                {
                    "device_id": str(item.get("deviceId") or "") or None,
                    "device_type": str(item.get("deviceType") or "") or None,
                    "device_source": str(item.get("deviceSource") or "") or None,
                    "mac_address": str(item.get("macAddress") or "") or None,
                    "firmware_version": str(item.get("firmwareVersion") or "") or None,
                }
            )
        return devices

    async def iter_workout_details(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        skip_ids: Container[str] = frozenset(),
    ) -> AsyncIterator[tuple[str, bool]]:
        """Archive one run/detail.json per workout.

        This is the single largest source of device data -- GPS track, per-second
        heart rate, pace, speed, altitude, gait -- and the only one with no typed
        table, so the raw payload *is* the record. Roughly 1.9 MB per workout
        uncompressed, which is why it skips whatever is already archived and
        re-fetches nothing.

        Yields (trackid, newly_archived) so a pass can report what it skipped.
        """
        client = self._client
        if client is None or not self.is_connected():
            return

        try:
            params = {"limit": 100}
            response = await client.get("/v1/sport/run/history.json", params=params)
            if response.status_code != 200:
                raise AdapterFetchError(
                    f"Failed to fetch workout history: HTTP {response.status_code}"
                )
            history = response.json()
            self._archive(
                "sport.run.history",
                params,
                history,
                window_start=start_date,
                window_end=end_date,
                http_status=response.status_code,
            )

            for item in history.get("data", {}).get("summary", []):
                trackid = str(item.get("trackid") or "")
                if not trackid:
                    continue

                workout_date = self._workout_local_date(item)
                if workout_date:
                    if start_date and workout_date < start_date:
                        continue
                    if end_date and workout_date > end_date:
                        continue

                if trackid in skip_ids:
                    yield trackid, False
                    continue

                detail_params = {
                    "trackid": trackid,
                    "source": item.get("source") or "run.mifit.huami.com",
                }
                detail = await client.get("/v1/sport/run/detail.json", params=detail_params)
                if detail.status_code != 200:
                    # One unavailable track must not abandon the rest of the backfill.
                    logger.warning(
                        "run/detail.json for trackid %s returned HTTP %s; skipping",
                        trackid,
                        detail.status_code,
                    )
                    continue

                self._archive(
                    "sport.run.detail",
                    detail_params,
                    detail.json(),
                    window_start=workout_date,
                    window_end=workout_date,
                    http_status=detail.status_code,
                    record_id=trackid,
                )
                yield trackid, True

        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching workout details: {exc}")
            raise AdapterFetchError("Failed to fetch workout details") from exc

    def _utc_from_timestamp(self, value: int | float) -> datetime:
        return datetime.fromtimestamp(value, UTC)

    def _local_date(self, value: datetime) -> str:
        return value.astimezone(self._timezone).date().isoformat()

    def _local_midnight_utc(self, value: str) -> datetime:
        local_midnight = datetime.combine(date.fromisoformat(value), time(), self._timezone)
        return local_midnight.astimezone(UTC)

    async def connect(self) -> bool:
        if not self.app_token:
            logger.error("No app_token provided")
            return False

        logger.info("Using Zepp API host %s", self.api_host)
        self._client = httpx.AsyncClient(
            base_url=self.api_host,
            headers={
                "apptoken": self.app_token,
                "appPlatform": "web",
                "appname": "com.xiaomi.hm.health",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

        try:
            user_info = await self._get_user_info()
            if user_info:
                self.user_id = user_info.get("user_id") or self.user_id
                if not self.user_id:
                    self.user_id = await self._discover_user_id()
                    if self.user_id:
                        save_user_id(self.user_id)
                if not self.user_id:
                    raise RuntimeError("Could not discover Zepp user id")
                self._connected = True
                self._available_types = await self._discover_data_types()
                logger.info("Connected to Zepp API")
                return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")

        if self._client:
            await self._client.aclose()
            self._client = None
        return False

    def is_connected(self) -> bool:
        return self._connected and self._client is not None

    def get_user_id(self) -> str | None:
        return self.user_id

    def get_available_data_types(self) -> list[str]:
        return self._available_types.copy()

    def _parse_band_data(self, data: dict[str, Any]) -> Any:
        """Parse band data from API response."""
        try:
            if "data" in data:
                encoded = data["data"]
                if isinstance(encoded, (list, dict)):
                    return encoded
                decoded = base64.b64decode(encoded)
                return json.loads(decoded)
        except Exception as e:
            logger.error(f"Failed to parse band data: {e}")
            raise ValueError("Invalid band data payload") from e

        return data

    def _decode_band_summary(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if not value:
            return {}
        try:
            return json.loads(base64.b64decode(value))
        except Exception:
            return {}

    def _iter_band_summary_entries(self, payload: Any) -> list[tuple[str, dict[str, Any]]]:
        entries: list[tuple[str, dict[str, Any]]] = []
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                date_str = str(item.get("date_time") or item.get("date") or "")
                summary = self._decode_band_summary(item.get("summary"))
                if date_str and summary:
                    entries.append((date_str, summary))
        elif isinstance(payload, dict):
            for date_str, day_data in payload.items():
                if isinstance(day_data, dict):
                    entries.append((str(date_str), day_data))
        return entries

    def _parse_heart_rate_data(self, encoded_data: str) -> list[tuple[int, int]]:
        try:
            decoded = base64.b64decode(encoded_data)
            hr_values: list[tuple[int, int]] = []
            for i, value in enumerate(decoded):
                if value not in (255, 254, 0) and 30 <= value <= 240:
                    hr_values.append((i, value))
            return hr_values
        except Exception as e:
            logger.error(f"Failed to parse heart rate data: {e}")
            raise ValueError("Invalid heart rate payload") from e

    async def _discover_user_id(self) -> str | None:
        client = self._client
        if client is None:
            return None

        today = datetime.now(self._timezone).date()
        start_date = (today - timedelta(days=30)).isoformat()
        try:
            response = await client.get(
                "/v1/data/band_data.json",
                params={
                    "query_type": "summary",
                    "device_type": "android_phone",
                    "from_date": start_date,
                    "to_date": today.isoformat(),
                },
            )
            if response.status_code != 200:
                return None
            data = response.json().get("data", [])
            if not isinstance(data, list):
                return None
            for item in data:
                if not isinstance(item, dict):
                    continue
                user_id = str(item.get("uid", ""))
                if user_id.isdigit():
                    return user_id
        except Exception as exc:
            logger.error(f"Failed to discover user id: {exc}")
        return None

    async def _get_user_info(self) -> dict[str, Any] | None:
        if not self._client:
            return None

        try:
            response = await self._client.get(
                "/v1/sport/run/history.json",
                params={"limit": 1},
            )
            if response.status_code == 200:
                return {"user_id": self.user_id, "valid": True}
        except Exception as e:
            logger.error(f"Failed to validate token: {e}")

        return None

    async def _discover_data_types(self) -> list[str]:
        """What this connected session can sync, deterministically.

        Previously this probed four endpoints and returned list(set(...)), so the
        order changed between runs and a single failed or momentarily-empty probe
        silently removed a type from the default sync set -- meaning the archive
        could quietly stop covering something it had covered yesterday. Presence
        of data is determined at sync time, where an empty result is visible.
        """
        return list(CLOUD_DATA_TYPES)

    async def iter_daily_activity(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> AsyncIterator[DailyActivity]:
        if not self._client or not self.is_connected():
            return
            yield

        start_date, end_date = self._resolve_range(start_date, end_date, 30)

        try:
            async for date_str, day_data in self._iter_band_summary_days(
                start_date, end_date, "daily activity"
            ):
                steps_data = day_data.get("stp", {})
                if steps_data:
                    yield DailyActivity(
                        id=f"cloud_{self.user_id}_{date_str}",
                        provider="zepp_life",
                        source_type="cloud_session",
                        source_record_id=None,
                        user_id=self.user_id or "unknown",
                        device_id=None,
                        collected_at=None,
                        date=date_str,
                        steps=steps_data.get("ttl", 0),
                        distance_m=steps_data.get("dis", 0),
                        active_kcal=steps_data.get("cal", 0),
                        total_kcal=None,
                        floors=None,
                        active_minutes=None,
                        tz_offset_seconds=self._tz_offset(day_data),
                    )

        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching activity: {exc}")
            raise AdapterFetchError("Failed to fetch daily activity") from exc

    async def iter_sleep_sessions(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> AsyncIterator[SleepSession]:
        if not self._client or not self.is_connected():
            return
            yield

        start_date, end_date = self._resolve_range(start_date, end_date, 30)

        try:
            async for date_str, day_data in self._iter_band_summary_days(
                start_date, end_date, "sleep"
            ):
                sleep_data = day_data.get("slp", {})
                if not sleep_data:
                    continue

                start_ts = sleep_data.get("st")
                end_ts = sleep_data.get("ed")
                metrics = parse_sleep_summary(sleep_data)

                if not start_ts or not end_ts or (end_ts <= start_ts and metrics.duration_minutes <= 0):
                    continue

                start_dt = self._utc_from_timestamp(start_ts)
                end_dt = self._utc_from_timestamp(end_ts)
                offset = self._tz_offset(day_data)
                sleep = SleepSession(
                    id=f"cloud_sleep_{self.user_id}_{date_str}",
                    provider="zepp_life",
                    source_type="cloud_session",
                    source_record_id=None,
                    user_id=self.user_id or "unknown",
                    device_id=None,
                    collected_at=None,
                    sleep_id=f"sleep_{date_str}",
                    timezone=self.timezone,
                    local_date=self._local_date(start_dt),
                    start_at=start_dt,
                    end_at=end_dt,
                    duration_minutes=metrics.duration_minutes,
                    time_asleep_minutes=metrics.time_asleep_minutes,
                    time_awake_minutes=metrics.awake_minutes,
                    sleep_score=metrics.score,
                    stages=metrics.stages,
                    tz_offset_seconds=offset,
                    algo_version=str(sleep_data.get("sleepAlgoVersion") or "") or None,
                    is_nap=self._looks_like_a_nap(
                        start_dt, offset, metrics.time_asleep_minutes
                    ),
                )
                yield sleep.model_copy(
                    update={
                        "rem_minutes": metrics.rem_minutes,
                        "deep_minutes": metrics.deep_minutes,
                        "light_minutes": metrics.light_minutes,
                        "wake_count": metrics.wake_count,
                    }
                )

        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching sleep: {exc}")
            raise AdapterFetchError("Failed to fetch sleep") from exc

    async def iter_heart_rate(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> AsyncIterator[HeartRateSample]:
        if not self._client or not self.is_connected():
            return
            yield

        start_date, end_date = self._resolve_range(start_date, end_date, 7)

        try:
            async for date_str, day_data in self._iter_band_summary_days(
                start_date, end_date, "resting heart rate"
            ):
                sleep_data = day_data.get("slp", {})
                resting_bpm = sleep_data.get("rhr")
                end_ts = sleep_data.get("ed")
                try:
                    resting_bpm = int(resting_bpm)
                    end_ts = int(end_ts)
                except (TypeError, ValueError):
                    continue
                if resting_bpm <= 0 or end_ts <= 0:
                    continue

                timestamp = self._utc_from_timestamp(end_ts)
                sample_date = self._local_date(timestamp)
                # Upstream occasionally files a sleep block under one day while
                # its timestamps point a YEAR elsewhere. iter_sleep_sessions
                # rejects those records, but this path only needs `rhr` and `ed`
                # and so accepted them -- putting resting samples in the archive
                # before the device existed and making get_data_coverage report a
                # first_date a year earlier than every other data type. A sample
                # derived from a window must fall inside that window.
                if sample_date < start_date or sample_date > end_date:
                    logger.warning(
                        "Discarding resting heart rate for %s: timestamp resolves to %s, "
                        "outside the requested %s..%s",
                        date_str,
                        sample_date,
                        start_date,
                        end_date,
                    )
                    continue
                yield HeartRateSample(
                    id=f"cloud_rhr_{self.user_id}_{date_str}",
                    provider="zepp_life",
                    source_type="cloud_session",
                    source_record_id=f"sleep_{date_str}",
                    user_id=self.user_id or "unknown",
                    device_id=None,
                    collected_at=None,
                    timezone=self.timezone,
                    timestamp=timestamp,
                    local_date=sample_date,
                    bpm=resting_bpm,
                    sample_type="resting",
                )
        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching resting heart rate: {exc}")
            raise AdapterFetchError("Failed to fetch resting heart rate") from exc

        try:
            async for item in self._iter_band_detail_rows(start_date, end_date, "heart rate"):
                date_str = item.get("date_time", "")
                hr_data = item.get("data_hr", "")

                if hr_data:
                    hr_values = self._parse_heart_rate_data(hr_data)
                    base_time = self._local_midnight_utc(date_str)

                    for minute, bpm in hr_values:
                        timestamp = base_time + timedelta(minutes=minute)
                        yield HeartRateSample(
                            id=f"cloud_hr_{self.user_id}_{date_str}_{minute}",
                            provider="zepp_life",
                            source_type="cloud_session",
                            source_record_id=None,
                            user_id=self.user_id or "unknown",
                            device_id=None,
                            collected_at=None,
                            timezone=self.timezone,
                            timestamp=timestamp,
                            local_date=date_str,
                            bpm=bpm,
                            sample_type="passive",
                        )

        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching heart rate: {exc}")
            raise AdapterFetchError("Failed to fetch heart rate") from exc

    async def iter_workouts(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> AsyncIterator[Workout]:
        if not self._client or not self.is_connected():
            return
            yield

        try:
            params = {"limit": 100}
            response = await self._client.get("/v1/sport/run/history.json", params=params)

            if response.status_code != 200:
                raise AdapterFetchError(f"Failed to fetch workouts: HTTP {response.status_code}")

            data = response.json()
            self._archive(
                "sport.run.history",
                params,
                data,
                window_start=start_date,
                window_end=end_date,
                http_status=response.status_code,
            )

            for item in data.get("data", {}).get("summary", []):
                start_time = item.get("start_time")
                end_time = item.get("end_time")
                run_time = item.get("run_time", 0)
                end_ts = int(float(end_time)) if end_time else None
                duration_sec = int(float(run_time)) if run_time else 0
                start_ts = (
                    int(float(start_time))
                    if start_time
                    else (end_ts - duration_sec if end_ts else None)
                )
                start_at = self._utc_from_timestamp(start_ts) if start_ts else datetime.now(UTC)
                workout_date = self._local_date(start_at)

                if start_ts:
                    if start_date and workout_date < start_date:
                        continue
                    if end_date and workout_date > end_date:
                        continue

                duration_min = int(float(run_time)) // 60 if run_time else 0
                raw_type = str(item.get("type", "unknown"))

                # The workout knows its own zone, unlike the daily summary which
                # only gives a numeric offset. Prefer it, and derive the offset
                # from it so all three tables answer the same question.
                zone_name = str(item.get("syncedTimezone") or "").strip()
                workout_timezone = self.timezone
                offset_seconds: int | None = None
                if zone_name:
                    try:
                        zone = ZoneInfo(zone_name)
                        workout_timezone = zone_name
                        offset = start_at.astimezone(zone).utcoffset()
                        offset_seconds = int(offset.total_seconds()) if offset else None
                    except Exception:
                        logger.debug("Unrecognised workout timezone %r", zone_name)

                yield Workout(
                    id=f"cloud_{self.user_id}_{item.get('trackid')}",
                    provider="zepp_life",
                    source_type="cloud_session",
                    source_record_id=None,
                    user_id=self.user_id or "unknown",
                    device_id=None,
                    collected_at=None,
                    workout_id=str(item.get("trackid")),
                    timezone=workout_timezone,
                    local_date=workout_date,
                    activity_type=SPORT_TYPE_MAP.get(raw_type, f"sport_{raw_type}"),
                    start_at=start_at,
                    end_at=self._utc_from_timestamp(end_ts) if end_ts else datetime.now(UTC),
                    duration_minutes=duration_min,
                    distance_m=float(item.get("dis", 0)) if item.get("dis") else None,
                    calories_kcal=float(item.get("calorie", 0)) if item.get("calorie") else None,
                    avg_heart_rate_bpm=int(float(item.get("avg_heart_rate")))
                    if item.get("avg_heart_rate")
                    else None,
                    max_heart_rate_bpm=int(float(item.get("max_heart_rate")))
                    if item.get("max_heart_rate")
                    else None,
                    # Upstream reports pace in seconds per METRE; the column is
                    # seconds per kilometre. Verified against distance/duration on
                    # real runs: avg_pace 0.3831 -> 383 s/km -> 6:23/km.
                    avg_pace_sec_per_km=_scaled(item.get("avg_pace"), 1000),
                    max_pace_sec_per_km=_scaled(item.get("max_pace"), 1000),
                    total_steps=_positive_int(item.get("total_step")),
                    tz_offset_seconds=offset_seconds,
                    vo2max=_scaled(item.get("VO2_max"), 1),
                    training_effect=_scaled(item.get("te"), 1),
                    city=str(item.get("city") or "").strip() or None,
                    geohash=str(item.get("location") or "").strip() or None,
                )

        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching workouts: {exc}")
            raise AdapterFetchError("Failed to fetch workouts") from exc

    async def iter_body_measurements(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> AsyncIterator[BodyMeasurement]:
        if not self._client or not self.is_connected():
            return
            yield

        try:
            url = f"{self.ZEPP_WEIGHT_API}/users/{self.user_id}/members/-1/weightRecords"
            params = {"limit": 200}

            response = await self._client.get(url, params=params)

            if response.status_code != 200:
                raise AdapterFetchError(
                    f"Failed to fetch body measurements: HTTP {response.status_code}"
                )

            data = response.json()
            self._archive(
                "weight.records",
                params,
                data,
                window_start=start_date,
                window_end=end_date,
                http_status=response.status_code,
            )

            for item in data.get("items", []):
                record_time = item.get("generatedTime")
                timestamp = (
                    self._utc_from_timestamp(record_time) if record_time else datetime.now(UTC)
                )
                record_date = self._local_date(timestamp)
                if record_time:
                    if start_date and record_date < start_date:
                        continue
                    if end_date and record_date > end_date:
                        continue

                summary = item.get("summary", {})

                yield BodyMeasurement(
                    id=f"cloud_weight_{self.user_id}_{item.get('id', record_time)}",
                    provider="zepp_life",
                    source_type="cloud_session",
                    source_record_id=None,
                    user_id=self.user_id or "unknown",
                    device_id=None,
                    collected_at=None,
                    timezone=self.timezone,
                    local_date=record_date,
                    timestamp=timestamp,
                    weight_kg=summary.get("weight", 0),
                    bmi=summary.get("bmi"),
                    body_fat_pct=summary.get("fatRate"),
                    muscle_mass_kg=summary.get("muscleRate"),
                    water_pct=summary.get("bodyWaterRate"),
                    bone_mass_kg=summary.get("boneMass"),
                    visceral_fat_score=int(summary.get("visceralFat", 0))
                    if summary.get("visceralFat")
                    else None,
                    basal_metabolism_kcal=int(summary.get("metabolism", 0))
                    if summary.get("metabolism")
                    else None,
                    metabolic_age=int(summary.get("muscleAge", 0))
                    if summary.get("muscleAge")
                    else None,
                )

        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching weight: {exc}")
            raise AdapterFetchError("Failed to fetch body measurements") from exc

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None
            self._connected = False
