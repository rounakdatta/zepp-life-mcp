"""Cloud session adapter for direct API access to Zepp Life."""

import base64
import json
import logging
from collections.abc import AsyncIterator
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
    ):
        self.app_token = app_token
        self.user_id = user_id
        self.region = region
        self.timezone = timezone
        self._timezone = ZoneInfo(timezone)
        self._connected = False
        self._client: httpx.AsyncClient | None = None
        self._available_types: list[str] = []

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

        self._client = httpx.AsyncClient(
            base_url=self.ZEPP_API_BASE,
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
        types = []
        client = self._client
        if client is None:
            return types
        today = datetime.now(self._timezone).date()
        start_date = (today - timedelta(days=30)).isoformat()

        try:
            response = await client.get(
                "/v1/data/band_data.json",
                params={
                    "query_type": "summary",
                    "device_type": "android_phone",
                    "userid": self.user_id,
                    "from_date": start_date,
                    "to_date": today.isoformat(),
                },
            )
            if response.status_code == 200:
                data = response.json()
                parsed = self._parse_band_data(data)
                for _, day_data in self._iter_band_summary_entries(parsed):
                    if day_data.get("stp"):
                        types.append("daily_activity")
                    if day_data.get("slp"):
                        types.append("sleep")
                    break
        except Exception as exc:
            logger.warning(f"Capability discovery failed for band summary: {exc}")

        try:
            response = await client.get(
                "/v1/sport/run/history.json",
                params={"limit": 1},
            )
            if response.status_code == 200:
                types.append("workouts")
        except Exception as exc:
            logger.warning(f"Capability discovery failed for workouts: {exc}")

        try:
            response = await client.get(
                "/v1/data/band_data.json",
                params={
                    "query_type": "detail",
                    "device_type": "android_phone",
                    "userid": self.user_id,
                    "from_date": start_date,
                    "to_date": today.isoformat(),
                },
            )
            if response.status_code == 200:
                for item in response.json().get("data", []):
                    if self._parse_heart_rate_data(item.get("data_hr", "")):
                        types.append("heart_rate")
                        break
        except Exception as exc:
            logger.warning(f"Capability discovery failed for heart rate: {exc}")

        try:
            url = f"{self.ZEPP_WEIGHT_API}/users/{self.user_id}/members/-1/weightRecords?limit=1"
            response = await client.get(url)
            if response.status_code == 200:
                types.append("body_measurements")
        except Exception as exc:
            logger.warning(f"Capability discovery failed for body measurements: {exc}")

        return list(set(types))

    async def iter_daily_activity(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> AsyncIterator[DailyActivity]:
        if not self._client or not self.is_connected():
            return
            yield

        if not end_date:
            end_date = datetime.now(self._timezone).date().isoformat()
        if not start_date:
            start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)
            start_date = start_dt.strftime("%Y-%m-%d")

        try:
            response = await self._client.get(
                "/v1/data/band_data.json",
                params={
                    "query_type": "summary",
                    "device_type": "android_phone",
                    "userid": self.user_id,
                    "from_date": start_date,
                    "to_date": end_date,
                },
            )

            if response.status_code != 200:
                raise AdapterFetchError(
                    f"Failed to fetch daily activity: HTTP {response.status_code}"
                )

            data = response.json()
            parsed_data = self._parse_band_data(data)

            for date_str, day_data in self._iter_band_summary_entries(parsed_data):
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

        if not end_date:
            end_date = datetime.now(self._timezone).date().isoformat()
        if not start_date:
            start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)
            start_date = start_dt.strftime("%Y-%m-%d")

        try:
            response = await self._client.get(
                "/v1/data/band_data.json",
                params={
                    "query_type": "summary",
                    "device_type": "android_phone",
                    "userid": self.user_id,
                    "from_date": start_date,
                    "to_date": end_date,
                },
            )

            if response.status_code != 200:
                raise AdapterFetchError(f"Failed to fetch sleep: HTTP {response.status_code}")

            data = response.json()
            parsed_data = self._parse_band_data(data)

            for date_str, day_data in self._iter_band_summary_entries(parsed_data):
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
                )
                yield sleep.model_copy(
                    update={
                        "rem_minutes": metrics.rem_minutes,
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

        if not end_date:
            end_date = datetime.now(self._timezone).date().isoformat()
        if not start_date:
            start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=7)
            start_date = start_dt.strftime("%Y-%m-%d")

        try:
            response = await self._client.get(
                "/v1/data/band_data.json",
                params={
                    "query_type": "summary",
                    "device_type": "android_phone",
                    "userid": self.user_id,
                    "from_date": start_date,
                    "to_date": end_date,
                },
            )
            if response.status_code != 200:
                raise AdapterFetchError(
                    f"Failed to fetch resting heart rate: HTTP {response.status_code}"
                )
            parsed_data = self._parse_band_data(response.json())
            for date_str, day_data in self._iter_band_summary_entries(parsed_data):
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
                    local_date=self._local_date(timestamp),
                    bpm=resting_bpm,
                    sample_type="resting",
                )
        except AdapterFetchError:
            raise
        except Exception as exc:
            logger.error(f"Error fetching resting heart rate: {exc}")
            raise AdapterFetchError("Failed to fetch resting heart rate") from exc

        try:
            response = await self._client.get(
                "/v1/data/band_data.json",
                params={
                    "query_type": "detail",
                    "device_type": "android_phone",
                    "userid": self.user_id,
                    "from_date": start_date,
                    "to_date": end_date,
                },
            )

            if response.status_code != 200:
                raise AdapterFetchError(f"Failed to fetch heart rate: HTTP {response.status_code}")

            data = response.json()

            for item in data.get("data", []):
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
            response = await self._client.get(
                "/v1/sport/run/history.json",
                params={"limit": 100},
            )

            if response.status_code != 200:
                raise AdapterFetchError(f"Failed to fetch workouts: HTTP {response.status_code}")

            data = response.json()

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

                yield Workout(
                    id=f"cloud_{self.user_id}_{item.get('trackid')}",
                    provider="zepp_life",
                    source_type="cloud_session",
                    source_record_id=None,
                    user_id=self.user_id or "unknown",
                    device_id=None,
                    collected_at=None,
                    workout_id=str(item.get("trackid")),
                    timezone=self.timezone,
                    local_date=workout_date,
                    activity_type=SPORT_TYPE_MAP.get(raw_type, raw_type),
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
                    avg_pace_sec_per_km=None,
                    max_pace_sec_per_km=None,
                    total_steps=None,
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
