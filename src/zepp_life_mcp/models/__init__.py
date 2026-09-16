"""Pydantic models for Zepp MCP."""

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class BaseEntity(BaseModel):
    """Base entity with common fields."""

    id: str = Field(description="Internal UUID")
    provider: Literal["zepp_life"] = Field(description="Data provider")
    source_type: Literal["export_file", "cloud_session"] = Field(description="Source of the data")
    source_record_id: str | None = Field(None, description="Provider's native ID")
    user_id: str = Field(description="User identifier")
    device_id: str | None = Field(None, description="Device that recorded the data")
    timezone: str = Field(default="UTC", description="Timezone for the data")
    collected_at: datetime | None = Field(None, description="When data was recorded")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DailyActivity(BaseEntity):
    """Daily activity summary (steps, calories, etc.)."""

    date: str = Field(description="Date in YYYY-MM-DD format")
    steps: int = Field(ge=0, description="Number of steps")
    distance_m: float = Field(ge=0, description="Distance in meters")
    active_kcal: float = Field(ge=0, description="Active calories burned")
    total_kcal: float | None = Field(None, ge=0, description="Total calories")
    floors: int | None = Field(None, ge=0, description="Floors climbed")
    active_minutes: int | None = Field(None, ge=0, description="Active minutes")
    tz_offset_seconds: int | None = Field(
        None, description="Device UTC offset that day, in seconds; the only location signal available"
    )


class SleepStage(BaseModel):
    """Sleep stage information."""

    stage: Literal["deep", "light", "rem", "awake"] = Field(description="Sleep stage")
    minutes: int = Field(ge=0, description="Duration in minutes")


class SleepSession(BaseEntity):
    """Sleep session data."""

    sleep_id: str = Field(description="Unique sleep session ID")
    local_date: str | None = Field(None, description="Local calendar date at sleep start")
    start_at: datetime = Field(description="Sleep start time")
    end_at: datetime = Field(description="Sleep end time")
    duration_minutes: int = Field(ge=0, description="Total duration")
    time_asleep_minutes: int = Field(ge=0, description="Actual sleep time")
    time_awake_minutes: int = Field(ge=0, description="Time awake during sleep")
    rem_minutes: int = Field(default=0, ge=0, description="Time in REM sleep")
    deep_minutes: int = Field(default=0, ge=0, description="Time in deep sleep")
    light_minutes: int = Field(default=0, ge=0, description="Time in light sleep")
    wake_count: int = Field(default=0, ge=0, description="Number of awake stages")
    sleep_score: int | None = Field(None, ge=0, le=100, description="Sleep quality score")
    is_nap: bool = Field(default=False, description="Whether this is a nap")
    stages: list[SleepStage] = Field(default_factory=list, description="Sleep stages breakdown")
    tz_offset_seconds: int | None = Field(
        None, description="Device UTC offset that day, in seconds"
    )
    algo_version: str | None = Field(
        None,
        description=(
            "Device sleep-algorithm version. Stage splits are NOT comparable across "
            "a change in this value."
        ),
    )


class Workout(BaseEntity):
    """Workout/exercise session."""

    workout_id: str = Field(description="Unique workout ID")
    local_date: str | None = Field(None, description="Local calendar date at workout start")
    activity_type: str = Field(description="Type of activity (running, cycling, etc.)")
    start_at: datetime = Field(description="Workout start time")
    end_at: datetime = Field(description="Workout end time")
    duration_minutes: int = Field(ge=0, description="Duration in minutes")
    distance_m: float | None = Field(None, ge=0, description="Distance in meters")
    calories_kcal: float | None = Field(None, ge=0, description="Calories burned")
    avg_heart_rate_bpm: int | None = Field(None, ge=0, description="Average heart rate")
    max_heart_rate_bpm: int | None = Field(None, ge=0, description="Maximum heart rate")
    avg_pace_sec_per_km: float | None = Field(None, ge=0, description="Average pace")
    max_pace_sec_per_km: float | None = Field(None, ge=0, description="Maximum pace")
    total_steps: int | None = Field(None, ge=0, description="Steps during workout")
    tz_offset_seconds: int | None = Field(
        None, description="Device UTC offset that day, in seconds"
    )
    city: str | None = Field(None, description="City the device reported for this workout")
    geohash: str | None = Field(None, description="Geohash the device reported for this workout")


class BodyMeasurement(BaseEntity):
    """Body composition measurement (from smart scale)."""

    timestamp: datetime = Field(description="Measurement time")
    local_date: str | None = Field(None, description="Local calendar date of measurement")
    weight_kg: float = Field(gt=0, description="Weight in kilograms")
    bmi: float | None = Field(None, gt=0, description="Body mass index")
    body_fat_pct: float | None = Field(None, ge=0, le=100, description="Body fat percentage")
    muscle_mass_kg: float | None = Field(None, ge=0, description="Muscle mass")
    water_pct: float | None = Field(None, ge=0, le=100, description="Body water percentage")
    bone_mass_kg: float | None = Field(None, ge=0, description="Bone mass")
    visceral_fat_score: int | None = Field(None, ge=0, description="Visceral fat level")
    basal_metabolism_kcal: int | None = Field(None, ge=0, description="BMR in kcal")
    metabolic_age: int | None = Field(None, ge=0, description="Metabolic age")


class HeartRateSample(BaseEntity):
    """Heart rate measurement."""

    timestamp: datetime = Field(description="Measurement time")
    local_date: str | None = Field(None, description="Local calendar date of sample")
    bpm: int = Field(ge=0, description="Heart rate in beats per minute")
    sample_type: Literal["resting", "active", "passive", "workout"] = Field(
        default="passive", description="Type of measurement"
    )


class SpO2Sample(BaseEntity):
    """Blood oxygen saturation measurement."""

    timestamp: datetime = Field(description="Measurement time")
    spo2_pct: int = Field(ge=0, le=100, description="SpO2 percentage")


class StressSample(BaseEntity):
    """Stress level measurement."""

    timestamp: datetime = Field(description="Measurement time")
    stress_score: int = Field(ge=0, le=100, description="Stress score")
    level: Literal["low", "medium", "high"] = Field(description="Stress level category")


class UserProfile(BaseModel):
    """User profile information."""

    user_id: str = Field(description="User identifier")
    display_name: str | None = Field(None, description="Display name")
    birth_year: int | None = Field(None, description="Birth year")
    sex: Literal["male", "female", "other"] | None = Field(None, description="Sex")
    height_cm: float | None = Field(None, gt=0, description="Height in cm")
    timezone: str = Field(default="UTC", description="User timezone")
    devices: list[dict[str, Any]] = Field(default_factory=list, description="Connected devices")


class DeviceInfo(BaseModel):
    """Connected device information."""

    device_id: str = Field(description="Device identifier")
    device_type: str = Field(description="Type (band, watch, scale)")
    model: str = Field(description="Device model")
    firmware_version: str | None = Field(None, description="Firmware version")
    last_sync_at: datetime | None = Field(None, description="Last sync time")


class DataCoverage(BaseModel):
    """Data coverage information for a metric."""

    data_type: str = Field(description="Type of data")
    first_date: str | None = Field(None, description="First date with data")
    last_date: str | None = Field(None, description="Last date with data")
    days_with_data: int = Field(ge=0, description="Number of days with data")
    gaps_detected: list[tuple[str, str]] = Field(
        default_factory=list, description="Detected date gaps"
    )


class ConnectionStatus(BaseModel):
    """Connection status response."""

    mode: Literal["export_file", "cloud_session", "not_configured"]
    connected: bool
    last_sync_at: datetime | None = None
    available_data_types: list[str] = Field(default_factory=list)
    account_id_masked: str | None = None
    sync_health: Literal["healthy", "stale", "error", "unknown"] = "unknown"
    next_action: str | None = None
    message: str | None = None


class SyncResult(BaseModel):
    """Data synchronization result."""

    sync_id: str = Field(description="Unique sync identifier")
    started_at: datetime = Field(description="Sync start time")
    finished_at: datetime = Field(description="Sync end time")
    records_added: int = Field(ge=0, description="New records added")
    records_updated: int = Field(ge=0, description="Records updated")
    records_skipped: int = Field(ge=0, description="Records skipped (duplicates)")
    data_types_synced: list[str] = Field(default_factory=list)


class QueryResponse(BaseModel):
    """Generic query response envelope."""

    status: Literal["ok", "error"] = "ok"
    source: Literal["export_file", "cloud_session", "cache", "unknown"]
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    timezone: str = "UTC"
    data: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
