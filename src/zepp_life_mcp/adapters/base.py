"""Base adapter interface for data sources."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Coroutine, Iterator
from dataclasses import dataclass
from typing import Any, Literal, Union

from zepp_life_mcp.models import (
    BodyMeasurement,
    DailyActivity,
    HeartRateSample,
    SleepSession,
    SleepStage,
    Workout,
)


@dataclass(frozen=True)
class SleepMetrics:
    stages: list[SleepStage]
    deep_minutes: int
    light_minutes: int
    rem_minutes: int
    awake_minutes: int
    time_asleep_minutes: int
    duration_minutes: int
    wake_count: int
    score: int | None


StageType = Literal["deep", "light", "rem", "awake"]

STAGE_TYPES: dict[int, StageType] = {
    4: "light",
    5: "deep",
    7: "awake",
    8: "rem",
}


def _minutes(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _score_or_none(value: Any) -> int | None:
    score = _int_or_none(value)
    if score is None or not 0 <= score <= 100:
        return None
    return score


def parse_sleep_summary(sleep_data: dict[str, Any]) -> SleepMetrics:
    stages = []
    totals = {"deep": 0, "light": 0, "rem": 0, "awake": 0}
    wake_count = 0

    for raw_stage in sleep_data.get("stage", []):
        if not isinstance(raw_stage, dict):
            continue
        mode = _int_or_none(raw_stage.get("mode"))
        if mode is None:
            continue
        stage_type = STAGE_TYPES.get(mode)
        if stage_type is None:
            continue
        start = _int_or_none(raw_stage.get("start"))
        stop = _int_or_none(raw_stage.get("stop", raw_stage.get("end")))
        if start is None or stop is None or stop < start:
            continue
        duration = stop - start + 1
        stages.append(SleepStage(stage=stage_type, minutes=duration))
        totals[stage_type] += duration
        if stage_type == "awake":
            wake_count += 1

    if not stages:
        # No per-stage breakdown, so fall back to the summary totals. `dt` is REM:
        # on a night that does carry stages, dp + lt + dt + wk reconstructs the
        # session length exactly, which is what identifies it. Omitting it used to
        # report REM as zero and understate the night by however long REM was.
        # Latent rather than active on the archives seen so far -- every
        # stage-less night there is empty -- but silent when it does bite.
        totals["deep"] = _minutes(sleep_data.get("dp"))
        totals["light"] = _minutes(sleep_data.get("lt"))
        totals["rem"] = _minutes(sleep_data.get("dt"))
        totals["awake"] = _minutes(sleep_data.get("wk"))

    asleep = totals["deep"] + totals["light"] + totals["rem"]
    duration = asleep + totals["awake"]
    return SleepMetrics(
        stages=stages,
        deep_minutes=totals["deep"],
        light_minutes=totals["light"],
        rem_minutes=totals["rem"],
        awake_minutes=totals["awake"],
        time_asleep_minutes=asleep,
        duration_minutes=duration,
        wake_count=wake_count,
        score=_score_or_none(sleep_data.get("ss")),
    )


class DataAdapter(ABC):
    """Abstract base class for data source adapters."""

    @abstractmethod
    def connect(self) -> Union[bool, "Coroutine[Any, Any, bool]"]:
        """Connect to data source.

        Returns:
            True if connection successful
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected to data source."""
        pass

    @abstractmethod
    def get_user_id(self) -> str | None:
        """Get user identifier from data source."""
        pass

    @abstractmethod
    def iter_daily_activity(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[DailyActivity] | AsyncIterator[DailyActivity]:
        """Iterate over daily activity records."""
        pass

    @abstractmethod
    def iter_sleep_sessions(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[SleepSession] | AsyncIterator[SleepSession]:
        """Iterate over sleep session records."""
        pass

    @abstractmethod
    def iter_workouts(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[Workout] | AsyncIterator[Workout]:
        """Iterate over workout records."""
        pass

    @abstractmethod
    def iter_body_measurements(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[BodyMeasurement] | AsyncIterator[BodyMeasurement]:
        """Iterate over body measurement records."""
        pass

    @abstractmethod
    def iter_heart_rate(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[HeartRateSample] | AsyncIterator[HeartRateSample]:
        """Iterate over heart rate records."""
        pass

    @abstractmethod
    def get_available_data_types(self) -> list[str]:
        """Get list of available data types."""
        pass
