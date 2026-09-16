"""Export file adapter for reading Zepp Life exported data."""

import csv
import hashlib
import json
import logging
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zepp_life_mcp.adapters.base import DataAdapter
from zepp_life_mcp.models import (
    BodyMeasurement,
    DailyActivity,
    HeartRateSample,
    SleepSession,
    Workout,
)

logger = logging.getLogger(__name__)


class ExportFileAdapter(DataAdapter):
    """Adapter for reading exported Zepp Life data files."""

    def __init__(self, export_path: Path):
        """Initialize adapter.

        Args:
            export_path: Path to exported data directory
        """
        self.export_path = Path(export_path)
        self._connected = False
        self._user_id: str | None = None
        self._available_types: list[str] = []

    def _stable_id(self, record_type: str, *parts: object) -> str:
        identity = ":".join(str(part) for part in (self._user_id or "unknown", *parts))
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"zepp-life:{record_type}:{identity}"))

    def connect(self) -> bool:
        """Validate export directory structure."""
        if not self.export_path.exists():
            return False

        # Check for common export file patterns
        self._available_types = []

        # Look for activity data
        if self._find_activity_files():
            self._available_types.append("daily_activity")

        # Look for sleep data
        if self._find_sleep_files():
            self._available_types.append("sleep")

        # Look for workout data
        if self._find_workout_files():
            self._available_types.append("workouts")

        # Look for body/weight data
        if self._find_body_files():
            self._available_types.append("body_measurements")

        self._connected = len(self._available_types) > 0

        # Try to extract user ID from any available file
        if self._connected:
            self._user_id = self._extract_user_id()

        return self._connected

    def is_connected(self) -> bool:
        """Check if adapter is connected."""
        return self._connected

    def get_user_id(self) -> str | None:
        """Get user ID from export files."""
        return self._user_id

    def get_available_data_types(self) -> list[str]:
        """Get available data types."""
        return self._available_types.copy()

    def _find_activity_files(self) -> list[Path]:
        """Find activity/steps data files."""
        patterns = [
            "ACTIVITY/*.csv",
            "activity/*.csv",
            "steps/*.csv",
            "STEPS/*.csv",
            "*activity*.csv",
            "*steps*.csv",
            "*.csv",  # Check all CSVs for activity data
        ]
        return self._find_files_by_patterns(patterns)

    def _find_sleep_files(self) -> list[Path]:
        """Find sleep data files."""
        patterns = [
            "SLEEP/*.csv",
            "sleep/*.csv",
            "*sleep*.csv",
        ]
        return self._find_files_by_patterns(patterns)

    def _find_workout_files(self) -> list[Path]:
        """Find workout data files."""
        patterns = [
            "WORKOUTS/*.csv",
            "workouts/*.csv",
            "SPORT/*.csv",
            "sport/*.csv",
            "*workout*.csv",
            "*sport*.csv",
        ]
        return self._find_files_by_patterns(patterns)

    def _find_body_files(self) -> list[Path]:
        """Find body/weight data files."""
        patterns = [
            "BODY/*.csv",
            "body/*.csv",
            "WEIGHT/*.csv",
            "weight/*.csv",
            "*weight*.csv",
            "*body*.csv",
        ]
        return self._find_files_by_patterns(patterns)

    def _find_files_by_patterns(self, patterns: list[str]) -> list[Path]:
        """Find files matching patterns."""
        found = []
        for pattern in patterns:
            found.extend(self.export_path.glob(pattern))
        return list(set(found))  # Remove duplicates

    def _extract_user_id(self) -> str | None:
        """Try to extract user ID from export files."""
        # Look for user info files
        user_files = list(self.export_path.glob("*user*.json"))
        user_files.extend(self.export_path.glob("*profile*.json"))

        for file_path in user_files:
            try:
                with open(file_path, encoding="utf-8") as f:
                    data = json.load(f)
                    if "user_id" in data:
                        return str(data["user_id"])
                    if "id" in data:
                        return str(data["id"])
            except (OSError, json.JSONDecodeError, KeyError):
                continue

        canonical_path = str(self.export_path.resolve())
        path_hash = hashlib.sha256(canonical_path.encode()).hexdigest()
        return f"export_{path_hash}"

    def iter_daily_activity(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[DailyActivity]:
        """Iterate over daily activity records from export files."""
        files = self._find_activity_files()

        for file_path in files:
            try:
                yield from self._parse_activity_file(file_path, start_date, end_date)
            except Exception as e:
                logger.warning("Failed to parse %s: %s", file_path, e)
                continue

    def _parse_activity_file(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[DailyActivity]:
        """Parse a single activity file."""
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            # Try to detect dialect
            sample = f.read(8192)
            f.seek(0)

            # Check if it's JSON
            if sample.strip().startswith("[") or sample.strip().startswith("{"):
                yield from self._parse_activity_json(file_path, start_date, end_date)
                return

            # Try CSV
            try:
                reader = csv.DictReader(f)
                for row in reader:
                    activity = self._row_to_activity(row)
                    if activity and self._date_in_range(activity.date, start_date, end_date):
                        yield activity
            except csv.Error:
                pass

    def _parse_activity_json(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[DailyActivity]:
        """Parse activity data from JSON file."""
        with open(file_path, encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        activity = self._dict_to_activity(item)
                        if activity and self._date_in_range(activity.date, start_date, end_date):
                            yield activity
                elif isinstance(data, dict):
                    # Single record or nested structure
                    if "data" in data and isinstance(data["data"], list):
                        for item in data["data"]:
                            activity = self._dict_to_activity(item)
                            if activity and self._date_in_range(
                                activity.date, start_date, end_date
                            ):
                                yield activity
                    else:
                        activity = self._dict_to_activity(data)
                        if activity and self._date_in_range(activity.date, start_date, end_date):
                            yield activity
            except json.JSONDecodeError:
                pass

    def _row_to_activity(self, row: dict[str, str]) -> DailyActivity | None:
        """Convert CSV row to DailyActivity."""
        # Try to identify date column
        date = None
        for key in ["date", "Date", "DATE", "day", "timestamp", "time"]:
            if key in row and row[key]:
                date = self._normalize_date(row[key])
                break

        if not date:
            return None

        # Try to identify steps column
        steps = 0
        for key in ["steps", "Steps", "STEPS", "step", "step_count", "steps_count"]:
            if key in row and row[key]:
                try:
                    steps = int(float(row[key]))
                    break
                except ValueError:
                    continue

        # Try to identify distance column
        distance_m = 0.0
        for key in ["distance", "Distance", "distance_m", "distance_meters", "dist"]:
            if key in row and row[key]:
                try:
                    val = float(row[key])
                    # Convert km to m if value is small
                    if val < 100:
                        val *= 1000
                    distance_m = val
                    break
                except ValueError:
                    continue

        # Try to identify calories column
        active_kcal = 0.0
        for key in ["calories", "Calories", "cal", "kcal", "active_calories", "energy"]:
            if key in row and row[key]:
                try:
                    active_kcal = float(row[key])
                    break
                except ValueError:
                    continue

        return DailyActivity(
            id=self._stable_id("activity", date),
            provider="zepp_life",
            source_type="export_file",
            source_record_id=None,
            user_id=self._user_id or "unknown",
            device_id=None,
            collected_at=None,
            date=date,
            steps=steps,
            distance_m=distance_m,
            active_kcal=active_kcal,
            total_kcal=None,
            floors=None,
            active_minutes=None,
        )

    def _dict_to_activity(self, data: dict[str, Any]) -> DailyActivity | None:
        """Convert dict to DailyActivity."""
        # Handle various field naming conventions
        date = None
        for key in ["date", "Date", "day", "timestamp", "time", "dateTime"]:
            if key in data and data[key]:
                date = self._normalize_date(str(data[key]))
                break

        if not date:
            return None

        steps = 0
        for key in ["steps", "step", "stepCount", "step_count", "total_steps"]:
            if key in data:
                try:
                    steps = int(float(data[key]))
                    break
                except (ValueError, TypeError):
                    continue

        distance_m = 0.0
        for key in ["distance", "distanceMeters", "dist", "total_distance"]:
            if key in data:
                try:
                    val = float(data[key])
                    if val < 100:
                        val *= 1000
                    distance_m = val
                    break
                except (ValueError, TypeError):
                    continue

        active_kcal = 0.0
        for key in ["calories", "cal", "kcal", "activeCalories", "energy", "total_calories"]:
            if key in data:
                try:
                    active_kcal = float(data[key])
                    break
                except (ValueError, TypeError):
                    continue

        return DailyActivity(
            id=self._stable_id("activity", date),
            provider="zepp_life",
            source_type="export_file",
            source_record_id=None,
            user_id=self._user_id or "unknown",
            device_id=None,
            collected_at=None,
            date=date,
            steps=steps,
            distance_m=distance_m,
            active_kcal=active_kcal,
            total_kcal=None,
            floors=None,
            active_minutes=None,
        )

    def iter_sleep_sessions(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[SleepSession]:
        """Iterate over sleep session records."""
        files = self._find_sleep_files()

        for file_path in files:
            try:
                yield from self._parse_sleep_file(file_path, start_date, end_date)
            except Exception as e:
                logger.warning("Failed to parse %s: %s", file_path, e)
                continue

    def _parse_sleep_file(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[SleepSession]:
        """Parse sleep data file."""
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            sample = f.read(8192)
            f.seek(0)

            if sample.strip().startswith("[") or sample.strip().startswith("{"):
                yield from self._parse_sleep_json(file_path, start_date, end_date)
            else:
                # Try CSV
                try:
                    reader = csv.DictReader(f)
                    for row in reader:
                        sleep = self._row_to_sleep(row)
                        if sleep:
                            sleep_date = sleep.start_at.strftime("%Y-%m-%d")
                            if self._date_in_range(sleep_date, start_date, end_date):
                                yield sleep
                except csv.Error:
                    pass

    def _parse_sleep_json(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[SleepSession]:
        """Parse sleep data from JSON."""
        with open(file_path, encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        sleep = self._dict_to_sleep(item)
                        if sleep:
                            sleep_date = sleep.start_at.strftime("%Y-%m-%d")
                            if self._date_in_range(sleep_date, start_date, end_date):
                                yield sleep
                elif isinstance(data, dict):
                    items = data.get("data", [data]) if "data" in data else [data]
                    for item in items:
                        sleep = self._dict_to_sleep(item)
                        if sleep:
                            sleep_date = sleep.start_at.strftime("%Y-%m-%d")
                            if self._date_in_range(sleep_date, start_date, end_date):
                                yield sleep
            except json.JSONDecodeError:
                pass

    def _row_to_sleep(self, row: dict[str, str]) -> SleepSession | None:
        """Convert CSV row to SleepSession."""
        # This is a simplified implementation
        # Real implementation would need to parse various export formats
        return None

    def _dict_to_sleep(self, data: dict[str, Any]) -> SleepSession | None:
        """Convert dict to SleepSession."""
        try:
            # Try to extract start/end times
            start = None
            end = None

            for key in ["start", "startTime", "start_time", "bedtime"]:
                if key in data and data[key]:
                    start = self._parse_datetime(str(data[key]))
                    break

            for key in ["end", "endTime", "end_time", "wake_time", "wakeup"]:
                if key in data and data[key]:
                    end = self._parse_datetime(str(data[key]))
                    break

            if not start or not end:
                return None

            duration = int((end - start).total_seconds() / 60)

            # Extract sleep score if available
            score = None
            for key in ["score", "sleepScore", "sleep_score", "quality"]:
                if key in data:
                    try:
                        score = int(data[key])
                        break
                    except (ValueError, TypeError):
                        continue

            sleep_id = self._stable_id("sleep", start.isoformat(), end.isoformat())
            return SleepSession(
                id=sleep_id,
                provider="zepp_life",
                source_type="export_file",
                source_record_id=None,
                user_id=self._user_id or "unknown",
                device_id=None,
                collected_at=None,
                sleep_id=sleep_id,
                local_date=start.date().isoformat(),
                start_at=start,
                end_at=end,
                duration_minutes=duration,
                time_asleep_minutes=duration,
                time_awake_minutes=0,
                sleep_score=score,
            )
        except Exception:
            return None

    def iter_workouts(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[Workout]:
        """Iterate over workout records."""
        files = self._find_workout_files()

        for file_path in files:
            try:
                yield from self._parse_workout_file(file_path, start_date, end_date)
            except Exception as e:
                logger.warning("Failed to parse %s: %s", file_path, e)
                continue

    def _parse_workout_file(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[Workout]:
        """Parse workout data file."""
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            sample = f.read(8192)
            f.seek(0)

            if sample.strip().startswith("[") or sample.strip().startswith("{"):
                yield from self._parse_workout_json(file_path, start_date, end_date)
            else:
                try:
                    reader = csv.DictReader(f)
                    for row in reader:
                        workout = self._row_to_workout(row)
                        if workout:
                            workout_date = workout.start_at.strftime("%Y-%m-%d")
                            if self._date_in_range(workout_date, start_date, end_date):
                                yield workout
                except csv.Error:
                    pass

    def _parse_workout_json(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[Workout]:
        """Parse workout data from JSON."""
        with open(file_path, encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        workout = self._dict_to_workout(item)
                        if workout:
                            workout_date = workout.start_at.strftime("%Y-%m-%d")
                            if self._date_in_range(workout_date, start_date, end_date):
                                yield workout
                elif isinstance(data, dict):
                    items = data.get("data", [data]) if "data" in data else [data]
                    for item in items:
                        workout = self._dict_to_workout(item)
                        if workout:
                            workout_date = workout.start_at.strftime("%Y-%m-%d")
                            if self._date_in_range(workout_date, start_date, end_date):
                                yield workout
            except json.JSONDecodeError:
                pass

    def _row_to_workout(self, row: dict[str, str]) -> Workout | None:
        """Convert CSV row to Workout."""
        return None  # Simplified

    def _dict_to_workout(self, data: dict[str, Any]) -> Workout | None:
        """Convert dict to Workout."""
        try:
            # Extract activity type
            activity_type = "unknown"
            for key in ["type", "activityType", "sport", "activity"]:
                if key in data and data[key]:
                    activity_type = str(data[key]).lower()
                    break

            # Extract start/end times
            start = None
            end = None

            for key in ["start", "startTime", "start_time", "begin_time"]:
                if key in data and data[key]:
                    start = self._parse_datetime(str(data[key]))
                    break

            for key in ["end", "endTime", "end_time", "stop_time"]:
                if key in data and data[key]:
                    end = self._parse_datetime(str(data[key]))
                    break

            if not start:
                return None

            if not end:
                # Try to calculate from duration
                for key in ["duration", "duration_minutes", "time", "elapsed"]:
                    if key in data:
                        try:
                            int(float(data[key]))
                            break
                        except (ValueError, TypeError):
                            continue
                end = start  # Placeholder

            duration = int((end - start).total_seconds() / 60) if end else 0

            # Extract distance
            distance_m = None
            for key in ["distance", "distanceMeters", "dist", "total_distance"]:
                if key in data:
                    try:
                        val = float(data[key])
                        if val < 100:
                            val *= 1000
                        distance_m = val
                        break
                    except (ValueError, TypeError):
                        continue

            # Extract calories
            calories = None
            for key in ["calories", "cal", "kcal", "energy", "total_calories"]:
                if key in data:
                    try:
                        calories = float(data[key])
                        break
                    except (ValueError, TypeError):
                        continue

            workout_id = self._stable_id("workout", start.isoformat(), activity_type)
            return Workout(
                id=workout_id,
                provider="zepp_life",
                source_type="export_file",
                source_record_id=None,
                user_id=self._user_id or "unknown",
                device_id=None,
                collected_at=None,
                workout_id=workout_id,
                local_date=start.date().isoformat(),
                activity_type=activity_type,
                start_at=start,
                end_at=end or start,
                duration_minutes=duration,
                distance_m=distance_m,
                calories_kcal=calories,
                avg_heart_rate_bpm=None,
                max_heart_rate_bpm=None,
                avg_pace_sec_per_km=None,
                max_pace_sec_per_km=None,
                total_steps=None,
            )
        except Exception:
            return None

    def iter_body_measurements(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[BodyMeasurement]:
        """Iterate over body measurement records."""
        files = self._find_body_files()

        for file_path in files:
            try:
                yield from self._parse_body_file(file_path, start_date, end_date)
            except Exception as e:
                logger.warning("Failed to parse %s: %s", file_path, e)
                continue

    def _parse_body_file(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[BodyMeasurement]:
        """Parse body measurement file."""
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            sample = f.read(8192)
            f.seek(0)

            if sample.strip().startswith("[") or sample.strip().startswith("{"):
                yield from self._parse_body_json(file_path, start_date, end_date)
            else:
                try:
                    reader = csv.DictReader(f)
                    for row in reader:
                        measurement = self._row_to_body_measurement(row)
                        if measurement:
                            m_date = measurement.timestamp.strftime("%Y-%m-%d")
                            if self._date_in_range(m_date, start_date, end_date):
                                yield measurement
                except csv.Error:
                    pass

    def _parse_body_json(
        self,
        file_path: Path,
        start_date: str | None,
        end_date: str | None,
    ) -> Iterator[BodyMeasurement]:
        """Parse body data from JSON."""
        with open(file_path, encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        measurement = self._dict_to_body_measurement(item)
                        if measurement:
                            m_date = measurement.timestamp.strftime("%Y-%m-%d")
                            if self._date_in_range(m_date, start_date, end_date):
                                yield measurement
                elif isinstance(data, dict):
                    items = data.get("data", [data]) if "data" in data else [data]
                    for item in items:
                        measurement = self._dict_to_body_measurement(item)
                        if measurement:
                            m_date = measurement.timestamp.strftime("%Y-%m-%d")
                            if self._date_in_range(m_date, start_date, end_date):
                                yield measurement
            except json.JSONDecodeError:
                pass

    def _row_to_body_measurement(self, row: dict[str, str]) -> BodyMeasurement | None:
        """Convert CSV row to BodyMeasurement."""
        return None  # Simplified

    def _dict_to_body_measurement(self, data: dict[str, Any]) -> BodyMeasurement | None:
        """Convert dict to BodyMeasurement."""
        try:
            # Extract timestamp
            timestamp = None
            for key in ["timestamp", "time", "date", "measured_at", "created_at"]:
                if key in data and data[key]:
                    timestamp = self._parse_datetime(str(data[key]))
                    break

            if not timestamp:
                return None

            # Extract weight
            weight = None
            for key in ["weight", "weightKg", "weight_kg", "value"]:
                if key in data:
                    try:
                        weight = float(data[key])
                        break
                    except (ValueError, TypeError):
                        continue

            if not weight:
                return None

            # Extract other metrics
            bmi = None
            for key in ["bmi", "BMI", "bodyMassIndex"]:
                if key in data:
                    try:
                        bmi = float(data[key])
                        break
                    except (ValueError, TypeError):
                        continue

            body_fat = None
            for key in ["bodyFat", "body_fat", "fat", "bodyFatPct", "body_fat_pct"]:
                if key in data:
                    try:
                        body_fat = float(data[key])
                        break
                    except (ValueError, TypeError):
                        continue

            muscle_mass = None
            for key in ["muscleMass", "muscle_mass", "muscle", "skeletalMuscle"]:
                if key in data:
                    try:
                        muscle_mass = float(data[key])
                        break
                    except (ValueError, TypeError):
                        continue

            water = None
            for key in ["water", "bodyWater", "body_water", "waterPct"]:
                if key in data:
                    try:
                        water = float(data[key])
                        break
                    except (ValueError, TypeError):
                        continue

            return BodyMeasurement(
                id=self._stable_id("body", timestamp.isoformat()),
                provider="zepp_life",
                source_type="export_file",
                source_record_id=None,
                user_id=self._user_id or "unknown",
                device_id=None,
                collected_at=None,
                timestamp=timestamp,
                local_date=timestamp.date().isoformat(),
                weight_kg=weight,
                bmi=bmi,
                body_fat_pct=body_fat,
                muscle_mass_kg=muscle_mass,
                water_pct=water,
                bone_mass_kg=None,
                visceral_fat_score=None,
                basal_metabolism_kcal=None,
                metabolic_age=None,
            )
        except Exception:
            return None

    def iter_heart_rate(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[HeartRateSample]:
        """Always empty: Zepp's export archives carry no heart-rate series.

        Not a stub awaiting a parser -- there is nothing in the export to parse.
        Heart rate is available only from the cloud source, so an export-only
        setup has no heart-rate history at all. Said plainly here because the
        README once implied export mode was the more complete of the two.
        """
        return iter(())

    def _normalize_date(self, date_str: str) -> str:
        """Normalize date string to YYYY-MM-DD format."""
        # Try various date formats
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d.%m.%Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # If all else fails, return as-is
        return date_str[:10] if len(date_str) >= 10 else date_str

    def _parse_datetime(self, dt_str: str) -> datetime:
        """Parse datetime string."""
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
        ]

        for fmt in formats:
            try:
                parsed = datetime.strptime(dt_str.strip(), fmt)
                return parsed.replace(tzinfo=UTC) if fmt.endswith("Z") else parsed
            except ValueError:
                continue

        # Try Unix timestamp
        try:
            timestamp = float(dt_str)
            return datetime.fromtimestamp(timestamp, UTC)
        except ValueError:
            pass

        raise ValueError(f"Unable to parse datetime: {dt_str}")

    def _date_in_range(
        self,
        date: str,
        start_date: str | None,
        end_date: str | None,
    ) -> bool:
        """Check if date is within range."""
        if start_date and date < start_date:
            return False
        return not (end_date and date > end_date)
