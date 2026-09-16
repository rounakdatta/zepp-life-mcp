"""Query service for retrieving data from database."""

from contextlib import suppress
from datetime import date, timedelta
from typing import Any

from zepp_life_mcp.storage import Database

# Sleep facts reachable through the AGGREGATED series, so a year of nights costs
# a few hundred bytes rather than the few hundred kilobytes a full session dump
# takes. query_sleep still returns whole sessions when the detail is wanted.
SLEEP_METRIC_COLUMNS = {
    "sleep_minutes": "time_asleep_minutes",
    "sleep_deep_minutes": "deep_minutes",
    "sleep_rem_minutes": "rem_minutes",
    "sleep_awake_minutes": "time_awake_minutes",
    "sleep_wake_count": "wake_count",
    "sleep_score": "sleep_score",
}


class QueryService:
    """Service for querying fitness data from local database."""

    def __init__(self, db: Database, user_id: str):
        """Initialize query service.

        Args:
            db: Database instance
            user_id: User identifier
        """
        self.db = db
        self.user_id = user_id

    @staticmethod
    def _validate_date_range(start_date: str, end_date: str) -> None:
        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        except ValueError as exc:
            raise ValueError("start_date and end_date must use YYYY-MM-DD format") from exc
        if start > end:
            raise ValueError("start_date must be on or before end_date")

    def get_daily_summaries(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Get daily activity summaries for date range."""
        self._validate_date_range(start_date, end_date)
        records = self.db.query_daily_activity(self.user_id, start_date, end_date)

        # Group by date and aggregate
        summaries = {}
        for record in records:
            date = record["date"]
            if date not in summaries:
                summaries[date] = {
                    "date": date,
                    "steps": 0,
                    "distance_m": 0,
                    "active_kcal": 0,
                    "total_kcal": 0,
                    "floors": 0,
                    "active_minutes": 0,
                    "tz_offset_seconds": record.get("tz_offset_seconds"),
                }

            summaries[date]["steps"] += record.get("steps", 0)
            summaries[date]["distance_m"] += record.get("distance_m", 0)
            summaries[date]["active_kcal"] += record.get("active_kcal", 0)
            if record.get("total_kcal"):
                summaries[date]["total_kcal"] += record["total_kcal"]
            if record.get("floors"):
                summaries[date]["floors"] += record["floors"]
            if record.get("active_minutes"):
                summaries[date]["active_minutes"] += record["active_minutes"]

        return list(summaries.values())

    def get_metric_series(
        self,
        metric: str,
        start_date: str,
        end_date: str,
        granularity: str = "day",
        aggregation: str = "sum",
    ) -> list[dict[str, Any]]:
        """Get time series for a metric."""
        self._validate_date_range(start_date, end_date)
        valid_metrics = {"steps", "distance_m", "active_kcal", "weight_kg"} | set(
            SLEEP_METRIC_COLUMNS
        )
        valid_granularities = {"day", "week", "month"}
        valid_aggregations = {"sum", "avg", "min", "max", "latest"}
        if metric not in valid_metrics:
            raise ValueError(f"Unknown metric: {metric}")
        if granularity not in valid_granularities:
            raise ValueError(f"Unknown granularity: {granularity}")
        if aggregation not in valid_aggregations:
            raise ValueError(f"Unknown aggregation: {aggregation}")

        if metric == "weight_kg":
            records = self.db.query_body_measurements(self.user_id, start_date, end_date)
            series = [
                {"date": record["local_date"], "value": record["weight_kg"]}
                for record in records
            ]
        elif metric in SLEEP_METRIC_COLUMNS:
            column = SLEEP_METRIC_COLUMNS[metric]
            records = self.db.query_sleep_sessions(self.user_id, start_date, end_date)
            series = [
                {"date": record["local_date"], "value": record[column] or 0}
                for record in records
            ]
        else:
            summaries = self.get_daily_summaries(start_date, end_date)
            series = [
                {"date": summary["date"], "value": summary[metric]}
                for summary in summaries
            ]

        return self._aggregate_series(series, granularity, aggregation)

    @staticmethod
    def _aggregate_series(
        series: list[dict[str, Any]],
        granularity: str,
        aggregation: str,
    ) -> list[dict[str, Any]]:
        groups: dict[str, list[float]] = {}
        for item in series:
            item_date = date.fromisoformat(item["date"])
            if granularity == "week":
                period_date = item_date - timedelta(days=item_date.weekday())
            elif granularity == "month":
                period_date = item_date.replace(day=1)
            else:
                period_date = item_date
            groups.setdefault(period_date.isoformat(), []).append(item["value"])

        result = []
        for period, values in sorted(groups.items()):
            if aggregation == "sum":
                value = sum(values)
            elif aggregation == "avg":
                value = sum(values) / len(values)
            elif aggregation == "min":
                value = min(values)
            elif aggregation == "max":
                value = max(values)
            else:
                value = values[-1]
            result.append({"date": period, "value": value})
        return result

    def get_sleep_sessions(
        self,
        start_date: str,
        end_date: str,
        include_naps: bool = True,
    ) -> list[dict[str, Any]]:
        """Get sleep sessions for date range."""
        self._validate_date_range(start_date, end_date)
        records = self.db.query_sleep_sessions(self.user_id, start_date, end_date)

        sessions = []
        for record in records:
            # Skip naps if not included
            if not include_naps and record.get("is_nap"):
                continue

            session = {
                "sleep_id": record["sleep_id"],
                # local_date and the offset are what make a multi-timezone series
                # readable; both existed in the row and neither was ever returned.
                "local_date": record.get("local_date"),
                "timezone": record.get("timezone"),
                "tz_offset_seconds": record.get("tz_offset_seconds"),
                "algo_version": record.get("algo_version"),
                "start_at": record["start_at"],
                "end_at": record["end_at"],
                "duration_minutes": record["duration_minutes"],
                "time_asleep_minutes": record["time_asleep_minutes"],
                "time_awake_minutes": record["time_awake_minutes"],
                "rem_minutes": record.get("rem_minutes", 0),
                "deep_minutes": record.get("deep_minutes", 0),
                "light_minutes": record.get("light_minutes", 0),
                "wake_count": record.get("wake_count", 0),
                "sleep_score": record.get("sleep_score"),
                "is_nap": record.get("is_nap", False),
            }

            # Parse stages if available
            if record.get("stages"):
                import json

                with suppress(json.JSONDecodeError):
                    session["stages"] = json.loads(record["stages"])

            sessions.append(session)

        return sessions

    def get_workouts(
        self,
        start_date: str,
        end_date: str,
        activity_types: list[str] | None = None,
        min_duration: int | None = None,
        min_distance_km: float | None = None,
    ) -> list[dict[str, Any]]:
        """Get workouts for date range with optional filters."""
        self._validate_date_range(start_date, end_date)
        records = self.db.query_workouts(self.user_id, start_date, end_date)

        workouts = []
        for record in records:
            # Apply filters
            if activity_types and record["activity_type"].lower() not in [
                t.lower() for t in activity_types
            ]:
                continue

            if min_duration and record.get("duration_minutes", 0) < min_duration:
                continue

            if min_distance_km:
                distance_km = (record.get("distance_m") or 0) / 1000
                if distance_km < min_distance_km:
                    continue

            workouts.append(
                {
                    "workout_id": record["workout_id"],
                    "activity_type": record["activity_type"],
                    "start_at": record["start_at"],
                    "end_at": record["end_at"],
                    "duration_minutes": record["duration_minutes"],
                    "distance_m": record.get("distance_m"),
                    "calories_kcal": record.get("calories_kcal"),
                    "avg_heart_rate_bpm": record.get("avg_heart_rate_bpm"),
                    "max_heart_rate_bpm": record.get("max_heart_rate_bpm"),
                    "avg_pace_sec_per_km": record.get("avg_pace_sec_per_km"),
                    "max_pace_sec_per_km": record.get("max_pace_sec_per_km"),
                    "total_steps": record.get("total_steps"),
                    "local_date": record.get("local_date"),
                    "timezone": record.get("timezone"),
                    "tz_offset_seconds": record.get("tz_offset_seconds"),
                    "city": record.get("city"),
                    "geohash": record.get("geohash"),
                }
            )

        return workouts

    def get_body_measurements(
        self,
        start_date: str,
        end_date: str,
        metrics: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Get body measurements for date range."""
        self._validate_date_range(start_date, end_date)
        records = self.db.query_body_measurements(self.user_id, start_date, end_date)

        measurements = []
        for record in records:
            measurement = {
                "timestamp": record["timestamp"],
                "weight_kg": record["weight_kg"],
            }

            # Add optional metrics
            optional_fields = [
                "bmi",
                "body_fat_pct",
                "muscle_mass_kg",
                "water_pct",
                "bone_mass_kg",
                "visceral_fat_score",
                "basal_metabolism_kcal",
                "metabolic_age",
            ]

            for field in optional_fields:
                if record.get(field) is not None:
                    measurement[field] = record[field]

            # Filter metrics if specified
            if metrics:
                filtered = {"timestamp": measurement["timestamp"]}
                for metric in metrics:
                    if metric in measurement:
                        filtered[metric] = measurement[metric]
                measurement = filtered

            measurements.append(measurement)

        return measurements

    def get_heart_rate_samples(
        self,
        start_date: str,
        end_date: str,
        sample_type: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        self._validate_date_range(start_date, end_date)
        records = self.db.query_heart_rate_samples(self.user_id, start_date, end_date)

        samples = []
        for record in records:
            if sample_type and record.get("sample_type") != sample_type:
                continue
            samples.append(
                {
                    "timestamp": record["timestamp"],
                    "bpm": record["bpm"],
                    "sample_type": record.get("sample_type", "passive"),
                }
            )

        if limit is not None:
            return samples[:limit]
        return samples

    def get_raw_payloads(
        self,
        endpoint: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        include_payload: bool = False,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Inventory of the verbatim upstream archive, newest fetch first.

        The typed tables keep a deliberate subset -- Zepp returns 193 fields per
        workout and the schema stores a dozen. Everything else is in this
        archive and was, until now, reachable only by opening the SQLite file.
        Payload bodies are omitted unless asked for, because a single band_data
        window can be several megabytes.
        """
        rows = []
        for record in self.db.read_raw_payloads(
            user_id=self.user_id,
            endpoint=endpoint,
            start_date=start_date,
            end_date=end_date,
        ):
            row = {
                "endpoint": record["endpoint"],
                "window_start": record["window_start"],
                "window_end": record["window_end"],
                "record_id": record.get("record_id"),
                "fetched_at": record["fetched_at"],
                "http_status": record["http_status"],
                "payload_bytes": record["payload_bytes"],
                "payload_sha256": record["payload_sha256"],
            }
            if include_payload:
                row["payload"] = record["payload"]
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    def get_data_coverage(self, data_types: list[str] | None = None) -> list[dict[str, Any]]:
        """Get data coverage information."""
        coverage = self.db.get_data_coverage(self.user_id)

        if data_types:
            coverage = [c for c in coverage if c["data_type"] in data_types]

        return coverage
