"""Configuration management for Zepp Life MCP."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from platformdirs import user_config_dir, user_data_dir, user_log_dir
from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)

DEPRECATED_NO_OP_KEYS = (
    "auto_sync_on_start",
    "default_lookback_days",
    "logs_path",
)
DEPRECATED_NO_OP_SCHEMA: dict[str, Any] = {"deprecated": True, "x-no-op": True}


def _default_database_path() -> Path:
    return Path(user_data_dir("zepp-life-mcp")) / "zepp_life.db"


def _default_logs_path() -> Path:
    return Path(user_log_dir("zepp-life-mcp")) / "zepp_life.log"


class Config(BaseModel):
    mode: Literal["export_file", "cloud_session", "not_configured"] = "not_configured"
    region: str = "eu"
    timezone: str = Field(default="UTC")
    database_path: Path = Field(default_factory=_default_database_path)
    logs_path: Path = Field(
        default_factory=_default_logs_path,
        json_schema_extra=DEPRECATED_NO_OP_SCHEMA,
    )
    export_path: Path | None = None
    auto_sync_on_start: bool = Field(
        default=True,
        json_schema_extra=DEPRECATED_NO_OP_SCHEMA,
    )
    stale_after_minutes: int = 60
    store_raw_payloads: bool = Field(
        default=True,
        description="Archive every upstream response verbatim in the raw_payloads table",
    )
    default_lookback_days: int = Field(
        default=30,
        json_schema_extra=DEPRECATED_NO_OP_SCHEMA,
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"Unknown timezone: {value}") from exc
        return value


def get_config_dir() -> Path:
    config_dir = Path(user_config_dir("zepp-life-mcp"))
    try:
        config_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        # A read-only or absent config dir is normal for a fully env-configured
        # container. Resolving the path must still work; only writing may fail.
        logger.debug("Config directory %s is not writable: %s", config_dir, exc)
    return config_dir


def get_config_path() -> Path:
    return get_config_dir() / "config.json"


# Env overrides so a deployed instance is configured entirely by its pod spec,
# with no writable config directory and nothing to drift between restarts.
ENV_OVERRIDES = {
    "ZEPP_MODE": "mode",
    "ZEPP_REGION": "region",
    "ZEPP_TIMEZONE": "timezone",
    "ZEPP_DATABASE_PATH": "database_path",
    "ZEPP_EXPORT_PATH": "export_path",
}


def _apply_env_overrides(data: dict[str, Any]) -> dict[str, Any]:
    for env_name, field_name in ENV_OVERRIDES.items():
        value = os.environ.get(env_name)
        if not value:
            continue
        if field_name in ("database_path", "export_path"):
            data[field_name] = Path(value)
        else:
            data[field_name] = value

    store_raw = os.environ.get("ZEPP_STORE_RAW_PAYLOADS")
    if store_raw:
        data["store_raw_payloads"] = store_raw.lower() in ("1", "true", "yes")
    return data


def load_config() -> Config:
    config_path = get_config_path()
    if not config_path.exists():
        config = Config(**_apply_env_overrides({}))
        # Only persist when there is somewhere to persist to; a read-only
        # config dir is normal in a container and must not be fatal.
        try:
            save_config(config)
        except OSError as exc:
            logger.warning("Could not write %s: %s", config_path, exc)
        return config

    with open(config_path, encoding="utf-8") as f:
        data = json.load(f)

    deprecated_keys = [key for key in DEPRECATED_NO_OP_KEYS if key in data]
    if deprecated_keys:
        logger.warning(
            "Deprecated config keys are retained but ignored: %s",
            ", ".join(deprecated_keys),
        )

    if "database_path" in data and isinstance(data["database_path"], str):
        data["database_path"] = Path(data["database_path"])
    if "logs_path" in data and isinstance(data["logs_path"], str):
        data["logs_path"] = Path(data["logs_path"])
    if "export_path" in data and isinstance(data["export_path"], str):
        data["export_path"] = Path(data["export_path"]) if data["export_path"] else None

    return Config(**_apply_env_overrides(data))


def save_config(config: Config) -> None:
    config_path = get_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    data = config.model_dump()
    data["database_path"] = str(data["database_path"])
    data["logs_path"] = str(data["logs_path"])
    data["export_path"] = str(data["export_path"]) if data["export_path"] else None

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
