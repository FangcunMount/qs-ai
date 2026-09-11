"""One validated configuration for HTTP, workers, gRPC and migrations."""

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict
from sqlalchemy.engine import make_url


class Options(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HTTPOptions(Options):
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    log_level: Literal["critical", "error", "warning", "info", "debug", "trace"]


class DatabaseOptions(Options):
    pool_size: int = Field(ge=1, le=100)
    max_overflow: int = Field(ge=0, le=100)
    pool_timeout: float = Field(gt=0)
    connect_timeout: int = Field(ge=1)


class LoopOptions(Options):
    health_file: str = Field(min_length=1)
    concurrency: int = Field(ge=1, le=32)
    idle_seconds: float = Field(gt=0)
    max_backoff_seconds: float = Field(gt=0)
    shutdown_seconds: float = Field(ge=0)


class WorkerOptions(LoopOptions):
    lease_seconds: int = Field(ge=3)


class GenerationOptions(Options):
    enabled: bool
    profile_id: str = Field(min_length=1)
    profile_version: str = Field(min_length=1)
    endpoint: str | None
    route: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    provider: Literal["deepseek"]
    model: str = Field(min_length=1)
    protocol: Literal["responses"]
    structured_output_mode: Literal["json_schema"]
    timeout_milliseconds: int = Field(ge=1000, le=600000)
    max_output_tokens: int = Field(ge=1, le=100000)
    reasoning_effort: Literal["none", "low", "medium", "high"]


class GRPCOptions(Options):
    bind_address: str = Field(min_length=1)
    result_address: str | None
    access_address: str | None = None
    max_receive_bytes: int = Field(ge=1024, le=16_777_216)
    shutdown_grace_seconds: float = Field(ge=0)
    request_timeout_seconds: float = Field(gt=0)
    ca_file: str | None
    cert_file: str | None
    key_file: str | None


class DeliveryOptions(LoopOptions):
    batch_size: int = Field(ge=1, le=100)
    max_retry_seconds: int = Field(ge=1, le=86400)


def config_directory() -> Path:
    bundled = Path(__file__).resolve().parent / "configs"
    if bundled.is_dir():
        return bundled
    return Path(__file__).resolve().parents[2] / "configs"


def read_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        raise ValueError(f"Cannot load configuration file: {path.name}") from None
    if not isinstance(data, dict) or any(not isinstance(key, str) for key in data):
        raise ValueError(f"Configuration must be a mapping: {path.name}")
    # Credentials belong to environment variables or explicit test parameters only.
    if any(key in data for key in ("database_url", "environment", "model_api_key")):
        raise ValueError(f"Reserved configuration key in: {path.name}")
    return data


def merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        previous = result.get(key)
        result[key] = (
            merge(previous, value)
            if isinstance(previous, dict) and isinstance(value, dict)
            else value
        )
    return result


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="QS_AI_",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
    )

    environment: Literal["local", "production"] = "local"
    database_url: SecretStr | None = None
    model_api_key: SecretStr | None = None
    generation: GenerationOptions
    http: HTTPOptions
    database: DatabaseOptions
    worker: WorkerOptions
    grpc: GRPCOptions
    delivery: DeliveryOptions

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[Any, ...]:
        def files() -> dict[str, Any]:
            environment = init_settings().get(
                "environment", env_settings().get("environment", "local")
            )
            if environment not in ("local", "production"):
                raise ValueError("QS_AI_ENVIRONMENT must be local or production")
            root = config_directory()
            return merge(read_yaml(root / "default.yaml"), read_yaml(root / f"{environment}.yaml"))

        # High to low priority. Deliberately do not load .env files implicitly.
        return init_settings, env_settings, files

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None:
            try:
                driver = make_url(value.get_secret_value()).drivername
            except Exception:
                raise ValueError("Invalid database URL") from None
            if driver != "mysql+asyncmy":
                raise ValueError("Database driver must be mysql+asyncmy")
        return value
