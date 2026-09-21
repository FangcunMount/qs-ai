"""One validated configuration for HTTP, workers, gRPC and migrations."""

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict
from sqlalchemy.engine import make_url

from qs_ai.model_configuration import ModelConfiguration


class Options(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HTTPOptions(Options):
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)


class LoggingOptions(Options):
    level: Literal["critical", "error", "warning", "info", "debug"] = "info"
    capacity: int = Field(default=1024, ge=2, le=65536)
    reserved: int = Field(default=128, ge=0)
    max_bytes: int = Field(default=16384, ge=1024, le=16384)
    flush_seconds: float = Field(default=2, ge=0, le=2)
    repeat_seconds: float = Field(default=30, ge=0, le=300)

    @model_validator(mode="after")
    def validate_reserve(self) -> "LoggingOptions":
        if self.reserved >= self.capacity:
            raise ValueError("Logging reserve must be smaller than capacity")
        return self


class DatabaseOptions(Options):
    pool_size: int = Field(ge=1, le=100)
    max_overflow: int = Field(ge=0, le=100)
    pool_timeout: float = Field(gt=0)
    connect_timeout: int = Field(ge=1)


class LoopOptions(Options):
    concurrency: int = Field(ge=1, le=32)
    idle_seconds: float = Field(gt=0)
    max_backoff_seconds: float = Field(gt=0)
    shutdown_seconds: float = Field(ge=0)


class EvaluationOptions(LoopOptions):
    candidate_mode_enabled: bool = False
    parallel_calls: int = Field(default=1, ge=1, le=32)
    per_run_parallel_calls: int = Field(default=1, ge=1, le=32)
    enabled: bool
    daily_provider_calls: int = Field(ge=1)
    max_active_runs: int = Field(ge=1)


class ProviderCapacityOptions(Options):
    total: int = Field(default=2, ge=1, le=32)
    generation_reserved: int = Field(default=1, ge=0, le=31)

    @model_validator(mode="after")
    def reserve(self) -> "ProviderCapacityOptions":
        if self.generation_reserved >= self.total:
            raise ValueError("Generation reserve must be smaller than total")
        return self


class ModelCapacityOptions(Options):
    deepseek: ProviderCapacityOptions = Field(default_factory=ProviderCapacityOptions)
    zhipu: ProviderCapacityOptions = Field(default_factory=ProviderCapacityOptions)


class WorkerOptions(LoopOptions):
    lease_seconds: int = Field(ge=3)


class ParticipantCapacityOptions(Options):
    daily_org: int = Field(ge=1)
    daily_user: int = Field(ge=1)
    daily_assessment: int = Field(ge=1)
    active_org: int = Field(ge=1)
    active_user: int = Field(ge=1)
    active_assessment: int = Field(ge=1)


class EvaluationQuotaOptions(Options):
    daily_provider_calls: int = Field(ge=1)
    max_active_runs: int = Field(ge=1)


class QuotaCeilings(Options):
    participant: ParticipantCapacityOptions
    evaluation: EvaluationQuotaOptions


class GenerationOptions(Options):
    enabled: bool
    endpoint: str | None


class GRPCOptions(Options):
    governance_enabled: bool = False
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
    if any(
        key in data
        for key in (
            "database_url",
            "environment",
            "model_api_key",
            "deepseek_api_key",
            "zhipu_api_key",
        )
    ):
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


class DiagnosticsOptions(Options):
    retention_days: int = Field(default=30, ge=1, le=365)


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
    deepseek_api_key: SecretStr | None = None
    zhipu_api_key: SecretStr | None = None
    models: ModelConfiguration = Field(default_factory=ModelConfiguration)
    # Only models verified for the deployed adapter may be offered for editing.
    governance_models: tuple[str, ...] = ("deepseek-v4-pro",)
    participant_capacity: ParticipantCapacityOptions
    quota_ceilings: QuotaCeilings | None = None
    generation: GenerationOptions
    logging: LoggingOptions = Field(default_factory=LoggingOptions)
    http: HTTPOptions
    database: DatabaseOptions
    diagnostics: DiagnosticsOptions = Field(default_factory=DiagnosticsOptions)
    worker: WorkerOptions
    evaluation: EvaluationOptions
    model_capacity: ModelCapacityOptions = Field(default_factory=ModelCapacityOptions)
    grpc: GRPCOptions
    delivery: DeliveryOptions

    @model_validator(mode="after")
    def validate_model_credentials(self) -> "Settings":
        old, new = self.model_api_key, self.deepseek_api_key
        if old is not None and new is not None and old.get_secret_value() != new.get_secret_value():
            raise ValueError("DeepSeek credential aliases disagree")
        return self

    @property
    def effective_deepseek_api_key(self) -> SecretStr | None:
        # Temporary read compatibility; deployment emits only the provider-specific name.
        return self.deepseek_api_key if self.deepseek_api_key is not None else self.model_api_key

    @model_validator(mode="after")
    def validate_quota_baseline(self) -> "Settings":
        if self.quota_ceilings is not None:
            defaults = {
                "participant": self.participant_capacity.model_dump(),
                "evaluation": {
                    "daily_provider_calls": self.evaluation.daily_provider_calls,
                    "max_active_runs": self.evaluation.max_active_runs,
                },
            }
            ceilings = self.quota_ceilings.model_dump()
            if any(
                value > ceilings[section][key]
                for section, fields in defaults.items()
                for key, value in fields.items()
            ):
                raise ValueError("Deployment quota defaults exceed ceilings")
        return self

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
