import shutil
from pathlib import Path

import pytest
from pydantic import ValidationError

from qs_ai import config
from qs_ai.config import Settings


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    import os

    for name in os.environ:
        if name.startswith("QS_AI_"):
            monkeypatch.delenv(name)


def test_precedence_and_partial_nested_overrides(monkeypatch):
    monkeypatch.setenv("QS_AI_ENVIRONMENT", "production")
    monkeypatch.setenv("QS_AI_HTTP__PORT", "9000")
    settings = Settings(logging={"level": "debug"})
    assert settings.environment == "production"
    assert settings.http.host == "0.0.0.0"
    assert settings.http.port == 9000
    assert settings.logging.level == "debug"
    assert Settings(environment="local").http.host == "127.0.0.1"
    assert Settings(http={"port": 7000}).http.port == 7000
    assert settings.database.pool_size == 5


def test_secrets_are_environment_only_and_redacted(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("QS_AI_DATABASE_URL=mysql+asyncmy://user:local-secret@host/db")
    assert Settings().database_url is None
    secret = "mysql+asyncmy://user:env-secret@host/db"
    monkeypatch.setenv("QS_AI_DATABASE_URL", secret)
    settings = Settings()
    assert settings.database_url.get_secret_value() == secret
    assert "env-secret" not in repr(settings)
    assert "env-secret" not in settings.model_dump_json()
    with pytest.raises(ValidationError) as error:
        Settings(database_url="postgresql://user:hidden-secret@host/db")
    assert "hidden-secret" not in str(error.value)


@pytest.mark.parametrize(
    "name,value",
    [
        ("QS_AI_WORKER__LEASE_SECONDS", "2"),
        ("QS_AI_DELIVERY__BATCH_SIZE", "101"),
        ("QS_AI_DATABASE__POOL_SIZE", "0"),
        ("QS_AI_HTTP__PORT", "65536"),
        ("QS_AI_HTTP__TYPO", "123"),
    ],
)
def test_invalid_runtime_options_fail_fast(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("environment", ["staging", "../production"])
def test_unknown_environment_is_rejected(environment):
    with pytest.raises(ValueError, match="must be local or production"):
        Settings(environment=environment)


def test_yaml_missing_invalid_and_secret_keys_fail_closed(monkeypatch, tmp_path):
    shutil.copytree(config.config_directory(), tmp_path / "configs")
    root = tmp_path / "configs"
    monkeypatch.setattr(config, "config_directory", lambda: root)
    path = root / "local.yaml"
    for text in ["http: [", "- not-a-mapping", "database_url: secret", "http:\n  typo: 1"]:
        path.write_text(text)
        with pytest.raises(ValueError):
            Settings()
    path.unlink()
    with pytest.raises(ValueError, match="Cannot load"):
        Settings()


def test_settings_do_not_depend_on_working_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    assert Settings().worker.lease_seconds == 30
    assert Path(config.config_directory() / "default.yaml").is_file()


def test_deployment_secret_encoding_and_rejection():
    import runpy

    script = Path(__file__).resolve().parents[1] / "deploy/serverA/deploy.py"
    encode = runpy.run_path(str(script))["secret_override"]
    value = "mysql+asyncmy://user:pa$word@host/qs_ai"
    override = encode(value)
    assert override["services"]["qs-ai"]["environment"]["QS_AI_DATABASE_URL"] == value.replace(
        "$", "$$"
    )
    with pytest.raises(ValueError):
        encode(value + "\nOTHER=value")


def test_logging_limits_and_old_http_option_rejected():
    with pytest.raises(ValidationError):
        Settings(logging={"capacity": 10, "reserved": 10})
    with pytest.raises(ValidationError):
        Settings(http={"log_level": "info"})
    assert Settings().logging.flush_seconds == 2
