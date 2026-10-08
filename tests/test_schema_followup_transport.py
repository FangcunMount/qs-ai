"""Safe Actions boundaries and production Prompt operations remain deliberately bounded."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_deployment import load


@pytest.fixture
def transport():
    return load("scripts/cd/schema_followup.py")


def environment(operation="status"):
    return {
        "FOLLOWUP_OPERATION": operation,
        "FOLLOWUP_ID": "followup_1",
        "DEPLOY_SHA": "a" * 40,
    }


@pytest.mark.parametrize(
    "key,value",
    [
        ("FOLLOWUP_ID", "../base"),
        ("FOLLOWUP_ID", "a;true"),
        ("FOLLOWUP_ID", "a" * 33),
        ("DEPLOY_SHA", "main"),
        ("FOLLOWUP_BASE_ID", "../base"),
        ("FOLLOWUP_BASE_REVISION", "main"),
        ("FOLLOWUP_RELEASE", "a-1-1;true"),
    ],
)
def test_request_cannot_inject_remote_arguments(transport, key, value):
    with pytest.raises(ValueError):
        transport.request({**environment(), key: value})


@pytest.mark.parametrize(
    "operation", ["apply", "retirement-apply", "rollback", "cleanup", "switch"]
)
def test_production_actions_do_not_expose_destructive_operations(transport, operation):
    with pytest.raises(ValueError, match="operation"):
        transport.request(environment(operation))


def test_v2_context_cannot_reuse_original_id(transport):
    with pytest.raises(ValueError, match="new context"):
        transport.request({**environment(), "FOLLOWUP_ID": "dbrefactor_20261008"})


def test_bootstrap_uses_only_verified_prepared_revision(transport, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    value = {"revision": "a" * 40, "release_id": "a" * 40 + "-123-1", "prepared": True}
    Path("release-receipt.json").write_text(json.dumps(value))
    assert transport.request(environment("bootstrap"))["release"] == value["release_id"]
    value["revision"] = "b" * 40
    Path("release-receipt.json").write_text(json.dumps(value))
    with pytest.raises(ValueError, match="differs"):
        transport.request(environment("bootstrap"))


def test_invalid_receipts_never_export_driver_output(transport):
    selected = {"id": "followup_1", "operation": "status"}
    for value in ("private driver error", {"followup": "ok", "id": "other", "operation": "status"}):
        with pytest.raises(RuntimeError, match="output withheld"):
            transport.safe_receipt(json.dumps(value), selected)
    with pytest.raises(RuntimeError, match="output withheld"):
        transport.safe_receipt(
            json.dumps(
                {
                    **selected,
                    "followup": "ok",
                    "retention": [{"schema": "ai_backup_one", "rows": "private"}],
                }
            ),
            selected,
        )
    receipt = transport.safe_receipt(
        json.dumps(
            {
                **selected,
                "followup": "ok",
                "tables": 44,
                "body": "private",
                "password": "private",
                "retain_until": None,
                "archives": [
                    {
                        "schema": "ai_backup_one",
                        "status": "unknown",
                        "retained_since": None,
                        "cleanup_not_before": None,
                        "body": "private",
                    }
                ],
            }
        ),
        selected,
    )
    assert "body" not in receipt and "password" not in receipt
    assert "body" not in receipt["archives"][0]
    assert receipt["retain_until"] is None


def test_release_cannot_publish_deployed_marker_before_runtime_proof(transport):
    with pytest.raises(RuntimeError, match="output withheld"):
        transport.safe_receipt(
            json.dumps(
                {"followup": "ok", "id": "followup_1", "operation": "release", "phase": "prepared"}
            ),
            {"id": "followup_1", "operation": "release"},
        )


def test_status_keeps_full_rollback_inspectable_without_exposing_rollback_action(transport):
    expected = {"id": "followup_1", "operation": "status"}
    value = {
        **expected,
        "followup": "ok",
        "source_head": "0038_messaging_observations",
        "tables": 54,
        "auto_increment_columns": [
            "evaluation_admission_locks.organization_id",
            "participant_admission_locks.organization_id",
        ],
    }
    assert transport.safe_receipt(json.dumps(value), expected)["tables"] == 54
    with pytest.raises(RuntimeError, match="output withheld"):
        transport.safe_receipt(
            json.dumps({**value, "source_head": "0040_module_table_names"}), expected
        )


def test_private_credentials_are_cleaned_on_host_failure(transport, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    values = {
        **environment(),
        "RUNNER_TEMP": str(tmp_path),
        "GITHUB_RUN_ID": "123",
        "GITHUB_RUN_ATTEMPT": "1",
        "MYSQL_MAINTENANCE_USERNAME": "private_user",
        "MYSQL_MAINTENANCE_PASSWORD": "private @:/#$' 中文",
        "SVRA_HOST": "servera.example",
        "SVRA_USERNAME": "deploy",
        "SVRA_SSH_KEY": "private-key",
        "SVRA_HOST_PUBLIC_KEY": "ssh-ed25519 YWJj",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    calls = []

    def run(phase, arguments):
        calls.append((phase, arguments))
        if phase == "server identity":
            return "serverA\n"
        if phase == "package versioned follow-up":
            Path(arguments[arguments.index("--output") + 1]).write_bytes(b"test source")
        if phase == "upload follow-up credentials":
            path = Path(arguments[-2])
            assert path.stat().st_mode & 0o777 == 0o600
            assert json.loads(path.read_text())["password"] == values["MYSQL_MAINTENANCE_PASSWORD"]
        if phase == "schema follow-up":
            raise RuntimeError("host connection ended")
        return ""

    monkeypatch.setattr(transport, "run", run)
    with pytest.raises(RuntimeError, match="connection"):
        transport.main()
    assert calls[-1][0] == "remove temporary follow-up credentials"
    assert all(values["MYSQL_MAINTENANCE_PASSWORD"] not in str(args) for _, args in calls)
    assert not list(tmp_path.glob("qs-ai-followup-*"))


def test_retries_do_not_overwrite_installed_executor(transport):
    command = transport.install_command(
        "/private/context", "/private/context/code-a", "/private/context/code-1.tar.gz", "f" * 64
    )
    assert "sha256sum -c" in command
    assert "test -d /private/context/code-a" in command
    assert "archive-sha256" in command and "mv " in command
    assert "tar -xzf /private/context/code-1.tar.gz -C /private/context/code-a;" not in command


@pytest.mark.parametrize("command", ["plan", "backup", "verify", "restore"])
def test_prompt_host_uses_fixed_app_image_without_source_mount(command, tmp_path):
    runner = load("deploy/serverA/prompt_retirement_followup.py")
    calls = []
    maintenance = SimpleNamespace(
        directory=tmp_path,
        executor_revision="a" * 40,
        executor_image="sha256:" + "b" * 64,
        bound=lambda: calls.append("bound"),
        assert_idle=lambda: calls.append("idle"),
        tools=SimpleNamespace(
            container=lambda *args, **kwargs: calls.append((args, kwargs)) or {"verified": True}
        ),
    )
    result = runner.run(maintenance, command, SimpleNamespace())
    assert calls[:2] == ["bound", "idle"] and result["verified"]
    arguments, options = calls[-1]
    assert arguments[0] == maintenance.executor_image and options["source"] is False
    assert "apply" not in arguments[1]
    if command in {"backup", "restore"}:
        assert "--target" in arguments[1]


def test_prompt_host_apply_is_never_exposed():
    with pytest.raises(ValueError, match="not exposed"):
        load("deploy/serverA/prompt_retirement_followup.py").run(None, "apply", None)


def test_workflow_binds_ci_secrets_and_continuous_deployment_lock():
    workflow = (
        Path(__file__).resolve().parents[1] / ".github/workflows/schema-followup.yml"
    ).read_text()
    assert "group: qs-ai-serverA" in workflow and "cancel-in-progress: false" in workflow
    assert "--event push" in workflow and "environment: production" in workflow
    assert "MYSQL_MAINTENANCE_USERNAME" in workflow and "MYSQL_MAINTENANCE_PASSWORD" in workflow
    assert "retirement-apply" not in workflow and "schema-followup-receipt.json" in workflow
    assert "inputs.operation == 'release'" in workflow
