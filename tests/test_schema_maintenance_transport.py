"""Actions transport checks privileged credentials and verified runtime receipts."""

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_deployment import load


@pytest.fixture
def transport():
    return load("scripts/cd/schema_maintenance.py")


@pytest.mark.parametrize("identifier", ["../ai", "ai;true", "A", "a" * 33, "a\n"])
def test_remote_identifiers_cannot_inject_commands(transport, identifier):
    with pytest.raises(ValueError, match="identifier"):
        transport.request(
            {
                "SCHEMA_OPERATION": "preflight",
                "SCHEMA_MAINTENANCE_ID": identifier,
                "DEPLOY_SHA": "a" * 40,
            }
        )


def test_prepared_release_is_bound_to_tested_main_revision(transport):
    with pytest.raises(ValueError, match="tested workflow revision"):
        transport.request(
            {
                "SCHEMA_OPERATION": "prepare",
                "SCHEMA_MAINTENANCE_ID": "rollout_1",
                "DEPLOY_SHA": "a" * 40,
                "SCHEMA_RELEASE": "b" * 40 + "-1-1",
            }
        )


def test_arbitrary_remote_output_and_wrong_receipts_are_withheld(transport):
    expected = {"id": "rollout_1", "operation": "switch"}
    for output in (
        "database error containing a credential",
        json.dumps({"id": "different", "operation": "switch", "schema_maintenance": "ok"}),
        json.dumps(
            {
                "id": "rollout_1",
                "operation": "switch",
                "schema_maintenance": "ok",
                "phase": "prepared",
            }
        ),
    ):
        with pytest.raises(RuntimeError, match="output withheld"):
            transport.safe_receipt(output, expected)
    receipt = transport.safe_receipt(
        json.dumps(
            {
                **expected,
                "schema_maintenance": "ok",
                "phase": "switched",
                "password": "do not publish",
                "QS_AI_DATABASE_URL": "do not publish",
            }
        ),
        expected,
    )
    assert "password" not in receipt
    assert "QS_AI_DATABASE_URL" not in receipt


def setup_transport(transport, monkeypatch, tmp_path, operation="preflight"):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    environment = {
        "SCHEMA_OPERATION": operation,
        "SCHEMA_MAINTENANCE_ID": "rollout_1",
        "DEPLOY_SHA": "a" * 40,
        "GITHUB_RUN_ID": "123",
        "GITHUB_RUN_ATTEMPT": "1",
        "MYSQL_MAINTENANCE_USERNAME": "maintenance-private-user",
        "MYSQL_MAINTENANCE_PASSWORD": "private @:/#?$'\\中文",
        "SVRA_HOST": "servera.example",
        "SVRA_USERNAME": "deploy",
        "SVRA_SSH_KEY": "test-private-key",
        "SVRA_HOST_PUBLIC_KEY": "ssh-ed25519 YWJj",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    calls, records, credentials = [], [], []

    def run(phase, args):
        calls.append((phase, args))
        if phase == "server identity":
            return "serverA\n"
        if phase == "upload maintenance credentials":
            path = Path(args[-2])
            credentials.append(json.loads(path.read_text()))
            assert path.stat().st_mode & 0o777 == 0o600
        if phase == "schema maintenance":
            return json.dumps(
                {
                    "id": "rollout_1",
                    "operation": operation,
                    "schema_maintenance": "ok",
                    **({"phase": "switched"} if operation == "switch" else {}),
                }
            )
        return ""

    monkeypatch.setattr(transport, "run", run)
    monkeypatch.setattr(
        transport,
        "deployment_module",
        lambda: SimpleNamespace(
            record_deployment=lambda ssh: records.append(ssh),
        ),
    )
    return environment, calls, records, credentials


def test_secret_credentials_travel_only_in_private_file_and_are_removed(
    transport,
    monkeypatch,
    tmp_path,
    capsys,
):
    before = os.umask(0o077)
    try:
        environment, calls, records, credentials = setup_transport(transport, monkeypatch, tmp_path)
        transport.main()
    finally:
        os.umask(before)
    assert credentials == [
        {
            "username": environment["MYSQL_MAINTENANCE_USERNAME"],
            "password": environment["MYSQL_MAINTENANCE_PASSWORD"],
        }
    ]
    public = capsys.readouterr().out + repr(calls)
    assert environment["MYSQL_MAINTENANCE_PASSWORD"] not in public
    assert environment["MYSQL_MAINTENANCE_USERNAME"] not in public
    assert not records
    assert calls[-1][0] == "remove temporary maintenance credentials"
    assert not list(tmp_path.glob("qs-ai-schema-*"))


def test_failed_or_unverified_switch_cannot_publish_deployment_marker(
    transport,
    monkeypatch,
    tmp_path,
):
    _, calls, records, _ = setup_transport(transport, monkeypatch, tmp_path, "switch")
    original = transport.run

    def fail(phase, arguments):
        if phase == "schema maintenance":
            raise RuntimeError("schema maintenance failed; output withheld")
        return original(phase, arguments)

    monkeypatch.setattr(transport, "run", fail)
    before = os.umask(0o077)
    try:
        with pytest.raises(RuntimeError, match="output withheld"):
            transport.main()
    finally:
        os.umask(before)
    assert not records
    assert calls[-1][0] == "remove temporary maintenance credentials"
    assert not (tmp_path / "deployment-receipt.json").exists()


def test_safe_failure_never_forwards_driver_or_ssh_secret_text(transport, monkeypatch):
    monkeypatch.setattr(
        transport.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="driver password private",
            stderr="database body private",
        ),
    )
    with pytest.raises(RuntimeError) as error:
        transport.run("schema maintenance", ["ssh", "example"])
    assert "driver password" not in str(error.value)
    assert "database body" not in str(error.value)


def test_preflight_metadata_survives_projection_but_free_text_errors_do_not(transport):
    expected = {"id": "rollout_1", "operation": "preflight"}
    receipt = transport.safe_receipt(
        json.dumps(
            {
                **expected,
                "schema_maintenance": "failed",
                "ok": False,
                "source_server_uuid": "83df0988-3e1b-11f1-b09e-00163e3ab5b1",
                "head": "0038_messaging_observations",
                "source_schema": "ai",
                "capabilities": {"process": True, "target_create_restore": False},
                "missing_capabilities": ["target_create_restore"],
                "reason": "potential raw driver credentials",
                "errors": {"password": "private"},
            }
        ),
        expected,
    )
    assert receipt["missing_capabilities"] == ["target_create_restore"]
    assert receipt["source_schema"] == "ai"
    assert "errors" not in receipt
    assert "reason" not in receipt


def test_failed_host_receipt_is_preserved_without_deployment_marker(
    transport, monkeypatch, tmp_path
):
    _, calls, records, _ = setup_transport(transport, monkeypatch, tmp_path, "switch")
    original = transport.run

    def failed(phase, arguments):
        if phase == "schema maintenance":
            return json.dumps(
                {
                    "id": "rollout_1",
                    "operation": "switch",
                    "schema_maintenance": "failed",
                    "reason": "unsafe internal detail",
                }
            )
        return original(phase, arguments)

    monkeypatch.setattr(transport, "run", failed)
    before = os.umask(0o077)
    try:
        with pytest.raises(RuntimeError, match="metadata receipt"):
            transport.main()
    finally:
        os.umask(before)
    receipt = json.loads((tmp_path / "schema-maintenance-receipt.json").read_text())
    assert receipt["schema_maintenance"] == "failed"
    assert "reason" not in receipt
    assert not records
    assert calls[-1][0] == "remove temporary maintenance credentials"
