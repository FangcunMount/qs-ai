"""Root material command/result boundary; no daemon, database or model calls."""

import copy
import json
from pathlib import Path

import pytest

from tests.deployment_fixtures import material_reply
from tests.test_deployment import load, setup_release, simulate_apply


def test_material_preflight_uses_only_the_installed_readonly_root_entry(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    release, _ = setup_release(remote, tmp_path, monkeypatch)
    path = Path("/opt/fangcun-m4/material-tools") / (remote.MATERIAL_TOOL_SHA256 + ".py")
    monkeypatch.setattr(remote, "installed_material_tool", lambda: path)
    calls = []

    def run(phase, args):
        calls.append((phase, args))
        return json.dumps(material_reply())

    monkeypatch.setattr(remote, "run", run)
    remote.material_preflight(release)
    assert calls == [
        (
            "MQ material receipt verification",
            [
                "sudo",
                "-n",
                "/usr/bin/python3",
                "-I",
                str(path),
                "--verify-installed",
                "--host",
                "serverA",
                "--expect-role",
                "ai",
                "--expect-revision",
                "m4-final-ai",
            ],
        )
    ]


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "ready"),
        ("read_only", False),
        ("activated", True),
        ("role", "worker"),
        ("host", "serverD"),
        ("revision", "m4-other"),
        ("material_root", "/data/infra"),
        ("receipt_sha256", "not-reviewed"),
    ],
)
def test_material_reply_conflict_rejects_before_db_migration_or_stop(
    tmp_path, monkeypatch, field, value
):
    remote = load("deploy/serverA/deploy.py")
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    calls = simulate_apply(remote, monkeypatch, manifest)
    original = remote.run
    reply = copy.deepcopy(material_reply())
    reply[field] = value

    def run(phase, args):
        if phase == "MQ material receipt verification":
            calls.append(phase)
            return json.dumps(reply)
        return original(phase, args)

    monkeypatch.setattr(remote, "run", run)
    monkeypatch.setattr(remote, "probe", lambda *args: pytest.fail("must not open database"))
    monkeypatch.setattr(remote, "stop_release", lambda *args: pytest.fail("must not stop service"))
    state = tmp_path / "state.json"
    state.write_bytes(b'{"protected":true}')
    with pytest.raises(remote.DeploymentError, match="materials not verified"):
        remote.apply(release, {})
    assert state.read_bytes() == b'{"protected":true}'
    assert not {"migration", "MQ key file preflight", "TLS file preflight"}.intersection(calls)


def test_absent_installed_tool_fails_without_root_process(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    monkeypatch.setattr(remote, "Path", lambda _: tmp_path / "absent-root-tool")
    monkeypatch.setattr(remote, "run", lambda *args: pytest.fail("no root process allowed"))
    with pytest.raises(remote.DeploymentError, match="tool unavailable"):
        remote.installed_material_tool()
