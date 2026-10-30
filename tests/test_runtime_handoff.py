"""Actual-runtime adoption preserves evidence and never starts/stops services."""

import contextlib
import copy
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_deployment import freeze_release_binding, load

REVISION = "d" * 40
IMAGE = "sha256:" + "a" * 64
NAME = REVISION + "-123-1"
HEAD = "0043_native_transaction_limit"


@pytest.fixture
def handoff(tmp_path, monkeypatch):
    module = load("deploy/serverA/runtime_handoff.py")
    monkeypatch.setattr(module.deploy, "ROOT", tmp_path)
    (tmp_path / "releases").mkdir()
    raw_state = b'{ "current": "old-pointer", "previous": "older-pointer" }\n'
    (tmp_path / "state.json").write_bytes(raw_state)
    existing = tmp_path / "fixture"
    freeze_release_binding(existing, {})
    runtime = json.loads((existing / "runtime.json").read_text())
    service = runtime["services"]["qs-ai"]
    service.update(image=IMAGE, user="10001", mem_limit=768 * 1024 * 1024, cpus=0.75)
    service["environment"]["QS_AI_DATABASE_URL"] = "mysql+asyncmy://test:literal$$value@host/db"
    service["volumes"].append(
        {
            "type": "bind",
            "source": "/data/infra/ssl/mysql/ca-chain.pem",
            "target": "/etc/qlume/mysql/ca-chain.pem",
            "read_only": True,
            "bind": {"create_host_path": False},
        }
    )
    config = {"services": {"qs-ai": service}, "name": "qs-ai"}
    facts = {
        "container_id": "c" * 64,
        "image_id": IMAGE,
        "revision": REVISION,
        "state_sha256": module.digest(raw_state),
        "config_sha256": module.deploy.json_digest(config),
        "native_files_sha256": {"native-compose": "f" * 64},
    }
    monkeypatch.setattr(module, "observe", lambda: (copy.deepcopy(facts), copy.deepcopy(config)))
    calls = []

    def run(phase, args):
        calls.append((phase, args))
        if phase == "MQ material receipt verification":
            from tests.deployment_fixtures import material_reply

            return json.dumps(material_reply())
        if phase == "compose services":
            return "qs-ai"
        if phase == "image schema contract":
            return json.dumps([HEAD])
        if phase == "database probe":
            return json.dumps({"current": [HEAD], "expected": [HEAD]})
        assert phase in {"TLS file preflight", "MQ key file preflight", "handoff live mTLS probe"}
        return ""

    monkeypatch.setattr(module.deploy, "run", run)
    monkeypatch.setattr(
        module.deploy,
        "installed_material_tool",
        lambda: (
            Path("/opt/fangcun-m4/material-tools") / (module.deploy.MATERIAL_TOOL_SHA256 + ".py")
        ),
    )
    for method in ("verify", "apply", "restore", "stop_release", "retain_successful_image"):
        monkeypatch.setattr(
            module.deploy, method, lambda *_a: pytest.fail("Runtime mutation forbidden")
        )
    return SimpleNamespace(
        module=module, facts=facts, raw_state=raw_state, calls=calls, config=config
    )


def prepared(handoff):
    f = handoff.facts
    return handoff.module.prepare(NAME, IMAGE, f["state_sha256"], f["config_sha256"])


def test_checkpoint_keeps_old_state_exact_and_only_adopts_after_reviewed_hash(handoff):
    module = handoff.module
    result = prepared(handoff)
    release = module.deploy.release_path(NAME)
    assert module.state_bytes() == handoff.raw_state
    assert (release / "old-state.json").read_bytes() == handoff.raw_state
    assert not (release / "verification.json").exists()
    assert module.deploy.release_image_id(release) == IMAGE
    assert module.deploy.require_final_messaging(release) is True
    assert "literal$$value" in (release / "compose.yaml").read_text()
    assert "ca-chain.pem" in (release / "compose.yaml").read_text()
    assert "ca-chain.pem" not in (release / "runtime.json").read_text()
    with pytest.raises(module.deploy.DeploymentError, match="proof differs"):
        module.adopt(NAME, "0" * 64)
    assert module.state_bytes() == handoff.raw_state
    assert module.adopt(NAME, result["prepared_sha256"]) == {
        "adopted": True,
        "release": NAME,
        "deployed": False,
    }
    assert json.loads(module.state_bytes()) == {"current": NAME, "previous": None}
    assert (release / "old-state.json").read_bytes() == handoff.raw_state
    proof = json.loads((release / "verification.json").read_text())
    assert proof["provenance"] == "observed-existing-runtime"
    assert proof["deployed"] is False
    assert not any(
        "up" in args or "stop" in args or "upgrade" in args or "down" in args
        for _, args in handoff.calls
    )


@pytest.mark.parametrize(
    "drift",
    [
        "container_id",
        "image_id",
        "revision",
        "state_sha256",
        "config_sha256",
        "native_files_sha256",
    ],
)
def test_changed_runtime_or_old_pointer_cannot_be_adopted(handoff, drift):
    result = prepared(handoff)
    handoff.facts[drift] = "changed"
    with pytest.raises(handoff.module.deploy.DeploymentError, match="changed"):
        handoff.module.adopt(NAME, result["prepared_sha256"])
    assert handoff.module.state_bytes() == handoff.raw_state


@pytest.mark.parametrize(
    "filename", ["runtime.json", "compose.yaml", "manifest.json", "old-state.json", "image.env"]
)
def test_changed_checkpoint_cannot_be_adopted(handoff, filename):
    result = prepared(handoff)
    path = handoff.module.deploy.release_path(NAME) / filename
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(handoff.module.deploy.DeploymentError, match="checkpoint changed"):
        handoff.module.adopt(NAME, result["prepared_sha256"])
    assert handoff.module.state_bytes() == handoff.raw_state


def test_schema_incompatibility_never_changes_the_release_pointer(handoff, monkeypatch):
    monkeypatch.setattr(
        handoff.module.deploy,
        "probe",
        lambda *_a: {"current": ["0040_module_table_names"], "expected": [HEAD]},
    )
    with pytest.raises(handoff.module.deploy.DeploymentError, match="exact rollback head"):
        prepared(handoff)
    assert handoff.module.state_bytes() == handoff.raw_state
    assert not (handoff.module.deploy.release_path(NAME) / "handoff-prepared.json").exists()


def test_stale_state_stops_normal_publish_before_any_runtime_or_database_action(
    tmp_path, monkeypatch
):
    remote = load("deploy/serverA/deploy.py")
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    monkeypatch.setattr(remote, "release_image_id", lambda *_a, **_k: IMAGE)
    calls = []

    def run(phase, args):
        calls.append(phase)
        return "c" * 64 if phase == "current runtime inventory" else "sha256:" + "b" * 64

    monkeypatch.setattr(remote, "run", run)
    with pytest.raises(remote.DeploymentError, match="differs from release state"):
        remote.guard_current_runtime({"current": NAME})
    assert calls == ["current runtime inventory", "current runtime image"]
    monkeypatch.setattr(remote, "global_deploy_lock", contextlib.nullcontext)
    monkeypatch.setattr(remote.sys, "argv", ["deploy.py", NAME])
    monkeypatch.setattr(remote, "apply", lambda *_a: pytest.fail("Must refuse before apply"))
    (tmp_path / "state.json").write_text(json.dumps({"current": NAME}))
    with pytest.raises(remote.DeploymentError, match="differs from release state"):
        remote.main()


def test_supplied_material_password_only_reaches_the_pinned_read_only_command(monkeypatch, capsys):
    remote = load("deploy/serverA/deploy.py")
    password = "private-$(do-not-execute)-value"
    monkeypatch.setattr(remote, "_MATERIAL_PASSWORD", password)
    seen = []

    def execute(args, **kwargs):
        seen.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout="verified", stderr="")

    monkeypatch.setattr(remote.subprocess, "run", execute)
    pinned = [
        "sudo",
        "-n",
        "/usr/bin/python3",
        "-I",
        "/opt/fangcun-m4/material-tools/" + remote.MATERIAL_TOOL_SHA256 + ".py",
        "--verify-installed",
        "--host",
        "serverA",
        "--expect-role",
        "ai",
        "--expect-revision",
        "m4-reviewed",
    ]
    remote.run("MQ material receipt verification", pinned)
    remote.run("arbitrary root command", ["sudo", "-n", "python3", "other.py"])
    assert seen[0][0] == ["sudo", "-S", "-p", "", *pinned[2:]]
    assert seen[0][1]["input"] == password + "\n"
    assert "input" not in seen[1][1]
    assert all(password not in " ".join(args) for args, _ in seen)
    assert password not in capsys.readouterr().out


def test_client_sends_password_over_ssh_stdin_and_never_argv(monkeypatch):
    client = load("scripts/cd/deploy.py")
    captured = []
    monkeypatch.setattr(
        client, "run", lambda phase, args, **kwargs: captured.append((phase, args, kwargs)) or "ok"
    )
    password = "private-$'literal'"
    assert (
        client.remote_operation(
            "prepare",
            ["ssh", "host"],
            "/tmp/reviewed.py",
            ["prepare", NAME],
            {"SVRA_SUDO_PASSWORD": password},
        )
        == "ok"
    )
    assert captured[0][2] == {"input": json.dumps({"material_password": password})}
    assert password not in " ".join(captured[0][1])
    assert hashlib.sha256(password.encode()).hexdigest() not in str(captured)


def test_checkpoint_compose_roundtrip_preserves_literal_environment_and_all_mounts(handoff):
    prepared(handoff)
    release = handoff.module.deploy.release_path(NAME)
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(release / "image.env"),
            "-f",
            str(release / "compose.yaml"),
            "-f",
            str(release / "runtime.json"),
            "config",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    restored = json.loads(result.stdout)["services"]["qs-ai"]
    original = handoff.config["services"]["qs-ai"]
    assert restored["image"] == IMAGE
    assert restored["environment"] == original["environment"]

    def mount_values(mounts):
        return sorted(
            (
                m["type"],
                m["source"],
                m["target"],
                m["read_only"],
                m["bind"].get("create_host_path", False),
            )
            for m in mounts
        )

    assert mount_values(restored["volumes"]) == mount_values(original["volumes"])
    frozen = json.loads((release / "runtime.json").read_text())
    assert all(
        m["bind"]["create_host_path"] is False for m in frozen["services"]["qs-ai"]["volumes"]
    )


@pytest.mark.parametrize(
    "conflict", [None, "environment", "mount", "source", "image", "inventory", "budget"]
)
def test_observation_compares_actual_runtime_with_frozen_config_without_exposing_it(
    handoff, tmp_path, monkeypatch, conflict
):
    module = load("deploy/serverA/runtime_handoff.py")
    monkeypatch.setattr(module.deploy, "ROOT", handoff.module.deploy.ROOT)
    directory = tmp_path / "native"
    directory.mkdir()
    monkeypatch.setattr(module, "NATIVE_ROOT", directory)
    compose = directory / "compose.json"
    compose.write_text("{}")
    (directory / "runtime.env").write_text("private-placeholder")
    config = copy.deepcopy(handoff.config)
    config["services"]["qs-ai"]["read_only"] = True
    service = config["services"]["qs-ai"]
    env = [
        key + "=" + str(value).replace("$$", "$") for key, value in service["environment"].items()
    ]
    env.append("QS_AI_RELEASE_SHA=" + REVISION)
    row = {
        "Id": "c" * 64,
        "Image": IMAGE,
        "Name": "/qs-ai",
        "State": {"Running": True, "Health": {"Status": "healthy"}},
        "Config": {
            "Env": env,
            "User": "10001",
            "Labels": {
                "com.docker.compose.project": "qs-ai",
                "com.docker.compose.service": "qs-ai",
                "com.docker.compose.project.config_files": str(compose),
            },
        },
        "HostConfig": {"Memory": 768 * 1024 * 1024, "NanoCpus": 750000000, "ReadonlyRootfs": True},
        "Mounts": [
            {"Type": "bind", "Source": m["source"], "Destination": m["target"], "RW": False}
            for m in service["volumes"]
        ],
    }
    image = {
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {"Env": ["QS_AI_RELEASE_SHA=" + REVISION]},
    }
    if conflict == "environment":
        service["environment"]["QS_AI_DATABASE_URL"] = "changed-private-url"
    if conflict == "mount":
        row["Mounts"][0]["RW"] = True
    if conflict == "source":
        image["Config"]["Env"] = ["QS_AI_RELEASE_SHA=" + "b" * 40]
    if conflict == "image":
        service["image"] = "sha256:" + "b" * 64
    if conflict == "budget":
        service["cpus"] = 1.0

    def run(phase, args):
        if phase == "handoff runtime":
            return json.dumps([row])
        if phase == "handoff image":
            return json.dumps([image])
        if phase == "handoff frozen Compose":
            return json.dumps(config)
        assert phase == "handoff project inventory"
        return "c" * 64 + ("\n" + "e" * 64 if conflict == "inventory" else "")

    monkeypatch.setattr(module.deploy, "run", run)
    if conflict:
        with pytest.raises(module.deploy.DeploymentError):
            module.observe()
    else:
        facts, _ = module.observe()
        assert facts["revision"] == REVISION
        assert service["environment"]["QS_AI_DATABASE_URL"] not in json.dumps(facts)
        assert facts["state_sha256"] == module.digest(handoff.raw_state)
