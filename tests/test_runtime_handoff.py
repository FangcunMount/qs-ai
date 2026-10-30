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
    assert captured[0][1][-1].startswith("python3 -B /tmp/reviewed.py ")
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
    "conflict",
    [
        None,
        "named_user",
        "environment",
        "mount",
        "source",
        "image",
        "inventory",
        "budget",
        "root_uid",
        "private_files",
        "private_files_mismatch",
        "null_defaults",
    ],
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
    if conflict in {"private_files", "private_files_mismatch"}:
        original_read = Path.read_bytes

        def private_read(path):
            if path.parent == directory:
                raise PermissionError("Native profile remains root-private")
            return original_read(path)

        monkeypatch.setattr(Path, "read_bytes", private_read)
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
    if conflict in {"environment", "private_files_mismatch"}:
        service["environment"]["QS_AI_DATABASE_URL"] = "changed-private-url"
    if conflict == "mount":
        row["Mounts"][0]["RW"] = True
    if conflict == "source":
        image["Config"]["Env"] = ["QS_AI_RELEASE_SHA=" + "b" * 40]
    if conflict == "image":
        service["image"] = "sha256:" + "b" * 64
    if conflict == "budget":
        service["cpus"] = 1.0
    if conflict == "named_user":
        service["user"] = row["Config"]["User"] = image["Config"]["User"] = "app"
    if conflict == "null_defaults":
        service.update(user=None, command=None, entrypoint=None)
        row["Config"]["User"] = image["Config"]["User"] = "app"
        row["Config"]["Cmd"] = image["Config"]["Cmd"] = ["python", "-m", "server"]
        row["Config"]["Entrypoint"] = image["Config"]["Entrypoint"] = ["/app/entry"]
        service["environment"]["REMOVED_BY_COMPOSE"] = None
        image["Config"]["Env"].append("REMOVED_BY_COMPOSE=old")

    def run(phase, args):
        if phase == "handoff runtime":
            return json.dumps([row])
        if phase == "handoff image":
            return json.dumps([image])
        if phase == "handoff frozen Compose":
            return json.dumps(config)
        if phase == "handoff effective uid":
            return "0" if conflict == "root_uid" else "10001\n"
        assert phase == "handoff project inventory"
        return "c" * 64 + ("\n" + "e" * 64 if conflict == "inventory" else "")

    monkeypatch.setattr(module.deploy, "run", run)
    if conflict not in (None, "named_user", "private_files", "null_defaults"):
        with pytest.raises(module.deploy.DeploymentError):
            module.observe()
    else:
        facts, _ = module.observe()
        assert facts["revision"] == REVISION
        assert service["environment"]["QS_AI_DATABASE_URL"] not in json.dumps(facts)
        assert facts["state_sha256"] == module.digest(handoff.raw_state)
        assert facts["effective_uid"] == "10001"
        assert facts["native_files_raw_hashes_available"] is (conflict != "private_files")
        if conflict == "private_files":
            assert set(facts["native_files_sha256"].values()) == {None}
            assert facts["config_sha256"] == module.deploy.json_digest(config)


def test_handoff_failure_reports_only_a_bounded_phase_not_private_output():
    client = load("scripts/cd/deploy.py")
    raw = (
        b"private credential and configuration withheld\n"
        b"QS_AI_HANDOFF_FAILURE:runtime_binding:DeploymentError\n"
    )
    assert client.export_failure_kind(raw) == "handoff_runtime_binding_deploymenterror"
    assert (
        client.export_failure_kind(b"QS_AI_HANDOFF_FAILURE:private-credential:Error")
        == "unclassified"
    )


@pytest.mark.parametrize(
    ("method", "phase"),
    [
        ("require_final_messaging", "checkpoint_binding"),
        ("_offline_release_preflight", "offline_preflight"),
        ("expected_heads", "schema_contract"),
        ("probe", "schema_probe"),
    ],
)
def test_checkpoint_failure_classifies_real_stage_without_private_output(
    handoff, monkeypatch, method, phase
):
    def fail(*_args):
        raise handoff.module.deploy.DeploymentError("private output withheld")

    monkeypatch.setattr(handoff.module.deploy, method, fail)
    with pytest.raises(handoff.module.deploy.DeploymentError):
        prepared(handoff)
    assert handoff.module.PHASE == phase
    client = load("scripts/cd/deploy.py")
    assert (
        client.export_failure_kind(
            ("QS_AI_HANDOFF_FAILURE:" + phase + ":DeploymentError\n").encode()
        )
        == "handoff_" + phase + "_deploymenterror"
    )
    assert handoff.module.state_bytes() == handoff.raw_state


def test_final_runtime_recheck_has_its_own_phase(handoff, monkeypatch):
    observations = 0

    def observe():
        nonlocal observations
        observations += 1
        facts = copy.deepcopy(handoff.facts)
        if observations > 1:
            facts["container_id"] = "changed"
        handoff.module.PHASE = "runtime_binding"
        return facts, copy.deepcopy(handoff.config)

    monkeypatch.setattr(handoff.module, "observe", observe)
    with pytest.raises(handoff.module.deploy.DeploymentError, match="Runtime changed"):
        prepared(handoff)
    assert handoff.module.PHASE == "runtime_recheck"
    assert handoff.module.state_bytes() == handoff.raw_state


@pytest.mark.parametrize(
    ("prefix", "phase"),
    [
        ("Installed MQ materials not verified;", "material_receipt"),
        ("Reviewed material tool unavailable;", "material_tool"),
        ("MQ key file preflight failed (", "mq_key_files"),
        ("TLS file preflight failed (", "tls_files"),
        ("compose services failed (", "compose_validation"),
    ],
)
def test_offline_failure_exports_only_allowlisted_local_phase(handoff, monkeypatch, prefix, phase):
    def fail(*_args):
        raise handoff.module.deploy.DeploymentError(prefix + " private output")

    monkeypatch.setattr(handoff.module.deploy, "_offline_release_preflight", fail)
    with pytest.raises(handoff.module.deploy.DeploymentError):
        prepared(handoff)
    assert handoff.module.PHASE == phase
    client = load("scripts/cd/deploy.py")
    assert (
        client.export_failure_kind(
            ("QS_AI_HANDOFF_FAILURE:" + phase + ":DeploymentError\n").encode()
        )
        == "handoff_" + phase + "_deploymenterror"
    )
    assert handoff.module.state_bytes() == handoff.raw_state


@pytest.mark.parametrize("operation", ["run", "up", "stop", "exec", "config"])
@pytest.mark.parametrize("observed", [True, False])
def test_only_native_oneoff_probes_release_reserved_addresses(tmp_path, operation, observed):
    remote = load("deploy/serverA/deploy.py")
    release = tmp_path / "checkpoint"
    release.mkdir()
    config = {
        "services": {
            "qs-ai": {
                "environment": {"PRIVATE": "unchanged"},
                "networks": {
                    "infra-network": {
                        "ipv4_address": "172.20.0.225",
                        "ipv6_address": "fd00::225",
                        "aliases": ["qs-ai-grpc"],
                        "priority": 1000,
                    }
                },
            }
        }
    }
    for name, value in {
        "compose.yaml": config,
        "runtime.json": {"services": {"qs-ai": {"environment": {}}}},
        "manifest.json": {"provenance": "observed-existing-runtime" if observed else "historical"},
    }.items():
        (release / name).write_text(json.dumps(value))
    (release / "image.env").write_text("QS_AI_IMAGE=" + IMAGE + "\n")
    original = {p.name: p.read_bytes() for p in release.iterdir()}
    args = remote.compose(release, operation, "--rm")
    transient = None
    with pytest.raises(RuntimeError, match="command fails"):
        with remote.preflight_network(args) as command:
            if operation == "run" and observed:
                transient = Path(command[9])
                assert transient.is_file()
                assert transient.stat().st_mode & 0o077 == 0
                derived = json.loads(transient.read_text())
                expected = copy.deepcopy(config)
                for network in expected["services"]["qs-ai"]["networks"].values():
                    network.pop("ipv4_address")
                    network.pop("ipv6_address")
                assert derived == expected
                assert command[:9] == args[:9] and command[10:] == args[10:]
            else:
                assert command == args
            raise RuntimeError("command fails")
    assert transient is None or not transient.exists()
    assert {p.name: p.read_bytes() for p in release.iterdir()} == original
    assert args[9] == str(release / "compose.yaml")
