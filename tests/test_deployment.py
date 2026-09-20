import importlib.util
import json
from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]


def load(relative):
    spec = importlib.util.spec_from_file_location(relative.replace("/", "_"), ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def remote():
    return load("deploy/serverA/deploy.py")


def test_mysql_components_round_trip():
    module = load("scripts/cd/deploy.py")
    password = "space @:/?#%$'\"\\中文"
    env = {
        "QS_AI_QS_ADDRESS": "qs-apiserver:9090",
        "MYSQL_HOST": "mysql.internal",
        "MYSQL_DATABASE": "qs_ai",
        "MYSQL_USERNAME": "ai@user",
        "MYSQL_PASSWORD": password,
    }
    url = make_url(module.database_url(env))
    assert url.password == password
    assert url.username == "ai@user"
    assert url.database == "qs_ai"
    encoded = module.runtime_config(env)["services"]["qs-ai"]["environment"]["QS_AI_DATABASE_URL"]
    assert make_url(encoded.replace("$$", "$")) == url
    assert (
        module.runtime_config(env)["services"]["qs-ai"]["environment"]["QS_AI_DATABASE_URL"]
        == encoded
    )
    env["MYSQL_DBNAME"] = "other"
    with pytest.raises(ValueError, match="disagree"):
        module.database_url(env)
    del env["MYSQL_DATABASE"]
    assert make_url(module.database_url(env)).database == "other"
    del env["MYSQL_PASSWORD"]
    with pytest.raises(ValueError, match="MYSQL_PASSWORD"):
        module.database_url(env)


def test_release_paths_and_private_atomic_state(remote, tmp_path, monkeypatch):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    for invalid in ("../other", "main", "a" * 40 + "-1/2"):
        with pytest.raises(ValueError):
            remote.release_path(invalid)
    path = tmp_path / "state.json"
    remote.write_json(path, {"current": "one"})
    remote.write_json(path, {"current": "two"})
    assert json.loads(path.read_text()) == {"current": "two"}
    assert path.stat().st_mode & 0o777 == 0o600
    assert not path.with_suffix(".tmp").exists()


def setup_release(remote, tmp_path, monkeypatch):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    revision = "a" * 40
    release = remote.release_path(revision + "-1-1")
    release.mkdir(parents=True)
    archive = release / "image.tar.gz"
    archive.write_bytes(b"image")
    manifest = {
        "revision": revision,
        "image_id": "sha256:abc",
        "archive_sha256": remote.hashlib.sha256(b"image").hexdigest(),
    }
    (release / "manifest.json").write_text(json.dumps(manifest))
    return release, manifest


def test_bad_archive_never_loads_or_migrates(remote, tmp_path, monkeypatch):
    release, _ = setup_release(remote, tmp_path, monkeypatch)
    (release / "image.tar.gz").write_bytes(b"damaged")
    monkeypatch.setattr(remote, "run", lambda *args: pytest.fail("must not run Docker"))
    with pytest.raises(remote.DeploymentError, match="checksum"):
        remote.apply(release, {})


def simulate_apply(remote, monkeypatch, manifest, fail_migration=False):
    calls = []

    def run(phase, args):
        calls.append(phase)
        if phase == "image identity":
            return json.dumps(
                [
                    {
                        "Id": manifest["image_id"],
                        "Architecture": "amd64",
                        "Config": {
                            "Labels": {"org.opencontainers.image.revision": manifest["revision"]}
                        },
                    }
                ]
            )
        if phase == "migration" and fail_migration:
            raise remote.DeploymentError("migration failed")
        return ""

    monkeypatch.setattr(remote, "run", run)
    return calls


def test_migration_failure_never_replaces_service(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    calls = simulate_apply(remote, monkeypatch, manifest, fail_migration=True)
    monkeypatch.setattr(remote, "probe", lambda *args: {"current": ["old"]})
    monkeypatch.setattr(remote, "verify", lambda *args: pytest.fail("must keep old service"))
    with pytest.raises(remote.DeploymentError, match="migration"):
        remote.apply(release, {"current": "b" * 40 + "-1-1"})
    assert calls == [
        "image load",
        "image identity",
        "compose services",
        "compose services",
        "migration",
    ]
    assert not (tmp_path / "state.json").exists()


@pytest.mark.parametrize("schema_changed", [False, True])
def test_failed_start_rolls_back_only_unchanged_schema(
    remote, tmp_path, monkeypatch, schema_changed
):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    simulate_apply(remote, monkeypatch, manifest)
    old = "b" * 40 + "-1-1"
    probes = iter([{"current": ["old"]}, {"current": ["new" if schema_changed else "old"]}, {}])
    monkeypatch.setattr(remote, "probe", lambda *args: next(probes))
    verified = []

    def verify(path):
        verified.append(path.name)
        if path == release:
            raise remote.DeploymentError("service readiness failed")

    monkeypatch.setattr(remote, "verify", verify)
    with pytest.raises(remote.DeploymentError, match="readiness"):
        remote.apply(release, {"current": old})
    assert verified == [release.name] + ([] if schema_changed else [old])
    assert not (tmp_path / "state.json").exists()


def test_first_release_failure_cleans_up_service(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    calls = simulate_apply(remote, monkeypatch, manifest)
    monkeypatch.setattr(remote, "probe", lambda *args: {"current": ["head"]})

    def fail(path):
        raise remote.DeploymentError("not ready")

    monkeypatch.setattr(remote, "verify", fail)
    with pytest.raises(remote.DeploymentError):
        remote.apply(release, {})
    assert calls[-1] == "first release cleanup"


def test_success_records_version_only_after_verification(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    simulate_apply(remote, monkeypatch, manifest)
    monkeypatch.setattr(remote, "probe", lambda *args: {"current": ["head"]})
    monkeypatch.setattr(remote, "verify", lambda path: None)
    remote.apply(release, {})
    assert json.loads((tmp_path / "state.json").read_text())["current"] == release.name
    assert not (release / "image.tar.gz").exists()
    assert json.loads((release / "verification.json").read_text())["phase"] == "ready"


def test_manual_rollback_checks_schema_before_switch(remote, tmp_path, monkeypatch):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    state = {"current": "a" * 40 + "-1-1", "previous": "b" * 40 + "-1-1"}

    def incompatible(*args):
        raise remote.DeploymentError("schema incompatible")

    monkeypatch.setattr(remote, "probe", incompatible)
    monkeypatch.setattr(remote, "verify", lambda *args: pytest.fail("must not switch"))
    with pytest.raises(remote.DeploymentError, match="incompatible"):
        remote.restore(state)
    assert not (tmp_path / "state.json").exists()


def test_invalid_database_config_fails_before_ssh(monkeypatch):
    module = load("scripts/cd/deploy.py")
    monkeypatch.setattr(
        module.os,
        "environ",
        {
            "MYSQL_DATABASE": "one",
            "MYSQL_DBNAME": "two",
        },
    )
    monkeypatch.setattr(module, "run", lambda *args: pytest.fail("must not contact SSH or Docker"))
    with pytest.raises(ValueError, match="disagree"):
        module.main()


def test_registry_auth_is_private_and_does_not_use_host_keychain(tmp_path):
    import base64

    module = load("scripts/cd/deploy.py")
    password = 'pa$word:" with spaces'
    module.write_registry_auth(
        tmp_path,
        {
            "ALIYUN_ACR_REGISTRY": "registry.test",
            "ALIYUN_ACR_USERNAME": "user",
            "ALIYUN_ACR_PASSWORD": password,
        },
    )
    path = tmp_path / "config.json"
    config = json.loads(path.read_text())
    assert list(config) == ["auths"]
    assert base64.b64decode(config["auths"]["registry.test"]["auth"]).decode() == "user:" + password
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        module.write_registry_auth(
            tmp_path,
            {
                "ALIYUN_ACR_REGISTRY": "registry.test",
                "ALIYUN_ACR_USERNAME": "user",
                "ALIYUN_ACR_PASSWORD": "replacement",
            },
        )


def test_tls_preflight_failure_preserves_running_release(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    calls = simulate_apply(remote, monkeypatch, manifest)
    original = remote.run

    def run(phase, args):
        if phase == "compose services":
            return "api\ngrpc\n"
        if phase == "TLS file preflight":
            raise remote.DeploymentError("TLS file preflight failed")
        return original(phase, args)

    monkeypatch.setattr(remote, "run", run)
    monkeypatch.setattr(remote, "probe", lambda *args: pytest.fail("must not access database"))
    with pytest.raises(remote.DeploymentError, match="TLS file"):
        remote.apply(release, {})
    assert calls == ["image load", "image identity"]
    assert not (tmp_path / "state.json").exists()


def test_verify_rejects_wrong_grpc_image(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    images = iter([manifest["image_id"], "sha256:wrong"])

    def run(phase, args):
        if phase == "service readiness":
            assert "--remove-orphans" in args
        if phase == "compose services":
            return "api\ngrpc\n"
        if phase == "running container":
            return args[-1]
        if phase == "running image":
            return next(images)
        return ""

    monkeypatch.setattr(remote, "run", run)
    monkeypatch.setattr(
        remote, "probe", lambda *args: pytest.fail("image mismatch must fail first")
    )
    with pytest.raises(remote.DeploymentError, match="Running image"):
        remote.verify(release)


def execution_environment():
    return {
        "MYSQL_HOST": "mysql.internal",
        "MYSQL_DATABASE": "qs_ai",
        "MYSQL_USERNAME": "ai",
        "MYSQL_PASSWORD": "synthetic-db-only",
        "QS_AI_EXECUTION_ENABLED": "true",
        "QS_AI_MODEL_ENDPOINT": "https://model.invalid/responses",
        "QS_AI_MODEL_API_KEY": "synthetic-$key-only",
        "QS_AI_QS_ADDRESS": "qs-apiserver:9090",
    }


def test_single_runtime_flags_preserve_delivery_when_generation_disabled():
    module = load("scripts/cd/deploy.py")
    env = execution_environment()
    runtime = module.runtime_config(env)["services"]
    assert set(runtime) == {"qs-ai"}
    values = runtime["qs-ai"]["environment"]
    assert values["QS_AI_MODEL_API_KEY"] == "synthetic-$$key-only"
    assert values["QS_AI_GENERATION__ENABLED"] == "true"
    env["QS_AI_EXECUTION_ENABLED"] = "false"
    del env["QS_AI_MODEL_API_KEY"], env["QS_AI_MODEL_ENDPOINT"]
    values = module.runtime_config(env)["services"]["qs-ai"]["environment"]
    assert values["QS_AI_GENERATION__ENABLED"] == "false"
    assert values["QS_AI_GRPC__RESULT_ADDRESS"] == "qs-apiserver:9090"
    assert "QS_AI_MODEL_API_KEY" not in values
    for key in ("QS_AI_MODEL_ENDPOINT", "QS_AI_MODEL_API_KEY", "QS_AI_QS_ADDRESS"):
        incomplete = execution_environment()
        del incomplete[key]
        with pytest.raises(ValueError):
            module.runtime_config(incomplete)


@pytest.mark.parametrize("flag", ["EXECUTION", "GOVERNANCE", "EVALUATION"])
def test_runtime_flags_reject_ambiguous_values(flag):
    module = load("scripts/cd/deploy.py")
    with pytest.raises(ValueError, match=f"QS_AI_{flag}_ENABLED"):
        module.runtime_config({**execution_environment(), f"QS_AI_{flag}_ENABLED": "yes"})


@pytest.mark.parametrize("flag", ["GOVERNANCE"])
def test_management_and_publication_binding_need_access_but_not_model_credentials(flag):
    module = load("scripts/cd/deploy.py")
    env = execution_environment()
    env["QS_AI_EXECUTION_ENABLED"] = "false"
    env[f"QS_AI_{flag}_ENABLED"] = "true"
    del env["QS_AI_MODEL_ENDPOINT"], env["QS_AI_MODEL_API_KEY"]
    services = module.runtime_config(env)["services"]
    assert set(services) == {"qs-ai"}
    assert services["qs-ai"]["environment"]["QS_AI_GRPC__ACCESS_ADDRESS"] == "qs-apiserver:9090"
    for address in ("", "https://qs-apiserver:9090", "qs-apiserver:0", "qs-apiserver:65536"):
        env["QS_AI_QS_ADDRESS"] = address
        with pytest.raises(ValueError):
            module.runtime_config(env)


def test_evaluation_is_independent_and_requires_credentials_before_deployment():
    module = load("scripts/cd/deploy.py")
    env = execution_environment()
    env["QS_AI_EXECUTION_ENABLED"] = "false"
    env["QS_AI_EVALUATION_ENABLED"] = "true"
    services = module.runtime_config(env)["services"]
    assert set(services) == {"qs-ai"}
    for key in ("QS_AI_MODEL_API_KEY", "QS_AI_MODEL_ENDPOINT"):
        incomplete = {name: value for name, value in env.items() if name != key}
        with pytest.raises(ValueError):
            module.runtime_config(incomplete)


@pytest.mark.parametrize("mode", ["disabled", "governance", "execution", "evaluation", "all"])
def test_execution_compose_resolves_isolation_tls_and_health(tmp_path, mode):
    import os
    import shutil
    import subprocess

    if not shutil.which("docker"):
        pytest.skip("Docker Compose is required for release configuration verification")
    module = load("scripts/cd/deploy.py")
    shutil.copy(ROOT / "deploy/serverA/compose.yaml", tmp_path / "compose.yaml")
    environment = execution_environment()
    for flag in ("EXECUTION", "GOVERNANCE", "EVALUATION"):
        environment[f"QS_AI_{flag}_ENABLED"] = str(mode in {flag.lower(), "all"}).lower()
    (tmp_path / "runtime.json").write_text(json.dumps(module.runtime_config(environment)))
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(tmp_path / "compose.yaml"),
            "-f",
            str(tmp_path / "runtime.json"),
            "config",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "QS_AI_IMAGE": "qs-ai:synthetic"},
        check=True,
    )
    services = json.loads(result.stdout)["services"]
    assert set(services) == {"qs-ai"}
    service = services["qs-ai"]
    values = service["environment"]
    for field, flag in (("GENERATION", "execution"), ("EVALUATION", "evaluation")):
        assert values[f"QS_AI_{field}__ENABLED"] == str(mode in {flag, "all"}).lower()
    assert service["read_only"]
    assert len(service["volumes"]) == 3
    assert all(v["read_only"] for v in service["volumes"])
    assert {"qs-ai-grpc", "qs-ai-api"} <= set(service["networks"]["backend"]["aliases"])
    assert service["stop_grace_period"] in {"210s", "3m30s"}
    assert "readyz" in " ".join(service["healthcheck"]["test"])
    assert values["QS_AI_GRPC__RESULT_ADDRESS"] == "qs-apiserver:9090"
    if mode in {"execution", "evaluation", "all"}:
        assert values["QS_AI_MODEL_API_KEY"] == "synthetic-$$key-only"
    else:
        assert "QS_AI_MODEL_API_KEY" not in values


def test_deployment_receipt_uses_actual_remote_revision(tmp_path, monkeypatch):
    module = load("scripts/cd/deploy.py")
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("DEPLOY_SHA", "a" * 40)
    actual = "b" * 40
    monkeypatch.setattr(
        module,
        "run",
        lambda *args: json.dumps({"current": actual + "-12-1", "previous": "ignored"}),
    )
    module.record_deployment(["ssh", "test"])
    assert json.loads((tmp_path / "deployment-receipt.json").read_text())["revision"] == actual
    assert output.read_text() == f"actual_revision={actual}\n"


def test_retention_failure_does_not_fail_successful_deployment(
    remote, tmp_path, monkeypatch, capsys
):
    release, _ = setup_release(remote, tmp_path, monkeypatch)
    (tmp_path / "state.json").write_text(json.dumps({"current": release.name}))

    def fail(*args):
        raise remote.DeploymentError("cleanup failed")

    monkeypatch.setattr(remote, "run", fail)
    remote.retain_successful_image(release)
    assert "::warning::" in capsys.readouterr().out
    assert not (release / "failure.json").exists()


def test_cutover_stops_admission_then_all_consumers_before_start(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    old = "b" * 40 + "-1-1"
    calls = simulate_apply(remote, monkeypatch, manifest)
    original = remote.run

    def run(phase, args):
        if phase == "compose services":
            calls.append(phase)
            return "api\ngrpc\nworker\ndelivery\nevaluation\n"
        return original(phase, args)

    monkeypatch.setattr(remote, "run", run)
    monkeypatch.setattr(remote, "probe", lambda *args: {"current": ["head"]})

    def verify(path):
        assert calls[-3:] == ["stop old admission", "drain release", "verify stopped"]

    monkeypatch.setattr(remote, "verify", verify)
    remote.apply(release, {"current": old})


def test_failed_new_release_is_stopped_before_old_restart(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    simulate_apply(remote, monkeypatch, manifest)
    old = "b" * 40 + "-1-1"
    monkeypatch.setattr(remote, "probe", lambda *args: {"current": ["head"]})
    order = []
    monkeypatch.setattr(remote, "stop_release", lambda path: order.append(("stop", path.name)))

    def verify(path):
        order.append(("start", path.name))
        if path == release:
            raise remote.DeploymentError("readiness failed")

    monkeypatch.setattr(remote, "verify", verify)
    with pytest.raises(remote.DeploymentError):
        remote.apply(release, {"current": old})
    assert order == [("stop", old), ("start", release.name), ("stop", release.name), ("start", old)]


def test_old_release_still_running_blocks_new_start(remote, tmp_path, monkeypatch):
    def run(phase, args):
        return "old-container" if phase == "verify stopped" else "api\ngrpc\n"

    monkeypatch.setattr(remote, "run", run)
    with pytest.raises(remote.DeploymentError, match="still running"):
        remote.stop_release(tmp_path)


def test_failed_manual_rollback_restores_current_without_changing_state(
    remote, tmp_path, monkeypatch
):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    state = {"current": "a" * 40 + "-1-1", "previous": "b" * 40 + "-1-1"}
    calls = []
    monkeypatch.setattr(remote, "probe", lambda p, *args: calls.append(("probe", p.name)))
    monkeypatch.setattr(remote, "stop_release", lambda p: calls.append(("stop", p.name)))

    def verify(path):
        calls.append(("verify", path.name))
        if path.name == state["previous"]:
            raise remote.DeploymentError("target failed")

    monkeypatch.setattr(remote, "verify", verify)
    with pytest.raises(remote.DeploymentError, match="target failed"):
        remote.restore(state)
    assert calls == [
        ("probe", state["previous"]),
        ("stop", state["current"]),
        ("verify", state["previous"]),
        ("stop", state["previous"]),
        ("probe", state["current"]),
        ("verify", state["current"]),
    ]
    assert not (tmp_path / "state.json").exists()
