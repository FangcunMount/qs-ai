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
        "MYSQL_HOST": "mysql.internal",
        "MYSQL_DATABASE": "qs_ai",
        "MYSQL_USERNAME": "ai@user",
        "MYSQL_PASSWORD": password,
    }
    url = make_url(module.database_url(env))
    assert url.password == password
    assert url.username == "ai@user"
    assert url.database == "qs_ai"
    encoded = module.runtime_config(env)["services"]["api"]["environment"]["QS_AI_DATABASE_URL"]
    assert make_url(encoded.replace("$$", "$")) == url
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
    assert calls == ["image load", "image identity", "migration"]
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
