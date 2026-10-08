"""Release preparation never admits runtime or touches the source database."""

import contextlib
import hashlib
import json
from pathlib import Path

import pytest

from tests.test_deployment import (
    image_config,
    inspected_image,
    load,
    setup_release,
    write_image_archive,
)

NEW = "0040_module_table_names"


def staged_release(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    (release / "runtime.json").write_text(json.dumps({"services": {"qs-ai": {"environment": {}}}}))
    (release / "compose.yaml").write_text("services: {qs-ai: {image: '${QS_AI_IMAGE}'}}\n")
    state = {"current": "b" * 40 + "-1-1", "previous": "c" * 40 + "-1-1"}
    (tmp_path / "state.json").write_text(json.dumps(state))
    calls = []

    def run(phase, args):
        calls.append((phase, args))
        if phase == "image identity":
            return json.dumps([inspected_image(manifest)])
        if phase == "compose services":
            return "qs-ai"
        if phase == "image schema contract":
            return json.dumps([NEW])
        if phase not in {"image load", "TLS file preflight", "MQ key file preflight"}:
            pytest.fail(f"Unexpected preparation side effect: {phase}")
        return ""

    monkeypatch.setattr(remote, "run", run)
    for name in ("probe", "stop_release", "verify", "apply", "restore", "retain_successful_image"):
        monkeypatch.setattr(
            remote, name, lambda *_a, **_kw: pytest.fail("Runtime must stay untouched")
        )
    return remote, release, manifest, calls


def test_preparation_never_contacts_source_database_or_running_release(tmp_path, monkeypatch):
    remote, release, manifest, calls = staged_release(tmp_path, monkeypatch)
    state_before = (tmp_path / "state.json").read_bytes()
    receipt = remote.stage_prepare(release)
    assert (tmp_path / "state.json").read_bytes() == state_before
    assert receipt["prepared"] is True
    assert receipt["release_id"] == release.name
    assert receipt["revision"] == manifest["revision"]
    assert receipt["image_id"] == manifest["image_id"]
    assert receipt["expected_heads"] == [NEW]
    assert json.loads((release / "prepared.json").read_text()) == receipt
    assert (release / "prepared.json").stat().st_mode & 0o777 == 0o600
    assert (release / "image.tar.gz").exists()
    assert not (release / "verification.json").exists()
    assert [phase for phase, _ in calls] == [
        "image load",
        "image identity",
        "compose services",
        "TLS file preflight",
        "compose services",
        "image schema contract",
    ]


@pytest.mark.parametrize("material", ["manifest.json", "runtime.json", "compose.yaml"])
def test_prepared_release_materials_cannot_be_rebound(tmp_path, monkeypatch, material):
    remote, release, _manifest, _calls = staged_release(tmp_path, monkeypatch)
    first = remote.stage_prepare(release)
    inode = (release / "prepared.json").stat().st_ino
    assert remote.stage_prepare(release) == first
    assert (release / "prepared.json").stat().st_ino == inode
    path = release / material
    path.write_text(path.read_text() + "\n")
    monkeypatch.setattr(remote, "run", lambda *_a: pytest.fail("Reject drift before Docker"))
    with pytest.raises(remote.DeploymentError, match="prepared.*binding"):
        remote.stage_prepare(release)
    assert json.loads((release / "prepared.json").read_text()) == first


@pytest.mark.parametrize("material", ["image.tar.gz", "image.env"])
def test_prepared_archive_and_loaded_image_binding_cannot_drift(tmp_path, monkeypatch, material):
    remote, release, _manifest, _calls = staged_release(tmp_path, monkeypatch)
    first = remote.stage_prepare(release)
    path = release / material
    path.write_bytes(path.read_bytes() + b"drift")
    monkeypatch.setattr(remote, "run", lambda *_a: pytest.fail("Reject drift before Docker"))
    with pytest.raises(remote.DeploymentError, match="checksum|binding invalid"):
        remote.stage_prepare(release)
    assert json.loads((release / "prepared.json").read_text()) == first


@pytest.mark.parametrize("protected", ["current", "previous", "ready"])
def test_prepare_cannot_overwrite_a_successful_release(tmp_path, monkeypatch, protected):
    remote, release, _manifest, _calls = staged_release(tmp_path, monkeypatch)
    if protected == "ready":
        (release / "verification.json").write_text(json.dumps({"phase": "ready"}))
    else:
        (tmp_path / "state.json").write_text(json.dumps({protected: release.name}))
    monkeypatch.setattr(remote, "run", lambda *_a: pytest.fail("Reject before Docker"))
    with pytest.raises(remote.DeploymentError, match="successful release"):
        remote.stage_prepare(release)
    assert not (release / "prepared.json").exists()


@pytest.mark.parametrize("failure", ["identity", "TLS", "MQ"])
def test_offline_failure_never_marks_release_prepared(tmp_path, monkeypatch, failure):
    remote, release, manifest, _calls = staged_release(tmp_path, monkeypatch)
    original = remote.run
    if failure == "MQ":
        monkeypatch.setattr(remote, "messaging_release", lambda *_a: True)

    def fail(phase, args):
        if failure == "identity" and phase == "image identity":
            inspected = inspected_image(manifest)
            inspected["Config"] = {**image_config(manifest["revision"])["config"], "User": "0"}
            return json.dumps([inspected])
        if (failure, phase) in {("TLS", "TLS file preflight"), ("MQ", "MQ key file preflight")}:
            raise remote.DeploymentError("Offline file check failed")
        return original(phase, args)

    monkeypatch.setattr(remote, "run", fail)
    with pytest.raises(remote.DeploymentError):
        remote.stage_prepare(release)
    assert not (release / "prepared.json").exists()


def test_remote_prepare_uses_both_locks_and_never_runs_retention(tmp_path, monkeypatch):
    remote, release, _manifest, _calls = staged_release(tmp_path, monkeypatch)
    locks = []

    @contextlib.contextmanager
    def global_lock():
        locks.append("global")
        yield

    monkeypatch.setattr(remote, "global_deploy_lock", global_lock)
    monkeypatch.setattr(remote.fcntl, "flock", lambda *_a: locks.append("service"))
    monkeypatch.setattr(remote.sys, "argv", ["deploy.py", "prepare", release.name])
    remote.main()
    assert locks == ["global", "service"]
    assert (release / "prepared.json").exists()


def client_environment(monkeypatch, tmp_path):
    env = {
        "DEPLOY_OPERATION": "prepare",
        "DEPLOY_SHA": "a" * 40,
        "GITHUB_RUN_ID": "12",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_OUTPUT": str(tmp_path / "output"),
        "SVRA_HOST": "servera.test",
        "SVRA_USERNAME": "deploy",
        "SVRA_SSH_KEY": "synthetic-private-key",
        "SVRA_HOST_PUBLIC_KEY": "ssh-ed25519 c3ludGhldGlj",
        "ALIYUN_ACR_REGISTRY": "registry.test",
        "ALIYUN_ACR_NAMESPACE": "test",
        "ALIYUN_ACR_USERNAME": "test",
        "ALIYUN_ACR_PASSWORD": "synthetic-registry",
        "IMAGE_DIGEST": "sha256:" + "d" * 64,
        "MYSQL_HOST": "mysql.test",
        "MYSQL_DATABASE": "ai",
        "MYSQL_USERNAME": "app",
        "MYSQL_PASSWORD": "synthetic-app-only",
        "QS_AI_QS_ADDRESS": "qs-apiserver:9090",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.chdir(tmp_path)
    return env


@pytest.mark.parametrize("wrong_receipt", [None, "revision", "runtime_sha256", "prepared"])
def test_client_prepare_exports_by_digest_and_records_only_preparation(
    tmp_path, monkeypatch, wrong_receipt
):
    module = load("scripts/cd/deploy.py")
    env = client_environment(monkeypatch, tmp_path)
    revision, release_id = env["DEPLOY_SHA"], env["DEPLOY_SHA"] + "-12-1"
    uploads, calls = {}, []
    config_id = None

    def export(_image, archive, _env):
        nonlocal config_id
        config_id = write_image_archive(archive, revision, tags=[f"qs-ai:{revision}"])

    def run(phase, args, **_kw):
        calls.append((phase, args))
        if phase == "server identity":
            return "serverA\n"
        if phase == "image inspect":
            # The exporter runs next; derive the same raw config ID without a Docker daemon.
            raw = json.dumps(image_config(revision), sort_keys=True).encode()
            return json.dumps(
                [
                    inspected_image(
                        {
                            "revision": revision,
                            "image_id": "sha256:" + hashlib.sha256(raw).hexdigest(),
                        }
                    )
                ]
            )
        if phase == "upload release":
            path = Path(args[-2])
            uploads[path.name] = path.read_bytes()
        if phase == "prepared release receipt":
            manifest = json.loads(uploads["manifest.json"])
            receipt = {
                "version": 1,
                "prepared": True,
                "release": release_id,
                "release_id": release_id,
                "revision": revision,
                "image_id": config_id,
                "source_image_id": manifest["image_id"],
                "archive_sha256": manifest["archive_sha256"],
                "registry_digest": manifest["digest"],
                "expected_heads": [NEW],
                **{
                    key + "_sha256": hashlib.sha256(uploads[name]).hexdigest()
                    for key, name in [
                        ("manifest", "manifest.json"),
                        ("runtime", "runtime.json"),
                        ("compose", "compose.yaml"),
                    ]
                },
            }
            if wrong_receipt:
                receipt[wrong_receipt] = False if wrong_receipt == "prepared" else "b" * 40
            return json.dumps(receipt)
        return ""

    monkeypatch.setattr(module, "run", run)
    monkeypatch.setattr(module, "pull_image", lambda image, _env: calls.append(("pull", image)))
    monkeypatch.setattr(module, "export_image", export)
    monkeypatch.setattr(module, "record_deployment", lambda *_a: pytest.fail("Not deployed"))
    if wrong_receipt:
        with pytest.raises(RuntimeError, match="preparation.*binding"):
            module.main()
        assert not (tmp_path / "release-receipt.json").exists()
        return
    module.main()
    receipt = json.loads((tmp_path / "release-receipt.json").read_text())
    assert receipt["prepared"] is True
    assert receipt["release_id"] == release_id
    assert (tmp_path / "output").read_text() == f"release_id={release_id}\nrevision={revision}\n"
    assert ("pull", "registry.test/test/qs-ai@" + env["IMAGE_DIGEST"]) in calls
    phases = [phase for phase, _args in calls]
    assert "remote preparation" in phases
    assert "remote deployment" not in phases
    assert "upload retention script" not in phases
    assert not (tmp_path / "deployment-receipt.json").exists()
