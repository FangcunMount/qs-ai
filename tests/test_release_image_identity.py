"""Release asset equivalence is strict; Docker's loaded ID is a separate identity."""

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from tests.test_deployment import (
    image_config,
    inspected_image,
    load,
    setup_release,
    write_image_archive,
)


@pytest.fixture
def remote():
    return load("deploy/serverA/deploy.py")


def prepare(remote, root, revision, actual):
    release = root / "releases" / (revision + "-1-1")
    release.mkdir(parents=True)
    archive = release / "image.tar.gz"
    source = write_image_archive(archive, revision, tags=[f"qs-ai:{revision}"])
    identity = remote.archive_identity(archive, f"qs-ai:{revision}", revision)
    manifest = {
        "revision": revision,
        "image_id": source,
        "image_identity": identity,
        "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "digest": "sha256:" + "f" * 64,
    }
    (release / "manifest.json").write_text(json.dumps(manifest))
    remote.bind_loaded_image(release, manifest, identity, actual)
    return release, manifest


def test_index_id_is_verified_by_raw_config_and_pinned_for_start(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    actual = "sha256:" + "d" * 64
    inspected = {**inspected_image(manifest), "Id": actual}
    calls = []

    def run(phase, args):
        calls.append((phase, args))
        if phase in {"image identity", "pinned image identity"}:
            return json.dumps([inspected])
        if phase == "loaded image archive":
            assert args[-1] == actual
            write_image_archive(Path(args[args.index("-o") + 1]), manifest["revision"])
        if phase == "compose services":
            return "qs-ai"
        if phase == "running container":
            return "container"
        if phase == "running image":
            return actual
        return ""

    monkeypatch.setattr(remote, "run", run)
    monkeypatch.setattr(remote, "probe", lambda *args: {"current": ["head"]})
    remote.apply(release, {})
    receipt = remote.loaded_image_binding(release)
    assert receipt["source_image_id"] == manifest["image_id"] != actual
    assert receipt["loaded_image_id"] == actual
    assert (release / "image.env").read_text() == f"QS_AI_IMAGE={actual}\n"
    assert json.loads((release / "verification.json").read_text())["image_id"] == actual
    assert calls.index(next(x for x in calls if x[0] == "loaded image archive")) < calls.index(
        next(x for x in calls if x[0] == "TLS file preflight")
    )
    assert not (release / "image.tar.gz").exists()
    # Verification works without the source archive or the original mutable tag.
    remote.verify(release)
    assert all(args[-1] == actual for phase, args in calls if phase == "pinned image identity")


@pytest.mark.parametrize("change", ["history", "layer", "wrong-tag"])
def test_index_metadata_match_alone_cannot_authorize_loading(remote, tmp_path, monkeypatch, change):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    actual = "sha256:" + "d" * 64
    calls = []

    def run(phase, args):
        calls.append(phase)
        if phase == "image identity":
            return json.dumps([{**inspected_image(manifest), "Id": actual}])
        if phase == "loaded image archive":
            config = image_config(manifest["revision"])
            if change == "history":
                config["history"] = [{"created_by": "different source asset"}]
            write_image_archive(
                Path(args[args.index("-o") + 1]),
                manifest["revision"],
                config=config,
                layer=b"wrong" if change == "layer" else b"layer",
                tags=["unrelated:tag"] if change == "wrong-tag" else None,
            )
        return ""

    monkeypatch.setattr(remote, "run", run)
    with pytest.raises(remote.DeploymentError, match="(digest mismatch|archive identity)"):
        remote.apply(release, {})
    assert "migration" not in calls and "TLS file preflight" not in calls
    assert not (release / "loaded-image.json").exists()
    assert not (tmp_path / "state.json").exists()


@pytest.mark.parametrize(
    "field",
    [
        "User",
        "Env",
        "Cmd",
        "Entrypoint",
        "WorkingDir",
        "Labels",
        "ExposedPorts",
        "Healthcheck",
        "Volumes",
        "StopSignal",
        "OnBuild",
        "UnexpectedConfigField",
    ],
)
def test_every_runtime_config_field_is_part_of_identity(remote, tmp_path, field):
    revision = "a" * 40
    archive = tmp_path / "image.tar.gz"
    source = write_image_archive(archive, revision, tags=[f"qs-ai:{revision}"])
    identity = remote.archive_identity(archive, f"qs-ai:{revision}", revision)
    inspected = inspected_image({"revision": revision, "image_id": source})
    inspected["Config"][field] = "changed-private-value"
    with pytest.raises(remote.DeploymentError, match="identity") as error:
        remote.match_image(inspected, identity, revision)
    assert "changed-private-value" not in str(error.value)


@pytest.mark.parametrize("field", ["Architecture", "Os", "Variant", "RootFS"])
def test_platform_and_all_layer_identities_are_checked(remote, tmp_path, field):
    revision = "a" * 40
    archive = tmp_path / "image.tar.gz"
    source = write_image_archive(archive, revision, tags=[f"qs-ai:{revision}"])
    identity = remote.archive_identity(archive, f"qs-ai:{revision}", revision)
    inspected = inspected_image({"revision": revision, "image_id": source})
    inspected[field] = (
        {"Type": "layers", "Layers": ["sha256:" + "0" * 64]} if field == "RootFS" else "wrong"
    )
    with pytest.raises(remote.DeploymentError, match="identity"):
        remote.match_image(inspected, identity, revision)


def test_docker_empty_defaults_do_not_hide_nonempty_config(remote):
    base = {"Env": ["PATH=/app"]}
    assert remote.config_digest(base) == remote.config_digest({**base, **remote.CONFIG_DEFAULTS})
    for field, default in remote.CONFIG_DEFAULTS.items():
        assert remote.config_digest(base) != remote.config_digest({**base, field: "nonempty"})
        if default is False:
            assert remote.config_digest(base) != remote.config_digest({**base, field: 0})


@pytest.mark.parametrize(
    "failure", ["layer", "source", "manifest", "digest", "tag", "arch", "revision"]
)
def test_bad_asset_never_loads_or_touches_existing_service(remote, tmp_path, monkeypatch, failure):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    if failure in {"layer", "tag", "arch", "revision"}:
        config = image_config(manifest["revision"])
        if failure == "arch":
            config["architecture"] = "arm64"
        if failure == "revision":
            config["config"]["Labels"]["org.opencontainers.image.revision"] = "b" * 40
        write_image_archive(
            release / "image.tar.gz",
            manifest["revision"],
            config=config,
            tags=["unrelated:tag"] if failure == "tag" else [f"qs-ai:{manifest['revision']}"],
            layer=b"changed" if failure == "layer" else b"layer",
        )
        manifest["archive_sha256"] = hashlib.sha256(
            (release / "image.tar.gz").read_bytes()
        ).hexdigest()
    if failure == "source":
        manifest["image_id"] = "sha256:" + "d" * 64
    if failure == "manifest":
        manifest["image_identity"] = {"forged": True}
    if failure == "digest":
        manifest["digest"] = "not-a-digest"
    (release / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(
        remote, "run", lambda *args: pytest.fail("no Docker or database before validation")
    )
    with pytest.raises(remote.DeploymentError):
        remote.apply(release, {})
    assert not (tmp_path / "state.json").exists()


def test_retention_protects_actual_loaded_ids_not_export_ids(remote, tmp_path, monkeypatch):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    current_id, previous_id = "sha256:" + "c" * 64, "sha256:" + "b" * 64
    current, source = prepare(remote, tmp_path, "a" * 40, current_id)
    previous, _ = prepare(remote, tmp_path, "b" * 40, previous_id)
    (tmp_path / "state.json").write_text(
        json.dumps({"current": current.name, "previous": previous.name})
    )
    calls = []
    monkeypatch.setattr(remote, "run", lambda phase, args: calls.append((phase, args)))
    remote.retain_successful_image(current)
    phase, args = calls[0]
    assert phase == "image retention"
    assert args[args.index("--image-ref") + 1] == current_id
    assert args[args.index("--protect-image-id") + 1] == previous_id
    assert source["image_id"] not in args


def test_rollback_uses_pinned_actual_id_before_probe_and_restores_state(
    remote, tmp_path, monkeypatch
):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    current, _ = prepare(remote, tmp_path, "a" * 40, "sha256:" + "c" * 64)
    target_id = "sha256:" + "b" * 64
    target, manifest = prepare(remote, tmp_path, "b" * 40, target_id)
    calls = []

    def run(phase, args):
        calls.append(phase)
        assert phase == "pinned image identity" and args[-1] == target_id
        return json.dumps([{**inspected_image(manifest), "Id": target_id}])

    monkeypatch.setattr(remote, "run", run)
    monkeypatch.setattr(remote, "probe", lambda path, head: calls.append("probe"))
    monkeypatch.setattr(remote, "stop_release", lambda path: calls.append("stop"))
    monkeypatch.setattr(remote, "verify", lambda path: calls.append("verify"))
    remote.restore({"current": current.name, "previous": target.name})
    assert calls == ["pinned image identity", "probe", "stop", "verify"]
    assert json.loads((tmp_path / "state.json").read_text())["current"] == target.name


@pytest.mark.parametrize("tamper", ["image.env", "manifest.json", "loaded-image.json"])
def test_binding_drift_blocks_rollback_before_any_command(remote, tmp_path, monkeypatch, tamper):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    current, _ = prepare(remote, tmp_path, "a" * 40, "sha256:" + "c" * 64)
    target, _ = prepare(remote, tmp_path, "b" * 40, "sha256:" + "b" * 64)
    path = target / tamper
    if tamper == "image.env":
        path.write_text("QS_AI_IMAGE=qs-ai:mutable\n")
    else:
        value = json.loads(path.read_text())
        value["archive_sha256"] = "changed"
        path.write_text(json.dumps(value))
    monkeypatch.setattr(
        remote, "run", lambda *args: pytest.fail("rollback must fail before preflight or stop")
    )
    with pytest.raises(remote.DeploymentError, match="binding invalid"):
        remote.restore({"current": current.name, "previous": target.name})


def test_legacy_mutable_tag_must_still_match_original_id(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    (release / "image.env").write_text(f"QS_AI_IMAGE=qs-ai:{manifest['revision']}\n")
    monkeypatch.setattr(remote, "run", lambda *args: json.dumps([{"Id": "sha256:" + "d" * 64}]))
    with pytest.raises(remote.DeploymentError, match="Historical image identity"):
        remote.release_image_id(release)


def test_existing_release_binding_and_successful_releases_are_immutable(
    remote, tmp_path, monkeypatch
):
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    release, manifest = prepare(remote, tmp_path, "a" * 40, "sha256:" + "c" * 64)
    before = {p.name: p.read_bytes() for p in release.iterdir()}
    with pytest.raises(remote.DeploymentError, match="cannot be overwritten"):
        remote.bind_loaded_image(
            release, manifest, manifest["image_identity"], "sha256:" + "d" * 64
        )
    for state in ({"current": release.name}, {"previous": release.name}):
        with pytest.raises(remote.DeploymentError, match="cannot be overwritten"):
            remote.apply(release, state)
    assert before == {p.name: p.read_bytes() for p in release.iterdir()}


def test_client_reuses_validator_and_refuses_existing_release_directory(tmp_path):
    client = load("scripts/cd/deploy.py")
    assert client.release_identity_module().config_digest({}) == load(
        "deploy/serverA/deploy.py"
    ).config_digest({})
    target = "/opt/qs-ai/releases/" + "a" * 40 + "-1-1"
    command = client.release_directory_command(target, "test")
    assert f"mkdir -p {target}" not in command and "chown -R" not in command
    # Exercise only the mkdir portion, mapped to a private local directory.
    mkdir = (
        command.split(" && sudo -n chown")[0]
        .replace("sudo -n ", "")
        .replace("/opt/qs-ai", str(tmp_path))
    )
    assert subprocess.run(mkdir, shell=True).returncode == 0
    preserved = tmp_path / "releases" / Path(target).name / "manifest.json"
    preserved.write_text("rollback material")
    assert subprocess.run(mkdir, shell=True, capture_output=True).returncode != 0
    assert preserved.read_text() == "rollback material"


def test_gzip_layer_blob_retains_uncompressed_diff_id(remote, tmp_path):
    import gzip

    revision = "a" * 40
    archive = tmp_path / "image.tar.gz"
    write_image_archive(
        archive, revision, tags=[f"qs-ai:{revision}"], layer=gzip.compress(b"layer")
    )
    assert (
        remote.archive_identity(archive, f"qs-ai:{revision}", revision)["rootfs_diff_ids"]
        == image_config(revision)["rootfs"]["diff_ids"]
    )


@pytest.mark.parametrize("failure", ["duplicate-member", "multiple-images", "duplicate-config-key"])
def test_ambiguous_archives_fail_before_loading(remote, tmp_path, failure):
    import io
    import tarfile

    revision = "a" * 40
    path = tmp_path / "ambiguous.tar"
    raw = json.dumps(image_config(revision)).encode()
    if failure == "duplicate-config-key":
        raw = raw.replace(
            b'"architecture": "amd64"', b'"architecture": "arm64", "architecture": "amd64"'
        )
    config_name = hashlib.sha256(raw).hexdigest() + ".json"
    entry = {"Config": config_name, "RepoTags": [f"qs-ai:{revision}"], "Layers": ["layer.tar"]}
    manifest = [entry, entry] if failure == "multiple-images" else [entry]
    members = [
        ("manifest.json", json.dumps(manifest).encode()),
        (config_name, raw),
        ("layer.tar", b"layer"),
    ]
    if failure == "duplicate-member":
        members.append(("manifest.json", json.dumps([entry]).encode()))
    with tarfile.open(path, "w") as archive:
        for name, content in members:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    with pytest.raises(remote.DeploymentError, match="archive identity"):
        remote.archive_identity(path, f"qs-ai:{revision}", revision)


def test_receipt_publication_is_atomic_and_failed_publish_can_resume(remote, tmp_path, monkeypatch):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    identity = remote.archive_identity(
        release / "image.tar.gz", f"qs-ai:{manifest['revision']}", manifest["revision"]
    )
    actual = "sha256:" + "d" * 64
    link = remote.os.link

    def fail_publish(*args):
        raise OSError("injected before publication")

    monkeypatch.setattr(remote.os, "link", fail_publish)
    with pytest.raises(OSError):
        remote.bind_loaded_image(release, manifest, identity, actual)
    assert not (release / "loaded-image.json").exists()
    assert not list(release.glob(".loaded-*"))
    assert (release / "image.tar.gz").exists()
    monkeypatch.setattr(remote.os, "link", link)
    remote.bind_loaded_image(release, manifest, identity, actual)
    assert remote.loaded_image_binding(release)["loaded_image_id"] == actual
    assert (release / "loaded-image.json").stat().st_mode & 0o777 == 0o600
    assert (release / "image.env").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("reference", ["other:tag", "qs-ai:tag;evil", "qs-ai@sha256:bad", "", None])
def test_release_reference_is_bounded_and_never_shell_code(
    remote, tmp_path, monkeypatch, reference
):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    manifest["image_ref"] = reference
    (release / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(remote, "run", lambda *args: pytest.fail("invalid reference before Docker"))
    with pytest.raises(remote.DeploymentError, match="reference"):
        remote.apply(release, {})


def test_older_successful_release_cannot_be_reapplied_even_outside_current_state(
    remote, tmp_path, monkeypatch
):
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    (release / "verification.json").write_text(json.dumps({"phase": "ready"}))
    before = {p.name: p.read_bytes() for p in release.iterdir()}
    monkeypatch.setattr(
        remote, "run", lambda *args: pytest.fail("old successful release must stay immutable")
    )
    with pytest.raises(remote.DeploymentError, match="cannot be overwritten"):
        remote.apply(release, {"current": "b" * 40 + "-1-1", "previous": "c" * 40 + "-1-1"})
    assert before == {p.name: p.read_bytes() for p in release.iterdir()}
