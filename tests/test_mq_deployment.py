"""Frozen deployment bindings and preflight failures do not replace business services."""

import json
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from qs_ai.config import MessagingOptions
from qs_ai.maintenance import messaging_preflight
from tests.test_deployment import execution_environment, load, setup_release, simulate_apply


def binding():
    return {
        "binding_revision": "mq-ai-v1",
        "enabled": True,
        "nsqd": {"nsqd:4150": "http://nsqd:4151"},
        "signing_key_file": "/run/qs-ai-jose/ai.sign.v1.json",
        "decrypt_key_files": {"ai.encrypt.v1": "/run/qs-ai-jose/ai.encrypt.v1.json"},
        "qs_signer_files": {"qs.sign.v1": "/run/qs-ai-jose/qs.sign.v1.json"},
        "qs_recipient_key_file": "/run/qs-ai-jose/qs.encrypt.v1.json",
        "max_in_flight": 1,
    }


def runtime(value=None):
    module = load("scripts/cd/deploy.py")
    return module.runtime_config(
        {**execution_environment(), "QS_AI_MESSAGING_BINDING": json.dumps(value or binding())}
    )


def freeze(release, manifest, value=None):
    module = load("scripts/cd/deploy.py")
    config = runtime(value)
    manifest["messaging_binding_sha256"] = module.messaging_binding_digest(config)
    (release / "runtime.json").write_text(json.dumps(config))
    (release / "manifest.json").write_text(json.dumps(manifest))
    return config


def test_binding_adds_only_existing_settings_and_individual_readonly_mounts():
    module = load("scripts/cd/deploy.py")
    original = module.runtime_config(execution_environment())
    assert "volumes" not in original["services"]["qs-ai"]
    new = runtime()
    service = new["services"]["qs-ai"]
    assert set(new["services"]) == {"qs-ai"}
    assert {k: v for k, v in service["environment"].items() if k != "QS_AI_MESSAGING"} == original[
        "services"
    ]["qs-ai"]["environment"]
    options = json.loads(service["environment"]["QS_AI_MESSAGING"])
    assert MessagingOptions.model_validate(options).max_in_flight == 1
    assert len(service["volumes"]) == 4
    for mount in service["volumes"]:
        assert mount["read_only"] and not mount["bind"]["create_host_path"]
        assert (
            mount["source"]
            == "/data/infra/qs-ai-messaging/versions/mq-ai-v1/" + Path(mount["target"]).name
        )
    assert "private" not in json.dumps(new)


@pytest.mark.parametrize(
    "field,value",
    [
        ("binding_revision", "../escape"),
        ("enabled", "true"),
        ("max_in_flight", True),
        ("max_in_flight", 2),
        ("nsqd", {}),
        ("nsqd", {"nsqd:4150": "http://other:4151"}),
        ("nsqd", {"nsqd:4150": "http://user:secret@nsqd:4151"}),
        ("nsqd", {"nsqd:70000": "http://nsqd:4151"}),
        ("signing_key_file", "/etc/key.json"),
        ("signing_key_file", "/run/qs-ai-jose/qs.sign.v1.json"),
        ("decrypt_key_files", {"wrong": "/run/qs-ai-jose/ai.encrypt.v1.json"}),
        ("qs_signer_files", {}),
        ("private_key", "DO_NOT_DISCLOSE"),
    ],
)
def test_bad_binding_is_rejected_without_disclosing_contents(field, value):
    original = binding()
    original[field] = value
    with pytest.raises(ValueError, match="contents withheld") as error:
        runtime(original)
    assert "DO_NOT_DISCLOSE" not in str(error.value) and "secret" not in str(error.value)


@pytest.mark.parametrize("raw", ['{"enabled":true,"enabled":false}', "[1]", "x" * 16385])
def test_ambiguous_or_oversized_binding_is_rejected(raw):
    with pytest.raises(ValueError, match="contents withheld"):
        load("scripts/cd/deploy.py").runtime_config(
            {**execution_environment(), "QS_AI_MESSAGING_BINDING": raw}
        )


def test_changed_release_binding_and_disabled_rollback_are_rejected(tmp_path):
    remote = load("deploy/serverA/deploy.py")
    current, target = tmp_path / "current", tmp_path / "target"
    current.mkdir()
    target.mkdir()
    config = freeze(current, {})
    assert remote.messaging_release(current)
    with pytest.raises(remote.DeploymentError, match="cannot fall back"):
        remote.messaging_transition(current, target)
    freeze(target, {})
    remote.messaging_transition(current, target)
    config["services"]["qs-ai"]["volumes"][0]["read_only"] = False
    (current / "runtime.json").write_text(json.dumps(config))
    with pytest.raises(remote.DeploymentError, match="contents withheld"):
        remote.messaging_release(current)


def test_key_preflight_failure_never_stops_or_replaces_old_service(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    freeze(release, manifest)
    simulate_apply(remote, monkeypatch, manifest)
    calls = []
    original = remote.run

    def run(phase, args):
        calls.append(phase)
        if phase == "compose services":
            return "qs-ai"
        if phase == "MQ key file preflight":
            raise remote.DeploymentError("MQ key file preflight failed")
        return original(phase, args)

    monkeypatch.setattr(remote, "run", run)
    monkeypatch.setattr(remote, "stop_release", lambda _: pytest.fail("must not stop old service"))
    with pytest.raises(remote.DeploymentError, match="key file preflight"):
        remote.apply(release, {"current": "b" * 40 + "-1-1"})
    assert "migration" not in calls and not (tmp_path / "state.json").exists()


@pytest.mark.parametrize(
    "failure", ["wrong-kid", "wrong-private-role", "world-readable", "wrong-use", "missing"]
)
def test_key_preflight_rejects_actual_loader_or_permissions_without_material(monkeypatch, failure):
    options = MessagingOptions.model_validate(
        {k: v for k, v in binding().items() if k != "binding_revision"}
    )
    monkeypatch.setattr(
        Path,
        "stat",
        lambda _: SimpleNamespace(
            st_mode=stat.S_IFREG | (0o644 if failure == "world-readable" else 0o640), st_size=100
        ),
    )

    def read_key(path, *, private, expected_id):
        if failure in {"wrong-kid", "wrong-private-role", "missing"}:
            raise ValueError("DO_NOT_DISCLOSE")
        return {"use": "enc" if failure == "wrong-use" else None}

    monkeypatch.setattr(messaging_preflight, "read_key", read_key)
    with pytest.raises(ValueError, match="material withheld") as error:
        messaging_preflight.check_keys(options)
    assert "DO_NOT_DISCLOSE" not in str(error.value)


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "missing",
        "kid",
        "role",
        "public-private",
        "curve",
        "point",
        "use",
        "alg",
        "operations",
        "permissions",
    ],
)
def test_real_key_files_are_validated_without_starting_resources(tmp_path, monkeypatch, failure):
    from jwcrypto import jwk

    original_stat, original_read = Path.stat, messaging_preflight.read_key
    options = MessagingOptions.model_validate(
        {k: v for k, v in binding().items() if k != "binding_revision"}
    )
    for role in ["ai.sign", "ai.encrypt", "qs.sign", "qs.encrypt"]:
        key = jwk.JWK.generate(kty="EC", crv="P-256", kid=role + ".v1")
        raw = json.loads(key.export(private_key=role.startswith("ai.")))
        if role == "qs.sign" and failure == "public-private":
            raw = json.loads(key.export(private_key=True))
        path = tmp_path / (role + ".v1.json")
        if role == "ai.sign":
            if failure == "missing":
                continue
            if failure == "kid":
                raw["kid"] = "untrusted"
            if failure == "role":
                raw.pop("d")
            if failure == "curve":
                raw["crv"] = "P-384"
            if failure == "point":
                raw["x"] = "AAAA"
            if failure == "use":
                raw["use"] = "enc"
            if failure == "alg":
                raw["alg"] = "ES384"
            if failure == "operations":
                raw["key_ops"] = ["encrypt"]
        path.write_text(json.dumps(raw))
        path.chmod(
            0o644
            if role.startswith("qs.") or (role == "ai.sign" and failure == "permissions")
            else 0o640
        )

    def relocated_stat(path, **kwargs):
        actual = tmp_path / path.name if path.parent == Path("/run/qs-ai-jose") else path
        return original_stat(actual, **kwargs)

    def relocated_read(path, *, private, expected_id):
        return original_read(
            str(tmp_path / Path(path).name), private=private, expected_id=expected_id
        )

    monkeypatch.setattr(Path, "stat", relocated_stat)
    monkeypatch.setattr(messaging_preflight, "read_key", relocated_read)
    if failure is None:
        messaging_preflight.check_keys(options)
    else:
        with pytest.raises(ValueError, match="material withheld"):
            messaging_preflight.check_keys(options)


def test_enabled_compose_keeps_one_service_original_tls_and_stop_budget(tmp_path):
    import os
    import subprocess

    from tests.test_deployment import ROOT

    (tmp_path / "runtime.json").write_text(json.dumps(runtime()))
    p = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "deploy/serverA/compose.yaml"),
            "-f",
            str(tmp_path / "runtime.json"),
            "config",
            "--format",
            "json",
        ],
        env={**os.environ, "QS_AI_IMAGE": "qs-ai:disposable"},
        capture_output=True,
        text=True,
    )
    assert p.returncode == 0, "Required Docker Compose configuration evidence unavailable"
    services = json.loads(p.stdout)["services"]
    assert set(services) == {"qs-ai"}
    service = services["qs-ai"]
    assert len(service["volumes"]) == 7 and all(v["read_only"] for v in service["volumes"])
    assert service["stop_grace_period"] == "3m30s"
    assert all(not v["bind"].get("create_host_path", False) for v in service["volumes"])
    assert {"qs-ai-grpc", "qs-ai-api"} <= set(service["networks"]["backend"]["aliases"])


def test_manual_mq_rollback_checks_old_key_files_before_stopping_current(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    monkeypatch.setattr(remote, "release_image_id", lambda *args, **kwargs: "sha256:" + "b" * 64)
    state = {"current": "a" * 40 + "-1-1", "previous": "b" * 40 + "-1-1"}
    for name in state.values():
        release = remote.release_path(name)
        release.mkdir(parents=True)
        freeze(release, {})
    monkeypatch.setattr(
        remote, "stop_release", lambda _: pytest.fail("current must remain running")
    )

    def run(phase, args):
        assert phase == "MQ key file preflight"
        raise remote.DeploymentError("old MQ key files unavailable")

    monkeypatch.setattr(remote, "run", run)
    with pytest.raises(remote.DeploymentError, match="old MQ key"):
        remote.restore(state)
    assert not (tmp_path / "state.json").exists()


def test_messaging_disabled_apply_cannot_replace_confirmed_mq_owner(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    release, _ = setup_release(remote, tmp_path, monkeypatch)
    current = remote.release_path("b" * 40 + "-1-1")
    current.mkdir(parents=True)
    freeze(current, {})
    monkeypatch.setattr(
        remote, "run", lambda *args: pytest.fail("must reject before Docker or migration")
    )
    with pytest.raises(remote.DeploymentError, match="cannot fall back"):
        remote.apply(release, {"current": current.name})
