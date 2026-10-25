"""Retired production modes fail before Docker, migration or service replacement."""

import json

import pytest

from tests.deployment_fixtures import transaction_binding
from tests.test_deployment import execution_environment, load, setup_release


@pytest.mark.parametrize("mode", ["missing", "disabled", "nsq", "normal-only"])
def test_packaging_requires_final_transaction_binding(mode):
    value = transaction_binding()
    if mode == "disabled":
        value["enabled"] = False
    elif mode == "nsq":
        for key in ("provider", "rocketmq", "publish_enabled", "transactions"):
            del value[key]
        value["nsqd"] = {"nsqd:4150": "http://nsqd:4151"}
    elif mode == "normal-only":
        del value["transactions"]
    environment = {**execution_environment(), "QS_AI_MESSAGING_BINDING": ""}
    if mode != "missing":
        environment["QS_AI_MESSAGING_BINDING"] = json.dumps(value)
    with pytest.raises(ValueError, match="contents withheld"):
        load("scripts/cd/deploy.py").runtime_config(environment)


@pytest.mark.parametrize("action", ["apply", "prepare", "restore"])
@pytest.mark.parametrize("mode", ["missing", "disabled", "nsq", "normal-only"])
def test_host_rejects_retired_target_before_any_external_action(
    tmp_path, monkeypatch, action, mode
):
    remote = load("deploy/serverA/deploy.py")
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    client = load("scripts/cd/deploy.py")
    config = client.runtime_config(
        {**execution_environment(), "QS_AI_MESSAGING_BINDING": json.dumps(transaction_binding())}
    )
    service = config["services"]["qs-ai"]
    options = json.loads(service["environment"]["QS_AI_MESSAGING"])
    if mode == "missing":
        del service["environment"]["QS_AI_MESSAGING"]
    else:
        if mode == "disabled":
            options["enabled"] = False
        elif mode == "nsq":
            for key in ("provider", "rocketmq", "publish_enabled", "transactions"):
                del options[key]
            options["nsqd"] = {"nsqd:4150": "http://nsqd:4151"}
        else:
            del options["transactions"]
        service["environment"]["QS_AI_MESSAGING"] = json.dumps(options)
    # A self-consistent digest must not authorize an obsolete production mode.
    manifest["messaging_binding_sha256"] = client.messaging_binding_digest(config)
    (release / "runtime.json").write_text(json.dumps(config))
    (release / "manifest.json").write_text(json.dumps(manifest))
    (release / "compose.yaml").write_text("services: {}\n")
    monkeypatch.setattr(remote, "run", lambda *a: pytest.fail("must fail before external actions"))
    monkeypatch.setattr(remote, "stop_release", lambda *a: pytest.fail("must keep current service"))
    monkeypatch.setattr(remote, "schema_followup_guard", lambda: None)
    with pytest.raises(remote.DeploymentError, match="contents withheld"):
        if action == "apply":
            remote.apply(release, {})
        elif action == "prepare":
            remote.stage_prepare(release)
        else:
            remote.restore({"current": "b" * 40 + "-1-1", "previous": release.name})
