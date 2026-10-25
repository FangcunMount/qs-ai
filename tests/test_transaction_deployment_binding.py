"""Final AI owner survives packaging and the standard-library host verifier."""

import copy
import json

import pytest

from qs_ai.config import MessagingOptions
from tests.test_deployment import load
from tests.test_mq_deployment import freeze, runtime
from tests.test_rocketmq_binding import rocketmq_binding


def transaction_binding():
    value = rocketmq_binding()
    value["binding_revision"] = "m4-final-ai"
    value["transactions"] = {
        "ai.mysql": {
            "store_id": "Original-AI-Primary",
            "writer_epoch": "Original-Writer-Epoch",
            "route_revision": "m3-1",
            "limits": {
                "sends": 4,
                "branches": 16,
                "check_workers": 1,
                "check_queue": 16,
                "reconcile_batch": 100,
                "check_timeout": 2,
                "rpc_timeout": 5,
                "lease": 60,
                "reconcile_interval": 5,
            },
        }
    }
    return value


def test_final_transaction_binding_round_trips_into_actual_options_and_host_verifier(tmp_path):
    value = transaction_binding()
    config = freeze(tmp_path, {}, value)
    service = config["services"]["qs-ai"]
    options = MessagingOptions.model_validate_json(service["environment"]["QS_AI_MESSAGING"])
    owner = options.transactions["ai.mysql"]
    assert owner.store_id == "Original-AI-Primary"
    assert owner.writer_epoch == "Original-Writer-Epoch"
    assert owner.route_revision == "m3-1"
    assert (owner.limits.sends, owner.limits.check_workers, owner.limits.check_queue) == (4, 1, 16)
    assert options.max_in_flight == 1 and not options.publish_enabled
    assert len(service["volumes"]) == 7
    assert all(m["read_only"] and not m["bind"]["create_host_path"] for m in service["volumes"])
    assert load("deploy/serverA/deploy.py").messaging_release(tmp_path)
    assert "password" not in json.dumps(config)


@pytest.mark.parametrize(
    "field,value",
    [
        ("store_id", " "),
        ("writer_epoch", "x" * 129),
        ("route_revision", "m4-unsafe"),
        ("store_id", False),
        ("limits", {}),
    ],
)
def test_transaction_identity_or_incomplete_budget_rejected_without_contents(field, value):
    original = transaction_binding()
    original["transactions"]["ai.mysql"][field] = value
    with pytest.raises(ValueError, match="contents withheld"):
        runtime(original)


@pytest.mark.parametrize(
    "field,value",
    [
        ("sends", 8),
        ("check_workers", 2),
        ("check_queue", 32),
        ("branches", 32),
        ("rpc_timeout", 10),
        ("lease", True),
        ("check_timeout", float("nan")),
    ],
)
def test_deployment_cannot_expand_frozen_ai_budget(field, value):
    original = transaction_binding()
    original["transactions"]["ai.mysql"]["limits"][field] = value
    with pytest.raises(ValueError, match="contents withheld"):
        runtime(original)


def test_wrong_owner_and_unknown_secret_field_are_rejected():
    for transactions in [
        {"qs.mysql": transaction_binding()["transactions"]["ai.mysql"]},
        {
            "ai.mysql": {
                **transaction_binding()["transactions"]["ai.mysql"],
                "secret": "DO_NOT_DISCLOSE",
            }
        },
    ]:
        value = transaction_binding()
        value["transactions"] = transactions
        with pytest.raises(ValueError, match="contents withheld") as error:
            runtime(value)
        assert "DO_NOT_DISCLOSE" not in str(error.value)


def test_host_revalidates_budget_even_if_a_manifest_digest_is_recomputed(tmp_path):
    module = load("scripts/cd/deploy.py")
    config = runtime(transaction_binding())
    service = config["services"]["qs-ai"]
    value = json.loads(service["environment"]["QS_AI_MESSAGING"])
    value["transactions"]["ai.mysql"]["limits"]["check_workers"] = 2
    service["environment"]["QS_AI_MESSAGING"] = json.dumps(value, sort_keys=True)
    (tmp_path / "runtime.json").write_text(json.dumps(config))
    (tmp_path / "manifest.json").write_text(
        json.dumps({"messaging_binding_sha256": module.messaging_binding_digest(config)})
    )
    remote = load("deploy/serverA/deploy.py")
    with pytest.raises(remote.DeploymentError, match="contents withheld"):
        remote.messaging_release(tmp_path)


def test_host_checks_exact_ca_and_one_revision_for_all_seven_mounts(tmp_path):
    config = freeze(tmp_path, {}, transaction_binding())
    for change in ["ca", "revision", "writable"]:
        altered = copy.deepcopy(config)
        service = altered["services"]["qs-ai"]
        if change == "ca":
            service["environment"]["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] = "/unreviewed.pem"
        elif change == "revision":
            service["volumes"][-1]["source"] = service["volumes"][-1]["source"].replace(
                "/m4-final-ai/", "/other/"
            )
        else:
            service["volumes"][-1]["read_only"] = False
        (tmp_path / "runtime.json").write_text(json.dumps(altered))
        manifest = {
            "messaging_binding_sha256": load("scripts/cd/deploy.py").messaging_binding_digest(
                altered
            )
        }
        remote = load("deploy/serverA/deploy.py")
        with pytest.raises(remote.DeploymentError, match="contents withheld"):
            remote.messaging_release(tmp_path, manifest)


def test_original_identity_dollar_bytes_are_only_compose_escaped(tmp_path):
    value = transaction_binding()
    value["transactions"]["ai.mysql"]["store_id"] = "Original-$Primary"
    value["transactions"]["ai.mysql"]["writer_epoch"] = "Writer-$$-Epoch"
    config = freeze(tmp_path, {}, value)
    raw = config["services"]["qs-ai"]["environment"]["QS_AI_MESSAGING"]
    actual = MessagingOptions.model_validate_json(raw.replace("$$", "$"))
    assert actual.transactions["ai.mysql"].store_id == "Original-$Primary"
    assert actual.transactions["ai.mysql"].writer_epoch == "Writer-$$-Epoch"
    assert load("deploy/serverA/deploy.py").messaging_release(tmp_path)
