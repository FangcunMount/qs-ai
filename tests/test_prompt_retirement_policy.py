import json

import pytest

from qs_ai.maintenance.prompt_retirement import policy
from scripts.retirement import prompt_policy as legacy


def asset(version="v1"):
    return {
        "template_id": policy.TEMPLATE_ID,
        "version": version,
        "fingerprint": "sha256:" + version,
        "package_sha256": "package-" + version,
    }


def test_historical_imports_are_the_same_policy_objects():
    for name in legacy.__all__:
        assert getattr(legacy, name) is getattr(policy, name)


@pytest.mark.parametrize("field", ["definition_json", "markdown", "snapshot_json", "evidence_json"])
def test_projected_body_references_are_conservative(field):
    row = asset()
    assert policy.references(
        {field: json.dumps({"id": policy.TEMPLATE_ID, "version": "v1"}).encode()}, row
    )
    assert not policy.references(
        {field: json.dumps({"id": policy.TEMPLATE_ID, "version": "v6"}).encode()}, row
    )
    assert policy.references({field: b"malformed:" + policy.TEMPLATE_ID.encode()}, row)


def test_only_the_exact_own_prompt_is_excluded():
    first, second = asset(), asset("v2")
    assert policy.unreferenced([first, second], {"prompt_assets": [first, second]}) == {
        (policy.TEMPLATE_ID, "v1"),
        (policy.TEMPLATE_ID, "v2"),
    }
    second["package_json"] = json.dumps({"template_id": policy.TEMPLATE_ID, "version": "v1"})
    assert policy.unreferenced([first, second], {"prompt_assets": [first, second]}) == {
        (policy.TEMPLATE_ID, "v2")
    }
