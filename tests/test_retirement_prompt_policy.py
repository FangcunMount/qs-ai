import json

import pytest

from scripts.retirement.prompt_policy import TEMPLATE_ID, candidate, unreferenced


def asset(version="v1"):
    return {
        "template_id": TEMPLATE_ID,
        "version": version,
        "fingerprint": "sha256:" + version + "-fingerprint",
        "package_sha256": version + "-package-digest",
        "package_json": json.dumps({"Ref": {"TemplateID": TEMPLATE_ID, "Version": version}}),
    }


def test_only_exact_five_versions_are_candidates():
    assert all(candidate(asset("v" + str(i))) for i in range(1, 6))
    assert not candidate(asset("v6"))
    assert not candidate(asset("v1-debug"))
    assert not candidate({**asset(), "template_id": "other"})


@pytest.mark.parametrize(
    "table",
    [
        "profile_assets",
        "evaluation_suites",
        "configuration_publications",
        "evaluation_runs",
        "interpretation_sessions",
        "execution_configurations",
        "unknown_evidence",
    ],
)
@pytest.mark.parametrize(
    "kind", ["identity", "fingerprint", "package", "nested_json", "unversioned"]
)
def test_every_reference_preserves_candidate(table, kind):
    row = asset()
    ref = {"identity": TEMPLATE_ID, "version": "v1"}
    value = {
        "identity": ref,
        "fingerprint": row["fingerprint"],
        "package": row["package_sha256"],
        "nested_json": json.dumps({"snapshot": json.dumps(ref)}),
        "unversioned": TEMPLATE_ID,
    }[kind]
    assert not unreferenced([row], {"prompt_assets": [row], table: [{"proof": value}]})


def test_current_version_does_not_keep_unreferenced_history():
    old, current = asset(), asset("v6")
    data = {
        "prompt_assets": [old, current],
        "profile_assets": [{"prompt": {"id": TEMPLATE_ID, "version": "v6"}}],
    }
    assert unreferenced(data["prompt_assets"], data) == {(TEMPLATE_ID, "v1")}


def test_incomplete_asset_identity_is_retained():
    row = {**asset(), "fingerprint": ""}
    assert not unreferenced([row], {"prompt_assets": [row]})
