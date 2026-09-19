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


def test_real_v6_profile_does_not_retain_v1_to_v5():
    from pathlib import Path

    baseline = json.loads(
        Path("integrations/qs_server/prompts/published-profile-baseline.json").read_text()
    )
    rows = [asset("v" + str(i)) for i in range(1, 7)]
    data = {"prompt_assets": rows, "profile_assets": [{"definition_json": json.dumps(baseline)}]}
    assert unreferenced(rows, data) == {(TEMPLATE_ID, "v" + str(i)) for i in range(1, 6)}


@pytest.mark.parametrize("version", [None, "", 6, ["v6"]])
def test_unknown_version_cannot_prove_unreferenced(version):
    row = asset()
    data = {"profile_assets": [{"prompt_template_id": TEMPLATE_ID, "prompt_version": version}]}
    assert not unreferenced([row], data)


def test_unrelated_version_does_not_qualify_prompt_reference():
    row = asset()
    assert not unreferenced(
        [row], {"profile_assets": [{"prompt_template_id": TEMPLATE_ID, "version": "v6"}]}
    )


def test_exact_v6_reference_does_not_hide_other_old_or_unknown_references():
    row = asset()
    for other in [
        row["fingerprint"],
        row["package_sha256"],
        TEMPLATE_ID,
        {"identity": TEMPLATE_ID, "version": "v1"},
    ]:
        data = {
            "evidence": [
                {"prompt_template_id": TEMPLATE_ID, "prompt_version": "v6", "other": other}
            ]
        }
        assert not unreferenced([row], data)


def test_explicit_old_profile_reference_is_retained():
    row = asset()
    assert not unreferenced(
        [row], {"profile_assets": [{"prompt_template_id": TEMPLATE_ID, "prompt_version": "v1"}]}
    )


@pytest.mark.parametrize("field", ["suite_id", "id", "identity"])
def test_known_published_suite_identifier_is_not_a_prompt_reference(field):
    row = asset()
    value = {field: TEMPLATE_ID + "-v6-published", "version": "1"}
    assert unreferenced([row], {"evaluation_suites": [value]}) == {(TEMPLATE_ID, "v1")}
    value["proof"] = {"template_id": TEMPLATE_ID, "version": "v1"}
    assert not unreferenced([row], {"evaluation_suites": [value]})


def test_unknown_suite_names_or_unstructured_mentions_still_retain():
    row = asset()
    for value in [{"suite_id": TEMPLATE_ID + "-unknown"}, {"notes": TEMPLATE_ID + "-v6-published"}]:
        assert not unreferenced([row], {"evaluation_suites": [value]})
