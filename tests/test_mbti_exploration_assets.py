"""Pinned exploratory initialization reuses messages, never base quality approval."""

import hashlib
import json
import shutil
from datetime import UTC, datetime

import pytest

from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.infrastructure.qs_server.evaluation_case import prepare_asset_evaluation_case
from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_input
from qs_ai.infrastructure.qs_server.evaluation_policies import evaluation_directory
from qs_ai.infrastructure.qs_server.evaluation_suite import canonical
from qs_ai.infrastructure.qs_server.mbti_assets import (
    load_mbti_exploration_root,
    load_mbti_themes_root,
)
from qs_ai.infrastructure.qs_server.preflight import run_preflight
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile


def test_exploration_root_has_independent_complete_plan_and_r17_source_proof():
    root = load_mbti_exploration_root()
    base = load_mbti_themes_root()
    assert root.profile.profile_id != base.profile.profile_id
    assert root.suite.input_construction_version == "qs-published-snapshot-v4"
    assert len(root.suite.slots()) == 35
    assert (
        run_preflight(root.suite.reference, datetime.now(UTC), frozen_suite=root.suite).status
        == "passed"
    )
    document = json.loads(root.suite.definition_json)
    definition = json.loads(root.profile.definition_json)
    profile = decode_published_profile(
        {"definition": definition, "status": "published", "fingerprint": root.profile.fingerprint}
    )
    for case in root.suite.generation_case_ids:
        prepared = prepare_asset_evaluation_case(
            root.release, case, profile, executable_prompt(root.prompt), frozen_suite=root.suite
        )
        facts = json.loads(prepared.assembled_input.canonical_json)["facts"]
        assert facts["model"]["code"] == "MBTI_FC_93"
        assert "{{" not in prepared.messages.task_message
        assert [d["raw_score"]["max"] for d in facts["dimensions"]] == [23, 23, 23, 24]
    proof = json.loads(
        (evaluation_directory() / "mbti-exploration/source-proof-v1.json").read_text()
    )
    package = json.loads(root.prompt.package_json)
    messages = {k: package[k] for k in ("SystemMessage", "TaskTemplate", "DataPreamble")}
    assert (
        proof["message_sha256"]
        == "sha256:" + hashlib.sha256(canonical(messages).encode()).hexdigest()
    )
    assert proof["candidate_evidence_reused"] is False
    assert proof["synthetic_cases_only"] is True
    assert root.release.generation_route == type(root.release.generation_route)(
        **proof["source_release"]["generation_route"]
    )
    assert root.release.semantic_route == type(root.release.semantic_route)(
        **proof["source_release"]["semantic_route"]
    )
    assert root.release.execution_policy == base.release.execution_policy
    assert root.release.gate_policy == base.release.gate_policy
    assert (
        document["execution_policy"] == json.loads(base.suite.definition_json)["execution_policy"]
    )


@pytest.mark.parametrize(
    "name",
    [
        "manifest.json",
        "profile-v1.json",
        "prompt-v1.md",
        "prompt-v1.json",
        "semantic-v1.md",
        "source-proof-v1.json",
        "reference-material-v1.json",
        "suite-v1.json",
    ],
)
def test_exploration_initializer_rejects_damage_without_current_file_fallback(tmp_path, name):
    shutil.copytree(evaluation_directory() / "mbti-exploration", tmp_path / "root")
    path = tmp_path / "root" / name
    path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError):
        load_mbti_exploration_root(tmp_path / "root")


@pytest.mark.parametrize("change", ["axis max", "reference", "model", "orientation"])
def test_exploration_case_validation_rejects_mixed_contract(change):
    root = load_mbti_exploration_root()
    payload = json.loads(root.suite.definition_json)["cases"][0]["provider_payload"]
    if change == "axis max":
        payload["facts"]["dimensions"][0]["raw_score"]["max"] = 24
    elif change == "reference":
        payload["reference_material"]["model_code"] = "MBTI_OEJTS"
    elif change == "model":
        payload["facts"]["model"]["version"] = "v55"
    else:
        payload["facts"]["dimensions"][0]["pole_facts"].update(left_pole="I", right_pole="E")
    with pytest.raises(ValueError):
        validate_suite_input(root.suite, root.release.input_schema, payload)
