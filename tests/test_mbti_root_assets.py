"""New MBTI root bytes are independent, complete, scoped and unapproved."""

import hashlib
import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.qs_server.evaluation_case import prepare_asset_evaluation_case
from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_input
from qs_ai.infrastructure.qs_server.evaluation_policies import evaluation_directory
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    MBTI_ROOT,
    V6_PUBLISHED,
    derive_suite,
    load_suite,
)
from qs_ai.infrastructure.qs_server.mbti_assets import load_mbti_root
from qs_ai.infrastructure.qs_server.preflight import run_preflight
from qs_ai.infrastructure.qs_server.profiles import canonical_definition, decode_published_profile
from tests.test_generation_manifest import assets as assets


def test_mbti_root_preserves_full_plan_without_scale_source_or_fake_report():
    root = load_mbti_root()
    suite = root.suite
    document = json.loads(suite.definition_json)
    assert "derived_from" not in document
    assert suite.reference != V6_PUBLISHED
    assert len(suite.slots()) == 35
    assert run_preflight(suite.reference, datetime.now(UTC), frozen_suite=suite).status == "passed"
    assert suite.input_construction_version == "qs-published-snapshot-v2"
    profile = decode_published_profile(
        {
            "definition": json.loads(root.profile.definition_json),
            "fingerprint": root.profile.fingerprint,
            "status": "published",
        }
    )
    for case in suite.generation_case_ids:
        prepared = prepare_asset_evaluation_case(
            root.release, case, profile, executable_prompt(root.prompt), frozen_suite=suite
        )
        assert prepared.messages.data_json
        assert set(json.loads(prepared.assembled_input.canonical_json)) == {"context", "facts"}
        assert "{{" not in prepared.messages.task_message
    assert root.release.semantic_prompt.id == "mbti-single-semantic-evaluator"
    assert root.release.generation_route.id == "balanced_text_v1"
    assert root.release.semantic_route.id == "semantic_judge_v1"


@pytest.mark.parametrize(
    "name",
    [
        "prompt-v1.json",
        "prompt-v1.md",
        "profile-v1.json",
        "suite-v1.json",
        "semantic-v1.md",
        "manifest.json",
    ],
)
def test_root_asset_damage_never_falls_back(tmp_path, name):
    shutil.copytree(evaluation_directory() / "mbti", tmp_path / "mbti")
    path = tmp_path / "mbti" / name
    path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError):
        load_mbti_root(tmp_path / "mbti")


@pytest.mark.parametrize(
    "damage", ["type", "duplicate_axis", "preference", "model", "range", "suggestion"]
)
def test_case_revision_cannot_bypass_personality_fact_contract(damage):
    root = load_mbti_root()
    payload = json.loads(root.suite.definition_json)["cases"][0]["provider_payload"]
    facts = payload["facts"]
    if damage == "type":
        facts["model_result"]["type_code"] = "ENTP"
    elif damage == "duplicate_axis":
        facts["dimensions"][1] = facts["dimensions"][0]
    elif damage == "preference":
        facts["dimensions"][0]["pole_facts"]["preference"] = "S"
    elif damage == "model":
        facts["model"]["version"] = "latest"
    elif damage == "range":
        facts["dimensions"][0]["pole_facts"]["threshold"] = 20
    else:
        facts["dimensions"][0]["standard_suggestion_refs"] = ["suggestion:absent"]
    with pytest.raises(ValueError):
        validate_suite_input(root.suite, root.release.input_schema, payload)


async def test_mbti_derivation_keeps_source_contract_and_rejects_scale_root(assets):
    from qs_ai.application.interpretation.manifest import build_generation_manifest
    from qs_ai.domain.governance.schema import SchemaAsset
    from qs_ai.infrastructure.qs_server.output import schema_directory

    root = load_mbti_root()
    stores, selection = assets
    d = json.loads(root.profile.definition_json)
    d["version"] = "derived-test-v1"
    raw = canonical_definition(d)
    profile = ProfileAsset(
        root.profile.profile_id,
        d["version"],
        "sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
        raw,
    )
    stores[0].get.return_value = profile
    stores[1].get.return_value = root.prompt
    output = (schema_directory() / "ai-explanation-output-v1.schema.json").read_text()
    schemas = [
        root.input_schema,
        SchemaAsset(
            "ai-explanation-output",
            "v1",
            "sha256:" + hashlib.sha256(output.encode()).hexdigest(),
            output,
        ),
    ]
    stores[3].get.side_effect = lambda identity, version: next(
        (a for a in schemas if (a.schema_id, a.version) == (identity, version)), None
    )
    selection.update(profile_id=profile.profile_id, profile_version=profile.version)
    manifest = await build_generation_manifest(*stores, **selection)
    derived = derive_suite("mbti-derived-test", "v1", profile, manifest, source=root.suite)
    assert derived.slots() == root.suite.slots()
    assert derived.manifest == manifest
    with pytest.raises(ValueError):
        derive_suite(
            "mbti-invalid-source", "v1", profile, manifest, source=load_suite(V6_PUBLISHED)
        )
    with pytest.raises(ValueError):
        load_suite(
            replace(MBTI_ROOT, version="unknown"), definition_json=root.suite.definition_json
        )
