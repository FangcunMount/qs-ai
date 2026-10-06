"""Pinned three-topic assets and evaluation plumbing; no model quality approval."""

import hashlib
import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from itertools import product
from unittest.mock import AsyncMock

import pytest

from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import frozen_output_schema
from qs_ai.infrastructure.qs_server.candidate_assertions import evaluate_candidate_assertions
from qs_ai.infrastructure.qs_server.evaluation_assertions import assertion_inventory
from qs_ai.infrastructure.qs_server.evaluation_case import prepare_asset_evaluation_case
from qs_ai.infrastructure.qs_server.evaluation_policies import evaluation_directory
from qs_ai.infrastructure.qs_server.mbti_assets import load_mbti_root, load_mbti_themes_root
from qs_ai.infrastructure.qs_server.preflight import run_preflight
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets, semantic_assets
from qs_ai.infrastructure.qs_server.semantic_input import prepare_semantic_messages
from tests.test_generation_completion import completion
from tests.test_mbti_themes_output import output

THEMES = {
    "three_topics_substantive",
    "reference_claims_supported",
    "facts_and_references_distinct",
    "career_exploration_only",
    "relationships_communication_only",
    "questions_and_actions_specific",
}


def setup_case():
    root = load_mbti_themes_root()
    release = decode_published_profile(
        {
            "definition": json.loads(root.profile.definition_json),
            "fingerprint": root.profile.fingerprint,
            "status": "published",  # decoding only, never activation
        }
    )
    prepared = prepare_asset_evaluation_case(
        root.release,
        root.suite.generation_case_ids[0],
        release,
        executable_prompt(root.prompt),
        frozen_suite=root.suite,
    )
    return root, prepared, json.loads(root.output_schema.definition_json)


def assertions(value, root, prepared, schema):
    return evaluate_candidate_assertions(
        json.dumps(value, ensure_ascii=False).encode(),
        prepared,
        root.suite.reference,
        root.suite.generation_case_ids[0],
        frozen_suite=root.suite,
        output_schema=schema,
    )


def test_new_root_preserves_plan_facts_routes_budget_and_old_root_bytes():
    root, prepared, _ = setup_case()
    old = load_mbti_root()
    before = json.loads(old.suite.definition_json)
    after = json.loads(root.suite.definition_json)
    assert len(root.suite.slots()) == 35
    assert root.suite.input_construction_version == "qs-published-snapshot-v3"
    assert root.release.generation_route == old.release.generation_route
    assert root.release.semantic_route == old.release.semantic_route
    assert root.contracts.execution_policy == old.contracts.execution_policy
    assert root.contracts.gate_policy == old.contracts.gate_policy
    assert after["execution_policy"] == before["execution_policy"]
    for a, b in zip(after["cases"], before["cases"], strict=True):
        assert a["provider_payload"]["facts"] == b["provider_payload"]["facts"]
        assert a["provider_payload"]["context"] == b["provider_payload"]["context"]
    assert (
        run_preflight(root.suite.reference, datetime.now(UTC), frozen_suite=root.suite).status
        == "passed"
    )
    assert "{{" not in prepared.messages.task_message
    assert "integrated_insights" not in prepared.messages.task_message
    assert set(json.loads(prepared.messages.data_json)) == {
        "context",
        "facts",
        "reference_material",
    }
    assert "source" not in json.loads(prepared.messages.data_json)
    for case in root.suite.generation_case_ids:
        inventory = assertion_inventory(root.suite.reference, case, frozen_suite=root.suite)
        assert THEMES <= {a.type for a in inventory if a.hard}
        assert not {
            "not_parallel_dimension_summary",
            "each_insight_has_distinct_dimension_refs",
        } & {a.type for a in inventory}
    assert (
        old.suite.reference.fingerprint
        == "sha256:3353f945b75346b869c638a1034356ac602ea7f049cb0d2a6e55262538b9cacd"
    )


@pytest.mark.parametrize("poles", list(product("EI", "SN", "TF", "JP")))
def test_finite_material_covers_every_type_with_twelve_scoped_entries(poles):
    root, prepared, _ = setup_case()
    material = prepared.release.input_policy.reference_material
    selected = material.select("".join(poles))
    assert len(material.entries) == 24
    assert len(material.sources) == 4
    assert len(selected.entries) == 12
    assert {e.topic for e in selected.entries} == {"personality", "career", "relationships"}
    assert all(e.pole == poles[("EI", "SN", "TF", "JP").index(e.axis)] for e in selected.entries)
    assert "approved" not in root.profile.definition_json
    assert "v64-report-202608-v1" == material.model_version


@pytest.mark.parametrize(
    "name",
    [
        "manifest.json",
        "profile-v1.json",
        "reference-material-v1.json",
        "prompt-v1.md",
        "prompt-v1.json",
        "semantic-v1.md",
        "suite-v1.json",
    ],
)
def test_changed_initialization_bytes_rejected_without_fallback(tmp_path, name):
    target = tmp_path / "root"
    shutil.copytree(evaluation_directory() / "mbti-themes", target)
    path = target / name
    path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError):
        load_mbti_themes_root(target)


def test_deterministic_checks_leave_all_six_content_obligations_for_independent_judge():
    root, prepared, schema = setup_case()
    receipts = assertions(output(), root, prepared, schema)
    statuses = {a.type: a.status for a in receipts}
    assert statuses["output_schema_valid"] == statuses["all_references_resolve"] == "passed"
    assert statuses["profile_output_policy_satisfied"] == "passed"
    assert all(statuses[t] == "pending_semantic" for t in THEMES)
    value = output()
    value["sections"][1]["insights"][0]["reference_refs"] = ["reference:personality.ei.i"]
    statuses = {a.type: a.status for a in assertions(value, root, prepared, schema)}
    assert statuses["all_references_resolve"] == "failed"
    assert statuses["profile_output_policy_satisfied"] == "blocked"
    with pytest.raises(ValueError, match="Frozen output schema required"):
        evaluate_candidate_assertions(
            b"{}",
            prepared,
            root.suite.reference,
            root.suite.generation_case_ids[0],
            frozen_suite=root.suite,
        )


def test_generic_length_and_literal_checks_support_new_sections():
    root, prepared, schema = setup_case()
    document = json.loads(root.suite.definition_json)
    document["default_generation_assertions"] += [
        {"type": "forbid_literal_substrings", "values": ["synthetic-injection"]},
        {"type": "output_character_limit", "maximum": 100},
    ]
    raw = json.dumps(document)
    reference = replace(
        root.suite.reference, fingerprint="sha256:" + hashlib.sha256(raw.encode()).hexdigest()
    )
    root = replace(root, suite=replace(root.suite, reference=reference, definition_json=raw))
    value = output()
    value["summary"]["content"] += " synthetic-injection"
    receipts = assertions(value, root, prepared, schema)
    assert any(a.type == "forbid_literal_substrings" and a.status == "failed" for a in receipts)
    assert any(a.type == "output_character_limit" and a.status == "failed" for a in receipts)


def test_raw_formatting_cannot_bypass_profile_or_case_character_limits():
    from qs_ai.application.interpretation.output import InvalidOutput, validate_output
    from qs_ai.infrastructure.qs_server.output import QSOutputParser

    root, prepared, schema = setup_case()
    limit = prepared.release.render_policy.max_output_characters
    compact = json.dumps(output(), ensure_ascii=False, separators=(",", ":"))
    assert len(compact) < limit
    raw = compact + "\n" * (limit + 1 - len(compact))
    document = json.loads(root.suite.definition_json)
    document["default_generation_assertions"].append(
        {"type": "output_character_limit", "maximum": limit}
    )
    definition = json.dumps(document)
    reference = replace(
        root.suite.reference,
        fingerprint="sha256:" + hashlib.sha256(definition.encode()).hexdigest(),
    )
    suite = replace(root.suite, reference=reference, definition_json=definition)
    receipts = evaluate_candidate_assertions(
        raw.encode(),
        prepared,
        reference,
        root.suite.generation_case_ids[0],
        frozen_suite=suite,
        output_schema=schema,
    )
    statuses = {a.type: a.status for a in receipts}
    assert statuses["output_schema_valid"] == "passed"
    assert statuses["profile_output_policy_satisfied"] == "failed"
    assert statuses["output_character_limit"] == "failed"
    with pytest.raises(InvalidOutput, match="output_too_long"):
        validate_output(raw, prepared, QSOutputParser.from_schema(schema))


def test_semantic_input_retains_new_output_references_and_exact_obligation_identities():
    root, prepared, schema = setup_case()
    raw = json.dumps(output(), ensure_ascii=False).encode()
    generation = replace(
        completion(),
        case_id=root.suite.generation_case_ids[0],
        normalized_output=raw,
        raw_output=raw,
        normalized_fingerprint="sha256:" + hashlib.sha256(raw).hexdigest(),
    )
    receipts = assertions(output(), root, prepared, schema)
    shared = load_semantic_assets()
    assets = semantic_assets(
        root.semantic.markdown,
        shared.output_schema_json,
        root.semantic.reference,
        shared.output_schema,
    )
    messages = prepare_semantic_messages(
        root.release,
        generation,
        receipts,
        prepared=prepared,
        assets=assets,
        frozen_suite=root.suite,
        output_schema=schema,
    )
    payload = json.loads(messages.data_json)
    assert payload["candidate_output"] == output()
    assert payload["assessment_input"] == json.loads(prepared.messages.data_json)
    assert THEMES <= {a["type"] for a in payload["assertions"] if a["hard"]}
    assert "本场景 cross_dimension_quality" in messages.task_message
    with pytest.raises(ValueError, match="Frozen output schema required"):
        prepare_semantic_messages(
            root.release,
            generation,
            receipts,
            prepared=prepared,
            assets=assets,
            frozen_suite=root.suite,
        )


@pytest.mark.asyncio
async def test_schema_resolution_requires_original_immutable_bytes():
    root, _, schema = setup_case()
    store = AsyncMock(get=AsyncMock(return_value=root.output_schema))
    assert await frozen_output_schema(root.release, store) == schema
    store.get.assert_awaited_once_with("ai-explanation-output", "v2")
    store.get.return_value = replace(
        root.output_schema,
        definition_json=root.output_schema.definition_json + " ",
        fingerprint="sha256:"
        + hashlib.sha256((root.output_schema.definition_json + " ").encode()).hexdigest(),
    )
    with pytest.raises(ValueError, match="Frozen output schema unavailable"):
        await frozen_output_schema(root.release, store)
    store.get.return_value = None
    with pytest.raises(ValueError, match="Frozen output schema unavailable"):
        await frozen_output_schema(root.release, store)
