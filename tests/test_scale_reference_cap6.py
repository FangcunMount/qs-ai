"""Cap6 is a new governed configuration, never an override of a frozen v6 request."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from qs_ai.application.governance.prompt_freeze import freeze_asset
from qs_ai.application.interpretation.input import assemble_input
from qs_ai.application.interpretation.output import InvalidOutput, validate_output
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.application.interpretation.safety import check_safety
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt_draft import PromptDraft, PromptDraftContent
from qs_ai.infrastructure.qs_server.deepseek_request import build_request
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.profiles import (
    canonical_definition,
    decode_profile_definition,
    decode_published_profile,
    load_migrated_release,
)
from tests.test_deepseek_request import route
from tests.test_generation_manifest import assets as assets

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE = ROOT / "integrations/qs_server/evaluation/scale-reference-cap6"
DISCLAIMER = "本解读仅基于本次测评结果，不构成诊断或确定性判断，也不能据此确认维度间因果关系。"


def candidate_release():
    definition = decode_profile_definition(json.loads((CANDIDATE / "profile-v7.json").read_text()))
    raw = canonical_definition(definition.model_dump())
    fingerprint = "sha256:" + hashlib.sha256(raw.encode()).hexdigest()
    asset = ProfileAsset(definition.profile_id, definition.version, fingerprint, raw)
    return decode_published_profile(
        {
            "definition": json.loads(asset.definition_json),
            "fingerprint": asset.fingerprint,
            "status": "published",
        }
    )


def candidate_prompt_asset():
    document = json.loads((CANDIDATE / "prompt-v7-draft.json").read_text())
    content = document["content"]
    draft = PromptDraft(
        UUID("00000000-0000-4000-8000-000000000001"),
        1,
        document["template_id"],
        document["target_version"],
        AssetReference(**document["source"]),
        1,
        PromptDraftContent(
            **{**content, "allowed_placeholders": tuple(content["allowed_placeholders"])}
        ),
        UUID("00000000-0000-4000-8000-000000000002"),
        1,
        document["reason"],
        datetime(2026, 10, 12, tzinfo=UTC),
    )
    checksum = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()
    return freeze_asset(draft, checksum)


def candidate_prompt():
    return executable_prompt(candidate_prompt_asset())


def prepared():
    snapshot = json.loads((ROOT / "tests/fixtures/report_snapshot.json").read_text())
    dimension = snapshot["dimensions"][0]
    snapshot["dimensions"] = [
        {
            **deepcopy(dimension),
            "code": f"D{i}",
            "name": f"观察维度{i}",
            "parent_code": "",
            "role": "primary",
            "hierarchy_level": 1,
            "sort_order": i,
            "description": "本次处于稳定范围。",
            "suggestion": "",
        }
        for i in range(1, 8)
    ]
    snapshot["suggestions"] = []
    release, package = candidate_release(), candidate_prompt()
    assembled = assemble_input(json.dumps(snapshot, ensure_ascii=False), release.input_policy)
    return PreparedExplanation(
        assembled,
        render_prompt(package, release.render_policy, assembled.provider_payload),
        release,
        package.fingerprint,
    )


def output(count):
    return {
        "schema_version": "ai-explanation-output/v1",
        "summary": "本次多个维度均处于稳定范围，可以分别观察。",
        "integrated_insights": [
            {
                "kind": "reinforcing_pattern",
                "title": "多维度联合观察",
                "content": "本次所引用维度均显示稳定范围，分别保留各自的观察对象。",
                "why_it_matters": "这些结果可以共同阅读，各维度仍需分别观察。",
                "evidence_refs": [
                    {"kind": "dimension", "ref": f"dimension:D{i}"} for i in range(1, count + 1)
                ],
            }
        ],
        "suggestions": [
            {
                "origin": "generated_low_risk",
                "category": "self_observation",
                "title": "分别记录",
                "goal": "分别记录各维度的观察",
                "actions": ["自愿分别记录一次日常观察。"],
                "rationale": "对应本次稳定结果。",
                "evidence_refs": [{"kind": "dimension", "ref": "dimension:D1"}],
                "source_suggestion_refs": [],
                "caution": "",
            }
        ],
        "limitations": [DISCLAIMER],
    }


def validate(document, source=None):
    return check_safety(
        validate_output(
            json.dumps(document, ensure_ascii=False), source or prepared(), QSOutputParser()
        )
    )


def test_new_profile_changes_only_version_prompt_and_dimension_cap():
    baseline = json.loads(
        load_migrated_release("participant-scale-score-range-default", "v6").definition_json
    )
    expected = deepcopy(baseline)
    expected["version"] = "v7"
    expected["insight_policy"]["max_dimension_refs_per_item"] = 6
    expected["generation_policy"]["prompt_version"] = "v7"
    assert json.loads(candidate_release().definition_json) == expected


@pytest.mark.parametrize("count", [2, 3, 4, 5, 6])
def test_legal_same_level_dimensions_are_accepted_without_trimming(count):
    document = output(count)
    assert json.loads(validate(document).content_json) == document


@pytest.mark.parametrize("counts", [(2, 2, 4), (2, 4)])
def test_original_failure_shapes_remain_invalid_under_frozen_v6(counts):
    document = output(2)
    document["integrated_insights"] = [output(n)["integrated_insights"][0] for n in counts]
    source = prepared()
    old = replace(
        source, release=load_migrated_release("participant-scale-score-range-default", "v6")
    )
    with pytest.raises(InvalidOutput, match="dimension_count_outside_policy"):
        validate(document, old)
    assert json.loads(validate(document, source).content_json) == document


@pytest.mark.parametrize("count", [1, 7])
def test_outside_current_contract_stays_rejected(count):
    with pytest.raises(InvalidOutput):
        validate(output(count))


@pytest.mark.parametrize("mutation", ["unknown", "duplicate", "parent", "causal", "source"])
def test_wider_cap_does_not_weaken_evidence_hierarchy_or_safety(mutation):
    document, source = output(4), prepared()
    insight = document["integrated_insights"][0]
    if mutation == "unknown":
        insight["evidence_refs"][-1]["ref"] = "dimension:unknown"
    elif mutation == "duplicate":
        insight["evidence_refs"][-1] = insight["evidence_refs"][0]
    elif mutation == "parent":
        payload = json.loads(source.assembled_input.provider_payload)
        payload["facts"]["dimensions"][1]["parent_ref"] = "dimension:D1"
        source = replace(
            source,
            assembled_input=replace(source.assembled_input, provider_payload=json.dumps(payload)),
        )
    elif mutation == "causal":
        insight["content"] = "这导致了另一维度的表现。"
    else:
        document["suggestions"][0]["origin"] = "standard_derived"
        document["suggestions"][0]["source_suggestion_refs"] = ["suggestion:unknown"]
    with pytest.raises(InvalidOutput):
        validate(document, source)


@pytest.mark.parametrize("count,allowed", [(4, True), (5, True), (6, False)])
def test_schema_limits_total_evidence_including_supporting_refs(count, allowed):
    document = output(count)
    document["integrated_insights"][0]["evidence_refs"].append(
        {"kind": "overall_result", "ref": "overall_result"}
    )
    if allowed:
        assert json.loads(validate(document).content_json) == document
    else:
        with pytest.raises(InvalidOutput, match="output_schema_invalid"):
            validate(document)


def test_prompt_freezes_through_existing_governance_and_reaches_provider():
    source = prepared()
    assert "2 到 3" not in source.messages.system_message
    assert "2 到 6" in source.messages.system_message
    assert "distinct dimension refs per item: 2..6" in source.messages.task_message
    request = build_request(source, route(), QSOutputParser().schema())
    assert request["instructions"] == source.messages.system_message
    assert request["input"][0]["content"][0]["text"] == source.messages.task_message
    assert "dimension_ref_1" not in json.dumps(request)
    assert request["model"] == "deepseek-v4-pro"


def test_cap6_root_retains_hard_safety_cases_and_has_no_stale_count_assertion():
    from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_input
    from qs_ai.infrastructure.qs_server.evaluation_suite import CAP6_ROOT, V6_PUBLISHED, load_suite

    suite, old = load_suite(CAP6_ROOT), load_suite(V6_PUBLISHED)
    document, baseline = json.loads(suite.definition_json), json.loads(old.definition_json)
    assert len(suite.slots()) == 35
    assert suite.reference != old.reference
    assert document["cases"][3:] == baseline["cases"][3:]
    expected = deepcopy(baseline["default_generation_assertions"])
    for item in expected:
        if item["type"] == "each_insight_has_distinct_dimension_refs":
            item["maximum"] = 6
    assert document["default_generation_assertions"] == expected
    for case in document["cases"]:
        if case["stage"] == "generation":
            validate_suite_input(suite, suite.input_schema, case["provider_payload"])
    assert [len(c["provider_payload"]["facts"]["dimensions"]) for c in document["cases"][:3]] == [
        4,
        5,
        6,
    ]


@pytest.mark.parametrize("index,count", [(0, 4), (1, 5), (2, 6)])
def test_cap6_root_assertions_accept_complete_multidimension_group(index, count):
    from qs_ai.infrastructure.qs_server.candidate_assertions import evaluate_candidate_assertions
    from qs_ai.infrastructure.qs_server.evaluation_suite import CAP6_ROOT, load_suite

    suite = load_suite(CAP6_ROOT)
    document = json.loads(suite.definition_json)
    case = document["cases"][index]
    source = prepared()
    payload = json.dumps(case["provider_payload"], ensure_ascii=False)
    source = replace(
        source, assembled_input=replace(source.assembled_input, provider_payload=payload)
    )
    receipts = evaluate_candidate_assertions(
        json.dumps(output(count), ensure_ascii=False).encode(),
        source,
        CAP6_ROOT,
        case["case_id"],
        frozen_suite=suite,
    )
    assert all(r.status in {"passed", "pending_semantic"} for r in receipts)
    counts = [r for r in receipts if r.type == "each_insight_has_distinct_dimension_refs"]
    assert len(counts) == 1 and counts[0].status == "passed"


async def test_existing_native_registration_can_bind_cap6_root_without_new_schema(assets):
    from qs_ai.application.interpretation.manifest import build_generation_manifest
    from qs_ai.domain.governance.profile import ProfileAsset
    from qs_ai.infrastructure.qs_server.evaluation_suite import CAP6_ROOT, derive_suite, load_suite
    from qs_ai.infrastructure.qs_server.profiles import canonical_definition

    stores, selection = assets
    release = candidate_release()
    stores[0].get.return_value = ProfileAsset(
        release.input_policy.profile_id,
        "v7",
        release.input_policy.profile_fingerprint,
        canonical_definition(json.loads(release.definition_json)),
    )
    stores[1].get.return_value = candidate_prompt_asset()
    selection["profile_version"] = "v7"
    manifest = await build_generation_manifest(*stores, **selection)
    root = load_suite(CAP6_ROOT)
    derived = derive_suite(
        "cap6-candidate", "v1", stores[0].get.return_value, manifest, source=root
    )
    assert derived.manifest == manifest
    assert derived.slots() == root.slots()
    assert (
        json.loads(derived.definition_json)["default_generation_assertions"]
        == json.loads(root.definition_json)["default_generation_assertions"]
    )
