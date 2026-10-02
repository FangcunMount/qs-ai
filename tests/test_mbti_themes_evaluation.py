"""Synthetic thematic evaluation plumbing, never real roots or quality evidence."""

import copy
import hashlib
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime

import pytest

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference, GenerationManifest
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.qs_server.evaluation_case import prepare_asset_evaluation_case
from qs_ai.infrastructure.qs_server.evaluation_input import (
    validate_suite_input,
    validate_suite_inputs,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    MBTI_THEMATIC_INPUT_VERSION,
    canonical,
    derive_suite,
)
from qs_ai.infrastructure.qs_server.mbti_assets import load_mbti_root
from qs_ai.infrastructure.qs_server.output import schema_directory
from qs_ai.infrastructure.qs_server.preflight import run_preflight
from qs_ai.infrastructure.qs_server.profiles import canonical_definition, decode_published_profile
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.test_mbti_contract import sign
from tests.test_mbti_themes_input import thematic_profile


def thematic_suite():
    root = load_mbti_root()
    entry = thematic_profile()
    release = decode_published_profile(entry)
    definition = entry["definition"]
    document = json.loads(root.suite.definition_json)
    document.update(
        suite_id="synthetic-mbti-themes",
        suite_version="test-v1",
        profile_fixture={**definition, "fingerprint": entry["fingerprint"], "status": "registered"},
    )
    package = load_prompt(release.render_policy.template_id, release.render_policy.version)
    document["prompt"] = {"template_id": package.template_id, "version": package.version}
    raw_schema = (schema_directory() / "ai-explanation-input-v3.schema.json").read_text()
    schema = FrozenContractRef(
        "ai-explanation-input", "ai-explanation-input/v3", digest(raw_schema)
    )
    document["input_contract"] = {
        "construction_version": MBTI_THEMATIC_INPUT_VERSION,
        "schema": asdict(schema),
    }
    for case in document["cases"]:
        payload = case["provider_payload"]
        payload["reference_material"] = release.input_policy.reference_material.select(
            payload["facts"]["model_result"]["type_code"]
        ).projection()
    raw = canonical(document)
    suite = replace(
        root.suite,
        reference=FrozenContractRef(document["suite_id"], document["suite_version"], digest(raw)),
        definition_json=raw,
        input_construction_version=MBTI_THEMATIC_INPUT_VERSION,
        input_schema=schema,
    )
    evidence = replace(
        root.release,
        suite=suite.reference,
        profile=FrozenContractRef(
            definition["profile_id"], definition["version"], entry["fingerprint"]
        ),
        prompt=FrozenContractRef(package.template_id, package.version, package.fingerprint),
        input_schema=schema,
        output_schema=FrozenContractRef(
            "ai-explanation-output",
            "ai-explanation-output/v2",
            digest((schema_directory() / "ai-explanation-output-v2.schema.json").read_text()),
        ),
    )
    return suite, release, package, evidence


def digest(raw):
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


def manifest_for(profile, evidence):
    def asset(ref):
        # Schema references use full wire versions, manifest versions use vN.
        version = (
            ref.version.split("/")[-1]
            if ref.id in {"ai-explanation-input", "ai-explanation-output"}
            else ref.version
        )
        return AssetReference(
            ref.id, version, ref.fingerprint, ref.fingerprint.removeprefix("sha256:")
        )

    return GenerationManifest(
        AssetReference(
            profile.profile_id, profile.version, profile.fingerprint, profile.fingerprint[7:]
        ),
        asset(evidence.prompt),
        asset(evidence.generation_route),
        asset(evidence.input_schema),
        asset(evidence.output_schema),
    )


def test_all_cases_keep_exact_synthetic_facts_and_frozen_reference_payloads():
    suite, profile, package, evidence = thematic_suite()
    validate_suite_inputs(suite, evidence.input_schema)
    assert len(suite.slots()) == 35
    for case in json.loads(suite.definition_json)["cases"]:
        if case["stage"] != "generation":
            continue
        prepared = prepare_asset_evaluation_case(
            evidence, case["case_id"], profile, package, frozen_suite=suite
        )
        assert json.loads(prepared.messages.data_json) == case["provider_payload"]
        assert json.loads(prepared.assembled_input.canonical_json) == case["provider_payload"]
        assert set(case["provider_payload"]) == {"context", "facts", "reference_material"}
        assert "source" not in case["provider_payload"]
    assert run_preflight(suite.reference, datetime.now(UTC), frozen_suite=suite).status == "passed"


@pytest.mark.parametrize(
    "damage", ["missing", "body", "source", "hash", "type", "extra", "profile"]
)
def test_tampered_or_missing_evaluation_references_fail_before_dispatch(damage):
    suite, _, _, evidence = thematic_suite()
    document = json.loads(suite.definition_json)
    payload = copy.deepcopy(document["cases"][0]["provider_payload"])
    refs = payload["reference_material"]
    if damage == "missing":
        del payload["reference_material"]
    elif damage == "body":
        refs["entries"][0]["content"] = "替换后的合成参考"
    elif damage == "source":
        refs["sources"][0]["title"] = "冒充来源"
    elif damage == "hash":
        refs["fingerprint"] = "sha256:" + "a" * 64
    elif damage == "type":
        refs["type_code"] = "ENTP"
    elif damage == "extra":
        payload["source"] = {"report_id": "invented"}
    else:
        document["profile_fixture"]["reference_material"]["entries"][0]["content"] = "修改来源包"
        fixture = document["profile_fixture"]
        fixture["fingerprint"] = sign(
            {"definition": {k: v for k, v in fixture.items() if k not in {"status", "fingerprint"}}}
        )["fingerprint"]
        suite = replace(suite, definition_json=canonical(document))
    with pytest.raises(ValueError):
        validate_suite_input(suite, evidence.input_schema, payload)


@pytest.mark.parametrize("change", ["inherit", "reference_material"])
def test_derivation_cannot_replace_reference_contract_without_new_cases(change):
    source, _, _, evidence = thematic_suite()
    definition = copy.deepcopy(json.loads(source.definition_json)["profile_fixture"])
    definition = {k: v for k, v in definition.items() if k not in {"status", "fingerprint"}}
    definition["version"] = "derived-test-v1"
    if change == "reference_material":
        definition["reference_material"]["entries"][0]["content"] = "替换来源素材"
    raw = canonical_definition(definition)
    profile = ProfileAsset(definition["profile_id"], definition["version"], digest(raw), raw)
    manifest = manifest_for(profile, evidence)
    if change == "inherit":
        derived = derive_suite("synthetic-derived", "v1", profile, manifest, source=source)
        assert derived.input_construction_version == MBTI_THEMATIC_INPUT_VERSION
        assert derived.slots() == source.slots()
        validate_suite_inputs(derived, evidence.input_schema)
    else:
        with pytest.raises(ValueError, match="matching case contract"):
            derive_suite("synthetic-invalid", "v1", profile, manifest, source=source)
