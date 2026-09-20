import hashlib
import json
from dataclasses import asdict, replace

import pytest

from qs_ai.application.interpretation.manifest import build_generation_manifest
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.qs_server.evaluation_assertions import assertion_inventory
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    V6_PUBLISHED,
    canonical,
    derive_suite,
    load_suite,
)
from tests.test_generation_manifest import assets as assets


@pytest.fixture
async def native_suite(assets):
    stores, selection = assets
    profile = stores[0].get.return_value
    definition = json.loads(profile.definition_json)
    definition["version"] = "native-case-v1"
    from qs_ai.infrastructure.qs_server.profiles import canonical_definition

    raw = canonical_definition(definition)
    profile = ProfileAsset(
        profile.profile_id,
        definition["version"],
        "sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
        raw,
    )
    stores[0].get.return_value = profile
    selection["profile_version"] = profile.version
    manifest = await build_generation_manifest(*stores, **selection)
    return derive_suite(
        "native-evaluation", "v1", profile, manifest, source=load_suite(V6_PUBLISHED)
    )


async def test_native_suite_has_independent_identity_and_retains_full_obligations(native_suite):
    baseline = load_suite(V6_PUBLISHED)
    assert native_suite.reference != baseline.reference
    assert native_suite.manifest.profile.version == "native-case-v1"
    assert native_suite.slots() == baseline.slots()
    for case in baseline.generation_case_ids:
        expected = assertion_inventory(V6_PUBLISHED, case)
        actual = assertion_inventory(native_suite.reference, case, frozen_suite=native_suite)
        assert [replace(a, parameters_json="") for a in actual] == [
            replace(a, parameters_json="") for a in expected
        ]
        assert [json.loads(a.parameters_json) for a in actual] == [
            json.loads(a.parameters_json) for a in expected
        ]
    assert json.loads(native_suite.definition_json)["derived_from"] == asdict(V6_PUBLISHED)


@pytest.mark.parametrize(
    "damage",
    [
        "assertions",
        "assertion_type",
        "cases",
        "repetitions",
        "source",
        "fixture",
        "manifest",
        "schema",
        "unknown",
        "eligibility",
        "bytes",
    ],
)
async def test_native_suite_rejects_changed_quality_or_binding_even_with_new_hash(
    native_suite, damage
):
    doc = json.loads(native_suite.definition_json)
    if damage == "assertion_type":
        doc["execution_policy"]["generation_repetitions_per_case"] = 5.0
    elif damage == "assertions":
        doc["default_generation_assertions"].pop()
    elif damage == "cases":
        doc["cases"][0]["provider_payload"]["facts"]["dimensions"].pop()
    elif damage == "repetitions":
        doc["execution_policy"]["generation_repetitions_per_case"] = 1
    elif damage == "source":
        doc["derived_from"]["fingerprint"] = "sha256:" + "0" * 64
    elif damage == "fixture":
        doc["profile_fixture"]["version"] = "other"
    elif damage == "manifest":
        doc["manifest"]["profile"]["content_sha256"] = "0" * 64
    elif damage == "schema":
        doc["input_contract"]["schema"]["fingerprint"] = "sha256:" + "0" * 64
    elif damage == "unknown":
        doc["skip_quality"] = True
    elif damage == "eligibility":
        doc["profile_fixture"]["eligibility"]["min_eligible_dimensions"] = 3
    raw = canonical(doc) + (" " if damage == "bytes" else "")
    ref = replace(
        native_suite.reference, fingerprint="sha256:" + hashlib.sha256(raw.encode()).hexdigest()
    )
    with pytest.raises(ValueError):
        load_suite(ref, definition_json=raw, source=load_suite(V6_PUBLISHED))
