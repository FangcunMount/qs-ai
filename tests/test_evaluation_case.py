import json
from dataclasses import replace

import pytest

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, load_suite
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.evaluation_helpers import prepare_evaluation_case
from tests.test_evaluation_identity import identity


def release():
    profile = load_migrated_release("participant-scale-score-range-default", "v6")
    prompt = load_prompt(profile.render_policy.template_id, profile.render_policy.version)
    return replace(
        identity(),
        suite=V6_PUBLISHED,
        input_schema=load_suite(V6_PUBLISHED).input_schema,
        profile=FrozenContractRef(
            profile.input_policy.profile_id,
            profile.input_policy.profile_version,
            profile.input_policy.profile_fingerprint,
        ),
        prompt=FrozenContractRef(prompt.template_id, prompt.version, prompt.fingerprint),
    )


@pytest.mark.parametrize("index", range(1, 8))
def test_original_case_facts_and_prompt_are_bound(index):
    case_id = f"PROMPT-EVAL-{index:03}"
    prepared = prepare_evaluation_case(release(), case_id)
    original = next(
        c
        for c in json.loads(load_suite(V6_PUBLISHED).definition_json)["cases"]
        if c["case_id"] == case_id
    )
    assert json.loads(prepared.assembled_input.provider_payload) == original["provider_payload"]
    assert prepared.prompt_fingerprint == release().prompt.fingerprint


@pytest.mark.parametrize("case_id", ["PROMPT-EVAL-008", "unknown"])
def test_preflight_or_unknown_case_cannot_render_generation(case_id):
    with pytest.raises(ValueError):
        prepare_evaluation_case(release(), case_id)


@pytest.mark.parametrize("field", ["profile", "prompt"])
def test_release_drift_rejected(field):
    bound = release()
    bound = replace(
        bound, **{field: replace(getattr(bound, field), fingerprint="sha256:" + "b" * 64)}
    )
    with pytest.raises(ValueError):
        prepare_evaluation_case(bound, "PROMPT-EVAL-001")
