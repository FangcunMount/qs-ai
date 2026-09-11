import hashlib
import json
from pathlib import Path

import pytest

from qs_ai.application.interpretation.input import assemble_input
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.application.interpretation.release import InvalidRelease
from qs_ai.infrastructure.qs_server.profiles import (
    canonical_definition,
    decode_published_profile,
    load_migrated_release,
)
from qs_ai.infrastructure.qs_server.prompts import load_prompt, prompt_directory

PROFILE = "participant-scale-score-range-default"


def envelope() -> dict:
    return json.loads((prompt_directory() / "published-profile-baseline.json").read_text())[
        "profiles"
    ][0]


def test_observed_published_profile_loads_and_prepares_prompt() -> None:
    release = load_migrated_release(PROFILE, "v6")
    assert release.input_policy.profile_fingerprint == envelope()["fingerprint"]
    assert release.render_policy.version == "v6"
    assert release.provider_route == "balanced_text_v1"
    raw = (Path(__file__).parent / "fixtures" / "report_snapshot.json").read_text()
    assembled = assemble_input(raw, release.input_policy, focus_areas=("sleep_routine",))
    messages = render_prompt(
        load_prompt(release.render_policy.template_id, release.render_policy.version),
        release.render_policy,
        assembled.provider_payload,
    )
    assert "{{" not in messages.task_message
    assert json.loads(messages.data_json)["context"]["focus_areas"] == ["sleep_routine"]


@pytest.mark.parametrize("status", ["draft", "disabled", "unknown"])
def test_unpublished_rejected(status: str) -> None:
    value = envelope()
    value["status"] = status
    with pytest.raises(InvalidRelease):
        decode_published_profile(value)


def test_content_fingerprint_and_input_mutation() -> None:
    value = envelope()
    release = decode_published_profile(value)
    value["definition"]["generation_policy"]["max_output_characters"] = 9000
    assert release.render_policy.max_output_characters == 8000
    with pytest.raises(InvalidRelease):
        decode_published_profile(value)


@pytest.mark.parametrize(
    "path,value",
    [
        (("selector", "audience"), "staff"),
        (("selector", "model_version"), "v1"),
        (("eligibility", "min_eligible_dimensions"), 1),
        (("eligibility", "max_input_dimensions"), 1),
        (("eligibility", "on_dimension_overflow"), "truncate"),
        (("input_policy", "allowed_focus_areas"), ["sleep_routine", "sleep_routine"]),
        (("input_policy", "context_scope"), "all_history"),
        (("input_policy", "include_model_result"), "false"),
        (("insight_policy", "allow_causal_claims"), True),
        (("insight_policy", "allowed_kinds"), ["diagnosis"]),
        (("insight_policy", "min_items"), 4),
        (("insight_policy", "min_items"), True),
        (("suggestion_policy", "require_evidence_refs"), False),
        (("suggestion_policy", "require_standard_refs_for_standard_derived"), False),
        (("suggestion_policy", "allowed_origins"), ["prescription"]),
        (("safety_policy", "forbidden_claims"), ["diagnosis"]),
        (("generation_policy", "output_schema_version"), "unknown"),
        (("generation_policy", "prompt_template_id"), " "),
        (("generation_policy", "provider_route"), "bad route"),
        (("generation_policy", "max_output_characters"), 20001),
        (("generation_policy", "unknown_field"), "unexpected"),
    ],
)
def test_invalid_policy_rejected_even_with_recomputed_fingerprint(
    path: tuple[str, str], value: object
) -> None:
    entry = envelope()
    entry["definition"][path[0]][path[1]] = value
    entry["fingerprint"] = (
        "sha256:" + hashlib.sha256(canonical_definition(entry["definition"]).encode()).hexdigest()
    )
    with pytest.raises(InvalidRelease):
        decode_published_profile(entry)


def test_ambiguous_and_missing_versions_rejected(tmp_path: Path) -> None:
    with pytest.raises(InvalidRelease):
        load_migrated_release(PROFILE, "latest")
    (tmp_path / "published-profile-baseline.json").write_text(
        json.dumps({"profiles": [envelope(), envelope()]})
    )
    with pytest.raises(InvalidRelease):
        load_migrated_release(PROFILE, "v6", directory=tmp_path)
