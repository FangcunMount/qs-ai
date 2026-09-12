"""Prepare registered synthetic evaluation facts, not an authorized business report."""

import hashlib
import json

from qs_ai.application.interpretation.input import AssembledInput
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import PromptPackage, render_prompt
from qs_ai.application.interpretation.release import ExplanationRelease
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_input
from qs_ai.infrastructure.qs_server.evaluation_suite import load_suite
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt


def prepare_evaluation_case(release: EvidenceReleaseIdentity, case_id: str) -> PreparedExplanation:
    return prepare_asset_evaluation_case(
        release,
        case_id,
        load_migrated_release(release.profile.id, release.profile.version),
        load_prompt(release.prompt.id, release.prompt.version),
    )


def prepare_asset_evaluation_case(
    release: EvidenceReleaseIdentity,
    case_id: str,
    profile: ExplanationRelease,
    package: PromptPackage,
) -> PreparedExplanation:
    """Render exact supplied assets after matching the registered suite and release."""
    suite = load_suite(release.suite)
    document = json.loads(suite.definition_json)
    case = next((c for c in document["cases"] if c["case_id"] == case_id), None)
    if case is None or case["stage"] != "generation":
        raise ValueError("Registered generation case required")
    validate_suite_input(suite, release.input_schema, case["provider_payload"])
    if (
        profile.input_policy.profile_id,
        profile.input_policy.profile_version,
        profile.input_policy.profile_fingerprint,
    ) != (release.profile.id, release.profile.version, release.profile.fingerprint) or (
        package.template_id,
        package.version,
        package.fingerprint,
    ) != (release.prompt.id, release.prompt.version, release.prompt.fingerprint):
        raise ValueError("Supplied assets differ from evaluation release")
    fixture = document["profile_fixture"]
    if (release.profile.id, release.profile.version, release.profile.fingerprint) != (
        fixture["profile_id"],
        fixture["version"],
        fixture["fingerprint"],
    ) or profile.input_policy.profile_fingerprint != release.profile.fingerprint:
        raise ValueError("Evaluation Profile does not match frozen suite")
    if (package.template_id, package.version, package.fingerprint) != (
        document["prompt"]["template_id"],
        document["prompt"]["version"],
        release.prompt.fingerprint,
    ):
        raise ValueError("Evaluation Prompt does not match frozen suite")
    # This digest identifies the synthetic provider payload. It is not a QS report fingerprint.
    payload = json.dumps(case["provider_payload"], ensure_ascii=False, separators=(",", ":"))
    assembled = AssembledInput(
        payload, "sha256:" + hashlib.sha256(payload.encode()).hexdigest(), payload
    )
    return PreparedExplanation(
        assembled,
        render_prompt(package, profile.render_policy, payload),
        profile,
        package.fingerprint,
    )
