"""Build a deliverable candidate only from bound facts and validated model output."""

import hashlib
import json
from uuid import NAMESPACE_URL, uuid5

from qs_ai.application.execution.generation import GeneratedExplanation
from qs_ai.application.interpretation.output import InvalidOutput, OutputParser, validate_output
from qs_ai.application.interpretation.ports import Claim
from qs_ai.application.interpretation.preparation import prepare_report_input
from qs_ai.application.interpretation.safety import check_safety
from qs_ai.domain.interpretation.artifact import ArtifactCandidate
from qs_ai.domain.interpretation.model import EvidenceSet, RuleViolation


def build_artifact(
    claim: Claim,
    evidence: EvidenceSet,
    generated: GeneratedExplanation,
    parser: OutputParser,
) -> ArtifactCandidate:
    frozen, response = generated.request, generated.response
    prepared = frozen.prepared
    # Recovery may use an older release, but never another session's facts.
    try:
        context = json.loads(prepared.assembled_input.provider_payload)["context"]
        locale, focus = context["locale"], tuple(context["focus_areas"])
    except (ValueError, KeyError, TypeError):
        raise RuleViolation("artifact_input_invalid") from None
    assembled = prepare_report_input(
        claim.session, evidence, prepared.release.input_policy, locale=locale, focus_areas=focus
    )
    if assembled != prepared.assembled_input:
        raise RuleViolation("artifact_evidence_mismatch")
    if (
        not response.invocation_id
        or not response.request_id
        or response.model != frozen.route.model
    ):
        raise InvalidOutput("artifact_response_identity_invalid")
    checked = check_safety(validate_output(response.validation_output, prepared, parser))
    content_hash = "sha256:" + hashlib.sha256(checked.content_json.encode()).hexdigest()
    policy = prepared.release.input_policy
    # Replay of the same accepted invocation has the same artifact identity.
    identity = str(uuid5(NAMESPACE_URL, f"qs-ai:artifact:{claim.run_id}:{response.invocation_id}"))
    return ArtifactCandidate(
        identity,
        claim.session.id,
        claim.run_id,
        evidence.id,
        evidence.fingerprint,
        response.invocation_id,
        response.request_id,
        checked.content_json,
        content_hash,
        assembled.fingerprint,
        policy.profile_id,
        policy.profile_version,
        policy.profile_fingerprint,
        prepared.prompt_fingerprint,
        frozen.route.fingerprint(),
        checked.deterministic_validator_version,
        checked.safety_validator_version,
    )
