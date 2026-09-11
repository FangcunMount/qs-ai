import hashlib
import json
from dataclasses import replace

import pytest

from qs_ai.application.execution.artifact import build_artifact
from qs_ai.application.execution.generation import FrozenGeneration, GeneratedExplanation
from qs_ai.application.interpretation.output import InvalidOutput
from qs_ai.application.interpretation.ports import Claim
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.test_deepseek_request import route, schema
from tests.test_input_binding import bound_case
from tests.test_output_validation import candidate


def case():
    session, evidence, _ = bound_case()
    claim = Claim("job", "run", session, 1, None, False, None)
    release = load_migrated_release("participant-scale-score-range-default", "v6")
    package = load_prompt(release.render_policy.template_id, release.render_policy.version)
    prepared = prepare_explanation(session, evidence, release, package)
    response = ModelResponse(
        "invocation", "provider-id", route().model, "raw", json.dumps(candidate()), "none", 2, 3, 4
    )
    return (
        claim,
        evidence,
        GeneratedExplanation(FrozenGeneration(prepared, route(), schema()), response),
    )


def test_artifact_is_reproducible_and_traces_validated_content():
    claim, evidence, generated = case()
    artifact = build_artifact(claim, evidence, generated, QSOutputParser())
    assert artifact == build_artifact(claim, evidence, generated, QSOutputParser())
    assert artifact.run_id == claim.run_id
    assert artifact.evidence_fingerprint == evidence.fingerprint
    assert artifact.provider_request_id == "provider-id"
    assert (
        artifact.content_fingerprint
        == "sha256:" + hashlib.sha256(artifact.content_json.encode()).hexdigest()
    )
    assert json.loads(artifact.content_json) == candidate()
    assert artifact.profile_version == "v6"
    assert artifact.output_validator_version and artifact.safety_validator_version


@pytest.mark.parametrize("failure", ["schema", "reference", "safety", "receipt", "evidence"])
def test_no_artifact_when_any_acceptance_gate_fails(failure):
    claim, evidence, generated = case()
    content = candidate()
    if failure == "schema":
        del content["summary"]
    elif failure == "reference":
        content["integrated_insights"][0]["evidence_refs"][0]["ref"] = "missing"
    elif failure == "safety":
        content["summary"] = "你一定会遭遇困难。"
    elif failure == "receipt":
        generated = replace(generated, response=replace(generated.response, invocation_id=""))
    else:
        evidence = replace(evidence, session_id="other")
    generated = replace(
        generated, response=replace(generated.response, validation_output=json.dumps(content))
    )
    with pytest.raises((InvalidOutput, RuleViolation)):
        build_artifact(claim, evidence, generated, QSOutputParser())
