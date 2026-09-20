"""Frozen judge instructions and explicit untrusted evaluation data projection."""

import json

from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.qs_server.evaluation_assertions import (
    assertion_inventory,
    semantic_obligations,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import FrozenSuite
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.semantic_assets import SemanticAssets


def prepare_semantic_messages(
    release: EvidenceReleaseIdentity,
    generation: GenerationCompletion,
    assertions: tuple[AssertionReceipt, ...],
    *,
    prepared: PreparedExplanation,
    assets: SemanticAssets,
    frozen_suite: FrozenSuite | None = None,
) -> PromptMessages:
    if generation.status != "succeeded":
        raise ValueError("Semantic evaluation requires accepted generation evidence")
    if (
        release.semantic_prompt != assets.prompt
        or release.semantic_output_schema != assets.output_schema
    ):
        raise ValueError("Semantic release assets mismatch")
    if (
        prepared.prompt_fingerprint != release.prompt.fingerprint
        or prepared.release.input_policy.profile_fingerprint != release.profile.fingerprint
    ):
        raise ValueError("Semantic input assets differ from frozen release")
    inventory = assertion_inventory(release.suite, generation.case_id, frozen_suite=frozen_suite)
    obligations = semantic_obligations(inventory, assertions)
    if not 1 <= len(obligations) <= 32:
        raise ValueError("Semantic obligation count outside contract")
    parameters = {(a.type, a.scope, a.ordinal): json.loads(a.parameters_json) for a in inventory}
    payload = {
        "schema_version": "ai-explanation-semantic-evaluation-input/v1",
        "suite_id": release.suite.id,
        "case_id": generation.case_id,
        # Original v2 runner uses the candidate slot ordinal, not retry ordinal.
        "attempt": generation.slot_ordinal,
        "assessment_input": json.loads(prepared.assembled_input.provider_payload),
        "candidate_output": QSOutputParser().parse(generation.normalized_output.decode()),
        "assertions": [
            dict(
                type=a.type,
                scope=a.scope,
                ordinal=a.ordinal,
                hard=a.hard,
                parameters=parameters[a.type, a.scope, a.ordinal],
            )
            for a in obligations
        ],
    }
    return PromptMessages(
        assets.system_message,
        assets.task_message,
        assets.data_preamble,
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    )
