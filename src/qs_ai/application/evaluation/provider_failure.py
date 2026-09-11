"""Map transport failures into stage-specific frozen recovery-policy vocabulary."""

import re

from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.domain.evaluation.failure import ClassifiedFailure, ProviderDiagnostics


def classify_provider_failure(
    stage: str, execution_id: str, error: ProviderFailure
) -> ClassifiedFailure:
    if stage not in ("generation", "semantic"):
        raise ValueError("Unsupported evaluation execution stage")
    code = error.code.strip()
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,127}", code):
        code = "provider_execution_failed"
    diagnostics = ProviderDiagnostics(code)
    unknown = error.result_unknown
    retryable = error.retryable and not unknown
    if stage == "generation":
        return ClassifiedFailure(
            "generation_execution",
            "result_unknown" if unknown else "infrastructure_execution",
            code,
            retryable,
            unknown,
            "manual_acknowledgement"
            if unknown
            else "retry_generation"
            if retryable
            else "no_action",
            "Provider execution did not complete",
            (execution_id,),
            diagnostics,
        )
    # Cardinality failure alone does not prove 'completed/no_message'. Without
    # those original diagnostics, do not enable the no-message retry selector.
    semantic_code = (
        "semantic_result_unknown"
        if unknown
        else "semantic_provider_rate_limited"
        if code == "provider_rate_limited"
        else "semantic_provider_failed"
    )
    return ClassifiedFailure(
        "semantic_evaluation",
        "result_unknown" if unknown else "semantic_execution",
        semantic_code,
        retryable,
        unknown,
        "manual_acknowledgement" if unknown else "retry_semantic",
        "Semantic provider execution did not complete",
        (execution_id,),
        diagnostics,
    )
