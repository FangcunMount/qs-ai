"""QS failure taxonomy v1; failures retain evidence references, not provider bodies."""

import re
from dataclasses import dataclass

STAGES = frozenset(
    {
        "generation_execution",
        "output_validation",
        "deterministic_validation",
        "semantic_evaluation",
        "human_review",
        "run_governance",
    }
)
KINDS = frozenset(
    {
        "infrastructure_execution",
        "result_unknown",
        "provider_protocol",
        "output_contract_conformance",
        "semantic_execution",
        "quality_failure",
    }
)
DISPOSITIONS = frozenset(
    {
        "retry_generation",
        "replace_generation",
        "retry_semantic",
        "manual_acknowledgement",
        "retain_candidate",
        "reject_release",
        "cancel_run",
        "no_action",
    }
)


@dataclass(frozen=True)
class ProviderDiagnostics:
    code: str
    request_id: str = ""
    response_status: str = ""
    response_shape: str = ""

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", self.code):
            raise ValueError("Invalid provider diagnostic code")
        for value in (self.request_id, self.response_status, self.response_shape):
            if value and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", value):
                raise ValueError("Invalid provider diagnostic metadata")


@dataclass(frozen=True)
class ClassifiedFailure:
    stage: str
    kind: str
    code: str
    retryable: bool
    result_unknown: bool
    disposition: str
    safe_message: str
    evidence_refs: tuple[str, ...]
    provider_diagnostics: ProviderDiagnostics | None = None
    schema_version: str = "ai-explanation-failure-taxonomy/v1"

    def __post_init__(self) -> None:
        if (
            self.schema_version != "ai-explanation-failure-taxonomy/v1"
            or self.stage not in STAGES
            or self.kind not in KINDS
            or self.disposition not in DISPOSITIONS
            or not re.fullmatch(r"[a-z][a-z0-9_-]{0,127}", self.code)
            or type(self.retryable) is not bool
            or type(self.result_unknown) is not bool
        ):
            raise ValueError("Invalid failure classification")
        if self.provider_diagnostics is not None and not isinstance(
            self.provider_diagnostics, ProviderDiagnostics
        ):
            raise ValueError("Invalid provider diagnostics")
        message = self.safe_message.strip()
        if not message or len(message.encode()) > 1000 or any(x in message for x in "<>"):
            raise ValueError("Invalid safe failure message")
        if not isinstance(self.evidence_refs, tuple) or not 1 <= len(self.evidence_refs) <= 16:
            raise ValueError("Invalid evidence references")
        seen = set()
        for value in self.evidence_refs:
            ref = value.strip()
            if (
                not ref
                or len(ref.encode()) > 256
                or any(x in ref for x in "<>\t\r\n ")
                or ref in seen
            ):
                raise ValueError("Invalid or duplicated evidence reference")
            seen.add(ref)
        if self.result_unknown != (self.kind == "result_unknown"):
            raise ValueError("Inconsistent unknown result")
        if self.kind == "result_unknown" and (
            self.retryable or self.disposition != "manual_acknowledgement"
        ):
            raise ValueError("Unknown results require manual acknowledgement")
        if self.kind == "output_contract_conformance" and (
            self.stage != "output_validation"
            or self.retryable
            or self.disposition != "replace_generation"
        ):
            raise ValueError("Invalid output-contract disposition")
        if self.kind == "semantic_execution" and (
            self.stage != "semantic_evaluation" or self.disposition != "retry_semantic"
        ):
            raise ValueError("Invalid semantic-execution disposition")
        if self.kind == "quality_failure" and (
            self.retryable or self.disposition not in {"retain_candidate", "reject_release"}
        ):
            raise ValueError("Quality failures must retain candidate or reject release")

    def candidate_exists(self) -> bool:
        return self.kind in {"semantic_execution", "quality_failure"}

    def allows_generation_replacement(self) -> bool:
        return self.disposition == "replace_generation"

    def allows_semantic_retry(self) -> bool:
        return self.disposition == "retry_semantic"
