"""Audited, one-execution exception; frozen policies and failed evidence are never edited."""

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.domain.evaluation.resolution import valid_id
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion

RECOVERY_VERSION = "semantic-contract-recovery/v1"
RECOVERY_INSTRUCTION = (
    "本次重试仅修复语义评判输出契约。必须逐项评判输入 assertions 中的全部检查项，"
    "每个 (type, scope, ordinal) 恰好返回一次，不得遗漏、重复或增加检查项。"
    "返回前核对 decisions 数量与 assertions 一致；status 必须与 detail 的判断一致。"
    "完整返回并不意味着通过：不满足的检查必须如实标记 failed，不得为通过评测改变判断。"
)
INSTRUCTION_FINGERPRINT = "sha256:" + hashlib.sha256(RECOVERY_INSTRUCTION.encode()).hexdigest()


@dataclass(frozen=True)
class ContractRecovery:
    execution_id: str
    candidate_id: str
    candidate_fingerprint: str
    output_fingerprint: str
    actor: str
    reason: str
    resolved_at: datetime
    acknowledged_policy_exception_and_cost: bool
    recovery_version: str = RECOVERY_VERSION
    instruction_fingerprint: str = INSTRUCTION_FINGERPRINT

    def __post_init__(self) -> None:
        if (
            not all(valid_id(v) for v in (self.execution_id, self.candidate_id, self.actor))
            or not all(
                re.fullmatch(r"sha256:[a-f0-9]{64}", v)
                for v in (self.candidate_fingerprint, self.output_fingerprint)
            )
            or not self.reason.strip()
            or len(self.reason.encode()) > 1000
            or any(c in self.reason for c in "<>")
            or self.resolved_at.tzinfo is None
            or self.resolved_at.utcoffset() is None
            or self.acknowledged_policy_exception_and_cost is not True
            or self.recovery_version != RECOVERY_VERSION
            or self.instruction_fingerprint != INSTRUCTION_FINGERPRINT
        ):
            raise ValueError("Invalid semantic contract recovery authorization")


def validate_recoveries(
    values: tuple[ContractRecovery, ...],
    semantics: tuple[SemanticCompletion, ...],
    policy: ExecutionPolicy,
) -> set[str]:
    """Validate original failures and bind each exception to exactly one later attempt."""
    authorized: set[str] = set()
    for value in values:
        target = next((s for s in semantics if s.execution_id == value.execution_id), None)
        if (
            target is None
            or target.execution_id in authorized
            or target.status != "failed"
            or target.failure is None
            or target.failure.code != "semantic_decision_contract_invalid"
            or not target.failure.allows_semantic_retry()
            or target.failure.result_unknown
            or target.receipt is None
            or not target.normalized_output
            or target.candidate_id != value.candidate_id
            or target.candidate_output_fingerprint != value.candidate_fingerprint
            or target.output_fingerprint != value.output_fingerprint
            or value.resolved_at < target.finished_at
            or target.execution_ordinal >= policy.semantic_per_candidate
        ):
            raise ValueError("Recovery requires matching contract failure and remaining budget")
        successors = [
            s
            for s in semantics
            if s.candidate_id == target.candidate_id
            and s.execution_ordinal > target.execution_ordinal
        ]
        if any(s.started_at < value.resolved_at for s in successors):
            raise ValueError("Semantic retry precedes recovery authorization")
        authorized.add(value.execution_id)
    return authorized


def decode_recoveries(values: list[dict]) -> tuple[ContractRecovery, ...]:
    return tuple(
        ContractRecovery(**{**v, "resolved_at": datetime.fromisoformat(v["resolved_at"])})
        for v in values
    )
