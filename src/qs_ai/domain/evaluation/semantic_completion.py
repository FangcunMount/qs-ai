"""Terminal judge evidence binds to the accepted candidate, never a replacement output."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.completion import ProviderReceipt
from qs_ai.domain.evaluation.failure import ClassifiedFailure


def _identity(value: str) -> bool:
    return re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", value) is not None


def _reject_constant(value: str) -> None:
    raise ValueError("Invalid semantic JSON")


@dataclass(frozen=True)
class SemanticCompletion:
    execution_id: str
    candidate_id: str
    candidate_output_fingerprint: str
    execution_ordinal: int
    invocation_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    provider_call_count: int
    receipt: ProviderReceipt | None = None
    raw_output: bytes = b""
    normalized_output: bytes = b""
    failure: ClassifiedFailure | None = None

    def __post_init__(self) -> None:
        if not all(
            _identity(x) for x in (self.execution_id, self.candidate_id, self.invocation_id)
        ):
            raise ValueError("Invalid semantic execution identity")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.candidate_output_fingerprint):
            raise ValueError("Semantic execution requires candidate output fingerprint")
        if type(self.execution_ordinal) is not int or not 1 <= self.execution_ordinal <= 2:
            raise ValueError("Invalid semantic execution ordinal")
        if (
            any(
                t.tzinfo is None or t.utcoffset() is None
                for t in (self.started_at, self.finished_at)
            )
            or self.finished_at < self.started_at
        ):
            raise ValueError("Invalid semantic execution times")
        if type(self.provider_call_count) is not int or self.provider_call_count not in (0, 1):
            raise ValueError("Invalid semantic provider call count")
        if any(
            not isinstance(raw, bytes) or len(raw) > 256 * 1024
            for raw in (self.raw_output, self.normalized_output)
        ):
            raise ValueError("Semantic output exceeds bound or is not bytes")
        if self.receipt is not None and (
            self.provider_call_count != 1 or self.receipt.invocation_id != self.invocation_id
        ):
            raise ValueError("Semantic receipt does not match invocation")
        if self.status == "succeeded":
            if (
                self.receipt is None
                or not self.raw_output
                or not self.normalized_output
                or self.failure is not None
            ):
                raise ValueError("Successful semantic evidence incomplete")
            try:
                json.loads(self.normalized_output.decode("utf-8"), parse_constant=_reject_constant)
            except (UnicodeError, ValueError):
                raise ValueError("Invalid semantic JSON") from None
        elif self.status in ("failed", "result_unknown"):
            if (
                self.failure is None
                or self.failure.result_unknown != (self.status == "result_unknown")
                or self.failure.kind
                not in {
                    "result_unknown",
                    "semantic_execution",
                    "infrastructure_execution",
                    "provider_protocol",
                }
            ):
                raise ValueError("Invalid semantic failure classification")
        else:
            raise ValueError("Terminal semantic status required")

    @property
    def output_fingerprint(self) -> str:
        return "sha256:" + hashlib.sha256(self.normalized_output).hexdigest()

    def matches_checkpoint(
        self, checkpoint: ExecutionCheckpoint, owner: str, candidate_fingerprint: str
    ) -> bool:
        return (
            checkpoint.kind == "semantic"
            and checkpoint.phase == "dispatching"
            and checkpoint.owner == owner.strip()
            and (self.execution_id, self.candidate_id, self.execution_ordinal, self.invocation_id)
            == (
                checkpoint.execution_id,
                checkpoint.candidate_id,
                checkpoint.execution_ordinal,
                checkpoint.invocation_id,
            )
            and self.candidate_output_fingerprint == candidate_fingerprint
            and checkpoint.dispatch_started_at is not None
            and self.started_at >= checkpoint.dispatch_started_at
        )
