"""Terminal generation evidence; acceptance into a Run remains transactional."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.failure import ClassifiedFailure


def _identity(value: str) -> bool:
    return re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", value) is not None


def _reject_constant(value: str) -> None:
    raise ValueError("Non-JSON numeric constant")


@dataclass(frozen=True)
class ProviderReceipt:
    invocation_id: str
    request_id: str
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    latency_ns: int

    def __post_init__(self) -> None:
        if not _identity(self.invocation_id) or not _identity(self.request_id):
            raise ValueError("Invalid provider receipt identity")
        if (
            not re.fullmatch(r"[a-z][a-z0-9_-]{0,127}", self.provider)
            or not self.model.strip()
            or len(self.model.encode()) > 128
        ):
            raise ValueError("Invalid provider or model")
        if any(
            type(x) is not int or not 0 <= x <= 2**63 - 1
            for x in (self.input_tokens, self.output_tokens, self.latency_ns)
        ):
            raise ValueError("Invalid provider usage")


@dataclass(frozen=True)
class GenerationCompletion:
    execution_id: str
    case_id: str
    slot_ordinal: int
    execution_ordinal: int
    invocation_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    provider_call_count: int
    receipt: ProviderReceipt | None = None
    raw_output: bytes = b""
    normalized_output: bytes = b""
    normalized_fingerprint: str = ""
    failure: ClassifiedFailure | None = None

    def __post_init__(self) -> None:
        if not all(_identity(x) for x in (self.execution_id, self.case_id, self.invocation_id)):
            raise ValueError("Invalid execution identity")
        if (
            type(self.slot_ordinal) is not int
            or not 1 <= self.slot_ordinal <= 5
            or type(self.execution_ordinal) is not int
            or not 1 <= self.execution_ordinal <= 2
        ):
            raise ValueError("Invalid execution ordinal")
        if (
            any(
                t.tzinfo is None or t.utcoffset() is None
                for t in (self.started_at, self.finished_at)
            )
            or self.finished_at < self.started_at
        ):
            raise ValueError("Invalid execution times")
        if type(self.provider_call_count) is not int or self.provider_call_count not in (0, 1):
            raise ValueError("Invalid provider call count")
        if any(
            not isinstance(raw, bytes) or len(raw) > 256 * 1024
            for raw in (self.raw_output, self.normalized_output)
        ):
            raise ValueError("Output evidence exceeds bound or is not bytes")
        if self.normalized_output:
            json.loads(self.normalized_output.decode("utf-8"), parse_constant=_reject_constant)
            if (
                self.normalized_fingerprint
                != "sha256:" + hashlib.sha256(self.normalized_output).hexdigest()
            ):
                raise ValueError("Normalized output fingerprint mismatch")
        elif self.normalized_fingerprint:
            raise ValueError("Fingerprint requires normalized output")
        if self.receipt is not None and (
            self.provider_call_count != 1 or self.receipt.invocation_id != self.invocation_id
        ):
            raise ValueError("Receipt does not match execution")
        if self.status == "succeeded":
            if (
                self.receipt is None
                or not self.raw_output
                or not self.normalized_output
                or self.failure is not None
            ):
                raise ValueError("Successful execution evidence incomplete")
        elif self.status in ("failed", "result_unknown"):
            if (
                self.failure is None
                or self.failure.candidate_exists()
                or self.failure.result_unknown != (self.status == "result_unknown")
            ):
                raise ValueError("Terminal failure classification mismatch")
        else:
            raise ValueError("Terminal status required")

    def matches_checkpoint(self, checkpoint: ExecutionCheckpoint, owner: str) -> bool:
        return (
            checkpoint.phase == "dispatching"
            and checkpoint.kind == "generation"
            and checkpoint.owner == owner.strip()
            and (
                self.execution_id,
                self.invocation_id,
                self.case_id,
                self.slot_ordinal,
                self.execution_ordinal,
            )
            == (
                checkpoint.execution_id,
                checkpoint.invocation_id,
                checkpoint.case_id,
                checkpoint.slot_ordinal,
                checkpoint.execution_ordinal,
            )
            and checkpoint.dispatch_started_at is not None
            and self.started_at >= checkpoint.dispatch_started_at
        )
