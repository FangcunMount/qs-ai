"""Frozen preflight evidence; passing means rejection was proven without a provider call."""

import re
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class AssertionReceipt:
    type: str
    scope: str
    ordinal: int
    hard: bool
    evaluator: str
    status: str
    detail: str

    def __post_init__(self) -> None:
        if (
            not self.type.strip()
            or not self.evaluator.strip()
            or type(self.ordinal) is not int
            or self.ordinal < 1
        ):
            raise ValueError("Invalid assertion identity")
        if (
            self.scope not in ("default", "case")
            or self.status not in ("passed", "failed", "pending_semantic", "blocked")
            or type(self.hard) is not bool
        ):
            raise ValueError("Invalid assertion receipt")


@dataclass(frozen=True)
class PreflightEvidence:
    case_id: str
    status: str
    evaluated_at: datetime
    provider_call_count: int
    rejection_reason: str
    assertions: tuple[AssertionReceipt, ...]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.case_id):
            raise ValueError("Invalid preflight case")
        if type(self.provider_call_count) is not int or self.provider_call_count != 0:
            raise ValueError("Preflight must not call provider")
        if (
            self.status not in ("passed", "failed")
            or self.evaluated_at.tzinfo is None
            or self.evaluated_at.utcoffset() is None
        ):
            raise ValueError("Terminal preflight requires status and timestamp")
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,127}", self.rejection_reason):
            raise ValueError("Invalid preflight rejection reason")
        if (
            not isinstance(self.assertions, tuple)
            or not 1 <= len(self.assertions) <= 16
            or any(not isinstance(x, AssertionReceipt) for x in self.assertions)
        ):
            raise ValueError("Preflight assertions required")
        passed = {x.type for x in self.assertions if x.status == "passed"}
        if self.status == "passed" and not {"provider_call_count", "rejection_reason"} <= passed:
            raise ValueError("Preflight required assertions did not pass")
