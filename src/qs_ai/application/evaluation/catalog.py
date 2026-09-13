"""Organization-scoped summaries; a list never grants permission to mutate a Run."""

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

STATUSES = (
    "requested",
    "collecting",
    "blocked",
    "awaiting_review",
    "approved",
    "rejected",
    "canceled",
)


@dataclass(frozen=True)
class EvaluationCatalogQuery:
    organization_id: int
    operator_user_id: int
    status: str = ""
    limit: int = 20
    cursor: str = ""

    def __post_init__(self) -> None:
        if any(
            type(v) is not int or not 0 < v <= 2**63 - 1
            for v in (self.organization_id, self.operator_user_id)
        ):
            raise ValueError("Trusted organization and reader required")
        if (
            self.status not in ("", *STATUSES)
            or type(self.limit) is not int
            or not 1 <= self.limit <= 100
        ):
            raise ValueError("Invalid evaluation catalog filter")
        self.after()

    def after(self) -> tuple[datetime, str] | None:
        if not isinstance(self.cursor, str) or len(self.cursor) > 1024:
            raise ValueError("Invalid evaluation catalog cursor")
        if not self.cursor:
            return None
        try:
            value = json.loads(base64.b64decode(self.cursor, altchars=b"-_", validate=True))
            if (
                not isinstance(value, list)
                or len(value) != 6
                or type(value[0]) is not int
                or type(value[2]) is not int
                or value[:4] != [1, "evaluation", self.organization_id, self.status]
            ):
                raise ValueError("Cursor belongs to another query")
            at, run_id = datetime.fromisoformat(value[4]), value[5]
            if at.utcoffset() is None or str(UUID(run_id)) != run_id or UUID(run_id).int == 0:
                raise ValueError("Invalid cursor position")
            return at.astimezone(UTC).replace(tzinfo=None), run_id
        except (binascii.Error, UnicodeError, TypeError, ValueError, AttributeError) as error:
            raise ValueError("Invalid evaluation catalog cursor") from error

    def next_cursor(self, created_at: str, run_id: str) -> str:
        at = datetime.fromisoformat(created_at).astimezone(UTC).isoformat()
        return base64.urlsafe_b64encode(
            json.dumps(
                [1, "evaluation", self.organization_id, self.status, at, run_id],
                separators=(",", ":"),
            ).encode()
        ).decode()


@dataclass(frozen=True)
class EvaluationSummary:
    run_id: str
    organization_id: int
    version: int
    status: str
    created_at: str
    requested_by: str
    profile_id: str
    profile_version: str
    prompt_id: str
    prompt_version: str
    release_fingerprint: str
    unresolved_result_unknown_count: int
    review_count: int
    required_candidates: int
    accepted_candidates: int
    review_ready_candidates: int
    last_cause: str
    last_reason: str


@dataclass(frozen=True)
class EvaluationPage:
    items: tuple[EvaluationSummary, ...]
    next_cursor: str = ""


class EvaluationCatalog(Protocol):
    async def list(self, query: EvaluationCatalogQuery) -> EvaluationPage: ...
