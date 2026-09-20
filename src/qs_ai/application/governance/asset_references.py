"""Exact, scoped policy usage queries; a cursor never supplies authorization."""

import base64
import json
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.evaluation.identity import FrozenContractRef


@dataclass(frozen=True)
class ReferenceQuery:
    kind: str
    reference: FrozenContractRef
    usage_kind: str
    limit: int = 20
    cursor: str = ""

    def __post_init__(self) -> None:
        if self.kind not in ("execution_policy", "gate_policy") or self.usage_kind not in (
            "suite",
            "evaluation",
            "publication",
        ):
            raise ValueError("Unsupported policy usage query")
        if (
            type(self.limit) is not int
            or not 1 <= self.limit <= 50
            or not isinstance(self.cursor, str)
            or len(self.cursor) > 4096
        ):
            raise ValueError("Invalid usage page")

    def binding(self, organization_id: int) -> list:
        return [1, organization_id, self.kind, asdict(self.reference), self.usage_kind]

    def after(self, organization_id: int) -> tuple[str, str] | None:
        if not self.cursor:
            return None
        try:
            value = json.loads(base64.b64decode(self.cursor, altchars=b"-_", validate=True))
            if (
                not isinstance(value, list)
                or len(value) != 7
                or value[:5] != self.binding(organization_id)
                or not all(isinstance(v, str) and len(v) <= 255 for v in value[5:])
            ):
                raise ValueError("Cursor differs from query")
            return value[5], value[6]
        except (ValueError, TypeError, UnicodeError) as error:
            raise ValueError("Invalid policy usage cursor") from error

    def next_cursor(self, organization_id: int, identity: str, version: str) -> str:
        return base64.urlsafe_b64encode(
            json.dumps(
                self.binding(organization_id) + [identity, version], separators=(",", ":")
            ).encode()
        ).decode()


class PolicyReferences(Protocol):
    async def get(self, scope: DraftScope, query: ReferenceQuery) -> dict[str, Any]: ...
