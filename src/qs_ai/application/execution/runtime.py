"""Read-only execution evidence delegated by QS; no orchestration dependencies."""

from typing import Any, Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope


def session_ids(values: list[str]) -> tuple[str, ...]:
    if not 1 <= len(values) <= 50 or len(set(values)) != len(values):
        raise ValueError("Between one and fifty distinct sessions required")
    for value in values:
        if not UUID(value).int or str(UUID(value)) != value:
            raise ValueError("Canonical session UUID required")
    return tuple(values)


class RuntimeReader(Protocol):
    async def summaries(self, scope: DraftScope, ids: tuple[str, ...]) -> dict[str, Any]: ...

    async def detail(self, scope: DraftScope, session_id: str) -> dict[str, Any]: ...

    async def health(self, scope: DraftScope) -> dict[str, Any]: ...
