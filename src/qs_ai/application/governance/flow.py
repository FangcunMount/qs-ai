"""Read models describing fixed execution; a diagram never drives the executor."""

from typing import Any, Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope


class FlowReader(Protocol):
    async def solution(self, scope: DraftScope, identity: UUID) -> dict[str, Any]: ...

    async def publication(self, scope: DraftScope, identity: UUID) -> dict[str, Any]: ...
