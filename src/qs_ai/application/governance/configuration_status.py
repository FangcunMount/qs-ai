"""Public configuration metadata read port; deployment credentials never cross it."""

from typing import Any, Protocol

from qs_ai.application.governance.prompt_drafts import DraftScope


class ConfigurationStatus(Protocol):
    async def get(self, scope: DraftScope) -> dict[str, Any]: ...
