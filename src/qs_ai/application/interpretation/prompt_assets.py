from typing import Protocol

from qs_ai.domain.governance.prompt import PromptAsset


class PromptAssets(Protocol):
    async def put(self, asset: PromptAsset, source_ref: str, imported_by: str) -> bool:
        """Insert immutable content; return False for an identical replay."""
        ...

    async def get(self, template_id: str, version: str) -> PromptAsset | None: ...
