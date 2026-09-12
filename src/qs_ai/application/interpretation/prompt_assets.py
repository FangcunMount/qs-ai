import json
from typing import Protocol

from qs_ai.application.interpretation.prompts import PromptPackage
from qs_ai.domain.governance.prompt import PromptAsset


def executable_prompt(asset: PromptAsset) -> PromptPackage:
    """Project a validated imported or native asset without inventing Git provenance."""
    data = json.loads(asset.package_json)
    return PromptPackage(
        asset.template_id,
        asset.version,
        asset.fingerprint,
        data["Ref"].get("GitBlobSHA"),
        data["SystemMessage"],
        data["TaskTemplate"],
        data["DataPreamble"],
        tuple(data["AllowedPlaceholders"]),
    )


class PromptReader(Protocol):
    async def get(self, template_id: str, version: str, /) -> PromptAsset | None: ...


class PromptAssets(PromptReader, Protocol):
    async def put(self, asset: PromptAsset, source_ref: str, imported_by: str) -> bool:
        """Insert immutable content; return False for an identical replay."""
        ...
