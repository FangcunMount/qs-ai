from typing import Protocol

from qs_ai.domain.governance.route import RouteAsset


class RouteAssets(Protocol):
    async def put(self, asset: RouteAsset, source_ref: str, imported_by: str) -> bool:
        """Insert immutable content; return False for an identical replay."""
        ...

    async def get(self, route: str, revision: str) -> RouteAsset | None: ...
