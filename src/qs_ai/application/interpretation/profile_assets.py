from typing import Protocol

from qs_ai.domain.governance.profile import ProfileAsset


class ProfileReader(Protocol):
    async def get(self, profile_id: str, version: str, /) -> ProfileAsset | None: ...


class ProfileAssets(ProfileReader, Protocol):
    async def put(self, asset: ProfileAsset, source_ref: str, imported_by: str) -> bool:
        """Insert immutable content; return False for an identical replay."""
        ...
