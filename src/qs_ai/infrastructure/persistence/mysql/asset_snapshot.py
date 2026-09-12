"""Immutable asset reads in the caller's existing transaction and snapshot."""

from dataclasses import fields
from typing import Generic, TypeVar

from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.release import resolve_generation_assets
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.governance.manifest import GenerationManifest
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)

Asset = TypeVar("Asset", ProfileAsset, PromptAsset, RouteAsset, SchemaAsset)


class AssetSnapshotReader(Generic[Asset]):
    def __init__(self, db: AsyncSession, table: Table, asset_type: type[Asset]) -> None:
        self.db = db
        self.table = table
        self.asset_type: type[Asset] = asset_type

    async def get(self, identity: str, version: str, /) -> Asset | None:
        keys = list(self.table.primary_key.columns)
        row = (
            (
                await self.db.execute(
                    select(self.table).where(keys[0] == identity, keys[1] == version)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return self.asset_type(**{field.name: row[field.name] for field in fields(self.asset_type)})


async def generation_snapshot(
    db: AsyncSession, release: EvidenceReleaseIdentity
) -> tuple[ProfileAsset, GenerationManifest]:
    profiles = AssetSnapshotReader(db, profile_assets, ProfileAsset)
    manifest = await resolve_generation_assets(
        release,
        profiles,
        AssetSnapshotReader(db, prompt_assets, PromptAsset),
        AssetSnapshotReader(db, route_assets, RouteAsset),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
    )
    profile = await profiles.get(release.profile.id, release.profile.version)
    if profile is None:
        raise ValueError("Profile disappeared from asset snapshot")
    return profile, manifest
