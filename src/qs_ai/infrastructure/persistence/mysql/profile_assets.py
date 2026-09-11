from dataclasses import asdict

from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from qs_ai.domain.governance.profile import AssetConflict, ProfileAsset
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import profile_assets


class MySQLProfileAssets:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def put(self, asset: ProfileAsset, source_ref: str, imported_by: str) -> bool:
        if any(not value.strip() or len(value) > 255 for value in (source_ref, imported_by)):
            raise ValueError("Profile import provenance is required")
        try:
            async with self.transactions.open() as db:
                await db.execute(
                    insert(profile_assets).values(
                        **asdict(asset), source_ref=source_ref, imported_by=imported_by
                    )
                )
                await db.commit()
            return True
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
        # Read in a fresh transaction after the competing insert has committed.
        # Never use an upsert that could overwrite content or its original audit.
        existing = await self.get(asset.profile_id, asset.version)
        if existing != asset:
            raise AssetConflict("Profile version already has different content")
        return False

    async def get(self, profile_id: str, version: str) -> ProfileAsset | None:
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(profile_assets).where(
                            profile_assets.c.profile_id == profile_id,
                            profile_assets.c.version == version,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return ProfileAsset(
            row["profile_id"], row["version"], row["fingerprint"], row["definition_json"]
        )
