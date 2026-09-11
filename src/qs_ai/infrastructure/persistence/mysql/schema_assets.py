from dataclasses import asdict

from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import schema_assets


class MySQLSchemaAssets:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def put(self, asset: SchemaAsset, source_ref: str, imported_by: str) -> bool:
        if any(not value.strip() or len(value) > 255 for value in (source_ref, imported_by)):
            raise ValueError("Schema import provenance is required")
        try:
            async with self.transactions.open() as db:
                await db.execute(
                    insert(schema_assets).values(
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
        existing = await self.get(asset.schema_id, asset.version)
        if existing != asset:
            raise AssetConflict("Schema version already has different content")
        return False

    async def get(self, schema_id: str, version: str) -> SchemaAsset | None:
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(schema_assets).where(
                            schema_assets.c.schema_id == schema_id,
                            schema_assets.c.version == version,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return SchemaAsset(
            row["schema_id"], row["version"], row["fingerprint"], row["definition_json"]
        )
