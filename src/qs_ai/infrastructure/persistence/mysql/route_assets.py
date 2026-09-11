from dataclasses import asdict

from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import route_assets


class MySQLRouteAssets:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def put(self, asset: RouteAsset, source_ref: str, imported_by: str) -> bool:
        if any(not value.strip() or len(value) > 255 for value in (source_ref, imported_by)):
            raise ValueError("Route import provenance is required")
        try:
            async with self.transactions.open() as db:
                await db.execute(
                    insert(route_assets).values(
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
        existing = await self.get(asset.route, asset.revision)
        if existing != asset:
            raise AssetConflict("Route revision already has different content")
        return False

    async def get(self, route: str, revision: str) -> RouteAsset | None:
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(route_assets).where(
                            route_assets.c.route == route,
                            route_assets.c.revision == revision,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return RouteAsset(row["route"], row["revision"], row["fingerprint"], row["definition_json"])
