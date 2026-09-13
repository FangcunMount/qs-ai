import asyncio
import hashlib
import json
import os
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from qs_ai.bootstrap.import_routes import baseline_assets
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema import route_assets

pytestmark = pytest.mark.integration


@pytest.fixture(params=[False, True], ids=["generation", "semantic"])
async def asset_store(request):
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires disposable MySQL with migrations applied")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    baseline = baseline_assets(include_evaluation=request.param)[1][-1]
    data = json.loads(baseline.definition_json)
    data["route"] = str(uuid4())
    raw = json.dumps(data)
    asset = RouteAsset(
        data["route"], data["revision"], "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), raw
    )
    try:
        yield MySQLRouteAssets(transactions), asset, transactions
    finally:
        async with transactions.open() as db:
            await db.execute(delete(route_assets).where(route_assets.c.route == asset.route))
            await db.commit()
        await database.close()


async def test_replay_conflict_and_history_preserve_import_audit(asset_store):
    store, asset, transactions = asset_store
    results = await asyncio.gather(*(store.put(asset, "original", "operator") for _ in range(5)))
    assert results.count(True) == 1
    assert await store.get(asset.route, asset.revision) == asset
    assert not await store.put(asset, "other", "another")
    changed = asset.definition_json + " "
    with pytest.raises(AssetConflict):
        await store.put(
            replace(
                asset,
                definition_json=changed,
                fingerprint="sha256:" + hashlib.sha256(changed.encode()).hexdigest(),
            ),
            "other",
            "another",
        )
    data = json.loads(asset.definition_json)
    data["revision"] = "v9"
    raw = json.dumps(data)
    next_asset = replace(
        asset,
        revision="v9",
        definition_json=raw,
        fingerprint="sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
    )
    assert await store.put(next_asset, "next", "operator")
    assert await store.get(asset.route, "v9") == next_asset
    assert await store.get(asset.route, asset.revision) == asset
    async with transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(route_assets).where(
                        route_assets.c.route == asset.route,
                        route_assets.c.revision == asset.revision,
                    )
                )
            )
            .mappings()
            .one()
        )
    assert row["source_ref"] == "original"
    assert row["imported_by"] == "operator"
