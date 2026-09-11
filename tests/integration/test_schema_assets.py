import asyncio
import hashlib
import json
import os
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from qs_ai.bootstrap.import_schemas import baseline_assets
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.schema import schema_assets
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets

pytestmark = pytest.mark.integration


@pytest.fixture
async def asset_store():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires disposable MySQL with migrations applied")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    baseline = baseline_assets()[1][0]
    data = json.loads(baseline.definition_json)
    data["schema_id"] = str(uuid4())
    data["properties"]["schema_version"] = {"const": data["schema_id"] + "/v1"}
    raw = json.dumps(data)
    asset = SchemaAsset(
        data["schema_id"], "v1", "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), raw
    )
    try:
        yield MySQLSchemaAssets(transactions), asset, transactions
    finally:
        async with transactions.open() as db:
            await db.execute(
                delete(schema_assets).where(schema_assets.c.schema_id == asset.schema_id)
            )
            await db.commit()
        await database.close()


async def test_replay_conflict_and_history_preserve_import_audit(asset_store):
    store, asset, transactions = asset_store
    results = await asyncio.gather(*(store.put(asset, "original", "operator") for _ in range(5)))
    assert results.count(True) == 1
    assert await store.get(asset.schema_id, asset.version) == asset
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
    data["properties"]["schema_version"] = {"const": asset.schema_id + "/v2"}
    raw = json.dumps(data)
    next_asset = replace(
        asset,
        version="v2",
        definition_json=raw,
        fingerprint="sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
    )
    assert await store.put(next_asset, "next", "operator")
    assert await store.get(asset.schema_id, "v2") == next_asset
    assert await store.get(asset.schema_id, "v1") == asset
    async with transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(schema_assets).where(
                        schema_assets.c.schema_id == asset.schema_id,
                        schema_assets.c.version == "v1",
                    )
                )
            )
            .mappings()
            .one()
        )
    assert row["source_ref"] == "original"
    assert row["imported_by"] == "operator"
