import asyncio
import hashlib
import json
import os
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from qs_ai.bootstrap.import_prompts import baseline_assets
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.schema import prompt_assets

pytestmark = pytest.mark.integration


@pytest.fixture
async def asset_store():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires disposable MySQL with migrations applied")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    data = json.loads(baseline_assets()[1][0].package_json)
    data["Ref"]["TemplateID"] = str(uuid4())
    raw = json.dumps(data, ensure_ascii=False)
    asset = PromptAsset(
        data["Ref"]["TemplateID"],
        data["Ref"]["Version"],
        data["Ref"]["Fingerprint"],
        hashlib.sha256(raw.encode()).hexdigest(),
        raw,
    )
    try:
        yield MySQLPromptAssets(transactions), asset, transactions
    finally:
        async with transactions.open() as db:
            await db.execute(
                delete(prompt_assets).where(prompt_assets.c.template_id == asset.template_id)
            )
            await db.commit()
        await database.close()


async def test_replay_conflict_and_history_preserve_import_audit(asset_store):
    store, asset, transactions = asset_store
    results = await asyncio.gather(*(store.put(asset, "original", "operator") for _ in range(5)))
    assert results.count(True) == 1
    assert await store.get(asset.template_id, asset.version) == asset
    assert not await store.put(asset, "other", "another")
    changed = asset.package_json + " "
    with pytest.raises(AssetConflict):
        await store.put(
            replace(
                asset,
                package_json=changed,
                package_sha256=hashlib.sha256(changed.encode()).hexdigest(),
            ),
            "other",
            "another",
        )
    data = json.loads(asset.package_json)
    data["Ref"]["Version"] = "v2"
    raw = json.dumps(data)
    next_asset = replace(
        asset,
        version="v2",
        package_json=raw,
        package_sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )
    assert await store.put(next_asset, "next", "operator")
    assert await store.get(asset.template_id, "v2") == next_asset
    assert await store.get(asset.template_id, "v1") == asset
    async with transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(prompt_assets).where(
                        prompt_assets.c.template_id == asset.template_id,
                        prompt_assets.c.version == "v1",
                    )
                )
            )
            .mappings()
            .one()
        )
    assert row["source_ref"] == "original"
    assert row["imported_by"] == "operator"
