import asyncio
import hashlib
import json
import os
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from qs_ai.bootstrap.import_profiles import baseline_assets
from qs_ai.domain.governance.profile import AssetConflict, ProfileAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.schema import profile_assets
from qs_ai.infrastructure.qs_server.profiles import canonical_definition

pytestmark = pytest.mark.integration


def asset_version(asset, version, *, max_characters=8000):
    definition = json.loads(asset.definition_json)
    definition["version"] = version
    definition["generation_policy"]["max_output_characters"] = max_characters
    raw = canonical_definition(definition)
    return ProfileAsset(
        asset.profile_id, version, "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), raw
    )


@pytest.fixture
async def asset_store():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires disposable MySQL with migrations applied")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    _, baseline = baseline_assets()
    definition = json.loads(baseline[0].definition_json)
    definition["profile_id"] = str(uuid4())
    raw = canonical_definition(definition)
    asset = ProfileAsset(
        definition["profile_id"],
        definition["version"],
        "sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
        raw,
    )
    try:
        yield MySQLProfileAssets(transactions), asset, transactions
    finally:
        async with transactions.open() as db:
            await db.execute(
                delete(profile_assets).where(profile_assets.c.profile_id == asset.profile_id)
            )
            await db.commit()
        await database.close()


async def test_concurrent_identical_import_preserves_original_audit(asset_store):
    store, asset, transactions = asset_store
    results = await asyncio.gather(*(store.put(asset, "source", "operator") for _ in range(5)))
    assert results.count(True) == 1
    assert await store.get(asset.profile_id, asset.version) == asset
    assert not await store.put(asset, "different-source", "different-operator")
    async with transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(profile_assets).where(profile_assets.c.profile_id == asset.profile_id)
                )
            )
            .mappings()
            .one()
        )
    assert row["source_ref"] == "source"
    assert row["imported_by"] == "operator"


async def test_conflicting_content_cannot_replace_version_or_history(asset_store):
    store, asset, _ = asset_store
    await store.put(asset, "source", "operator")
    changed = asset_version(asset, asset.version, max_characters=9000)
    with pytest.raises(AssetConflict):
        await store.put(changed, "source", "operator")
    newer = asset_version(asset, "v7", max_characters=9000)
    assert await store.put(newer, "source", "operator")
    assert await store.get(asset.profile_id, asset.version) == asset
    assert await store.get(asset.profile_id, "v7") == newer
    assert await store.get(asset.profile_id, "missing") is None


async def test_invalid_audit_does_not_insert(asset_store):
    store, asset, _ = asset_store
    with pytest.raises(ValueError):
        await store.put(asset, "", "operator")
    assert await store.get(asset.profile_id, asset.version) is None
    with pytest.raises(ValueError):
        replace(asset, fingerprint="sha256:" + "0" * 64)


async def test_concurrent_conflicting_import_has_one_immutable_winner(asset_store):
    store, asset, _ = asset_store
    changed = asset_version(asset, asset.version, max_characters=9000)
    outcomes = await asyncio.gather(
        store.put(asset, "source-a", "operator-a"),
        store.put(changed, "source-b", "operator-b"),
        return_exceptions=True,
    )
    assert sum(result is True for result in outcomes) == 1
    assert sum(isinstance(result, AssetConflict) for result in outcomes) == 1
    expected = asset if outcomes[0] is True else changed
    assert await store.get(asset.profile_id, asset.version) == expected
