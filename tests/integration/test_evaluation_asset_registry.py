"""Exact asset storage must not silently replace versions or cross organization boundaries."""

import hashlib
import os
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, update

from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.import_evaluation_assets import baseline_assets
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import (
    MySQLEvaluationAssets,
    read_policy,
    read_semantic_prompt,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_policy_assets,
    semantic_prompt_assets,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def registry():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    store = MySQLEvaluationAssets(Transactions(database))
    source = f"test:{uuid4()}"
    try:
        yield store, source
    finally:
        async with store.transactions.open() as db:
            for table in (evaluation_policy_assets, semantic_prompt_assets):
                await db.execute(delete(table).where(table.c.source_ref == source))
            await db.commit()
        await database.close()


def unique_policy():
    _, policies, _, _ = baseline_assets()
    asset = policies[0]
    identity = f"test-policy-{uuid4()}"
    raw = asset.definition_json.replace(asset.reference.id, identity)
    return replace(
        asset,
        reference=FrozenContractRef(
            identity, asset.reference.version, "sha256:" + hashlib.sha256(raw.encode()).hexdigest()
        ),
        definition_json=raw,
    )


async def test_exact_policy_idempotency_collision_and_corruption(registry):
    store, source = registry
    asset = unique_policy()
    assert await store.put_policy(asset, source, "test")
    assert not await store.put_policy(asset, source, "test-replay")
    raw = asset.definition_json + "\n"
    changed = replace(
        asset,
        definition_json=raw,
        reference=replace(
            asset.reference, fingerprint="sha256:" + hashlib.sha256(raw.encode()).hexdigest()
        ),
    )
    with pytest.raises(AssetConflict):
        await store.put_policy(changed, source, "test")
    async with store.transactions.open() as db:
        assert await read_policy(db, asset.kind, asset.reference) == asset
        with pytest.raises(NotFound):
            await read_policy(db, asset.kind, replace(asset.reference, version="missing"))
        await db.execute(
            update(evaluation_policy_assets)
            .where(evaluation_policy_assets.c.asset_id == asset.reference.id)
            .values(definition_json=raw)
        )
        await db.commit()
    async with store.transactions.open() as db:
        with pytest.raises(ValueError, match="content mismatch"):
            await read_policy(db, asset.kind, asset.reference)


async def test_semantic_owner_is_explicit_and_never_falls_back(registry):
    store, source = registry
    _, _, baseline, _ = baseline_assets()
    prompt = replace(
        baseline,
        reference=replace(baseline.reference, id=f"test-prompt-{uuid4()}"),
        organization_id=52,
    )
    assert await store.put_semantic_prompt(prompt, source, "test")
    assert not await store.put_semantic_prompt(prompt, source, "test")
    async with store.transactions.open() as db:
        assert (
            await read_semantic_prompt(
                db, prompt.reference, owner_organization_id=52, requesting_organization_id=52
            )
            == prompt
        )
        with pytest.raises(NotFound):
            await read_semantic_prompt(
                db, prompt.reference, owner_organization_id=52, requesting_organization_id=53
            )
        with pytest.raises(NotFound):
            await read_semantic_prompt(
                db, prompt.reference, owner_organization_id=0, requesting_organization_id=52
            )


async def test_policy_catalog_keeps_kinds_separate_and_exact_versions(registry):
    from qs_ai.application.governance.asset_catalog import CatalogQuery
    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.infrastructure.persistence.mysql.asset_catalog import MySQLAssetCatalog

    store, source = registry
    asset = unique_policy()
    await store.put_policy(asset, source, "test")
    catalog = MySQLAssetCatalog(store.transactions)
    scope = DraftScope(52, 42)
    page = await catalog.list(scope, CatalogQuery("execution_policy", asset.reference.id))
    assert len(page.items) == 1
    value = await catalog.get(
        scope, "execution_policy", asset.reference.id, asset.reference.version
    )
    assert value.item == page.items[0]
    assert value.definition_json == asset.definition_json
    assert value.item.reference.fingerprint == asset.reference.fingerprint
    assert not (await catalog.list(scope, CatalogQuery("gate_policy", asset.reference.id))).items
    with pytest.raises(NotFound):
        await catalog.get(scope, "gate_policy", asset.reference.id, asset.reference.version)
    with pytest.raises(NotFound):
        await catalog.get(scope, "execution_policy", asset.reference.id, "missing")
