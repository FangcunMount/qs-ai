"""Catalog discovery over real MySQL with native and imported immutable configuration."""

import hashlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, update

from qs_ai.application.governance.asset_catalog import CatalogQuery
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.persistence.mysql.asset_catalog import TABLES, MySQLAssetCatalog
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_suites, profile_assets
from qs_ai.infrastructure.qs_server.evaluation_suite import SUITE_FILES
from tests.integration.test_evaluation_suites import assets as assets
from tests.integration.test_evaluation_suites import complete_release as complete_release
from tests.integration.test_evaluation_suites import evaluation_release as evaluation_release
from tests.integration.test_evaluation_suites import persisted_assets as persisted_assets
from tests.integration.test_evaluation_suites import registration as registration
from tests.integration.test_evaluation_suites import setup_run as setup_run
from tests.integration.test_evaluation_suites import suite_registration as suite_registration

pytestmark = pytest.mark.integration


@pytest.fixture
async def catalog(suite_registration):
    tx, store, scope, command, at, *_ = suite_registration
    receipt = await store.register(scope, command, at)
    return MySQLAssetCatalog(tx), scope, receipt


async def all_items(store, scope, kind, identity="", limit=1):
    result, cursor = [], ""
    for _ in range(100):
        page = await store.list(scope, CatalogQuery(kind, identity, limit, cursor))
        result.extend(page.items)
        if not page.next_cursor:
            return result
        assert page.items and page.next_cursor != cursor
        cursor = page.next_cursor
    raise AssertionError("Pagination did not terminate")


@pytest.mark.parametrize("kind", ["profile", "prompt", "route", "schema", "suite"])
async def test_discover_all_kinds_detail_matches_summary_and_shared_scope(catalog, kind):
    store, scope, receipt = catalog
    items = await all_items(store, scope, kind)
    assert items
    keys = [(item.reference.identity, item.reference.version) for item in items]
    assert keys == sorted(set(keys))
    shared = await all_items(store, DraftScope(2, 43), kind, limit=50)
    if kind == "suite":
        assert shared == [item for item in items if item.reference.identity != receipt.suite.id]
        with pytest.raises(NotFound):
            await store.get(DraftScope(2, 43), kind, receipt.suite.id, receipt.suite.version)
    else:
        assert items == shared
    for item in items:
        value = await store.get(scope, kind, item.reference.identity, item.reference.version)
        assert value.item == item
        assert (
            hashlib.sha256(value.definition_json.encode()).hexdigest()
            == item.reference.content_sha256
        )
        assert not hasattr(value, "registered_by") and not hasattr(value, "receipt")
    if kind == "suite":
        assert {(ref.id, ref.version) for ref in SUITE_FILES} < set(keys)
        assert (receipt.suite.id, receipt.suite.version) in keys
    if kind == "profile":
        assert (receipt.manifest.profile.identity, receipt.manifest.profile.version) in keys


async def test_binary_keyset_unicode_versions_and_exact_identity_filter(
    catalog, suite_registration
):
    store, scope, _ = catalog
    tx = suite_registration[0]
    identities = ["目录/" + str(uuid4()) + suffix for suffix in ("A", "a", "%_")]
    versions = ["v1", "v10", "v2", "V1"]
    try:
        for identity in identities:
            for version in versions:
                raw = json.dumps({"profile_id": identity, "version": version}, ensure_ascii=False)
                asset = ProfileAsset(
                    identity, version, "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), raw
                )
                await MySQLProfileAssets(tx).put(asset, "catalog-test", "synthetic")
        for identity in identities:
            items = await all_items(store, scope, "profile", identity)
            assert [(i.reference.identity, i.reference.version) for i in items] == [
                (identity, v) for v in sorted(versions)
            ]
        assert not (await store.list(scope, CatalogQuery("profile", "%"))).items
        assert not (await store.list(scope, CatalogQuery("profile", identities[0].upper()))).items
        with pytest.raises(NotFound):
            await store.get(scope, "profile", identities[0].upper(), "v1")
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(profile_assets).where(profile_assets.c.profile_id.in_(identities))
            )
            await db.commit()


async def test_keyset_new_version_does_not_repeat_previous_page(catalog, suite_registration):
    store, scope, _ = catalog
    tx = suite_registration[0]
    identity = "catalog-" + str(uuid4())

    async def put(version):
        raw = json.dumps({"profile_id": identity, "version": version})
        await MySQLProfileAssets(tx).put(
            ProfileAsset(
                identity, version, "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), raw
            ),
            "test",
            "synthetic",
        )

    try:
        await put("v1")
        await put("v3")
        first = await store.list(scope, CatalogQuery("profile", identity, 1))
        await put("v2")
        second = await store.list(scope, CatalogQuery("profile", identity, 1, first.next_cursor))
        third = await store.list(scope, CatalogQuery("profile", identity, 1, second.next_cursor))
        assert [page.items[0].reference.version for page in (first, second, third)] == [
            "v1",
            "v2",
            "v3",
        ]
        assert third.next_cursor == ""
    finally:
        async with tx.open() as db:
            await db.execute(delete(profile_assets).where(profile_assets.c.profile_id == identity))
            await db.commit()


@pytest.mark.parametrize("kind", ["profile", "prompt", "route", "schema", "suite"])
async def test_corrupt_asset_cannot_be_advertised_or_read(catalog, suite_registration, kind):
    store, scope, receipt = catalog
    tx = suite_registration[0]
    if kind == "suite":
        identity, version = receipt.suite.id, receipt.suite.version
    else:
        reference = {
            "profile": receipt.manifest.profile,
            "prompt": receipt.manifest.prompt,
            "route": receipt.manifest.generation_route,
            "schema": receipt.manifest.input_schema,
        }[kind]
        identity, version = reference.identity, reference.version
    table = TABLES[kind]
    original = await store.get(scope, kind, identity, version)
    first, second = list(table.primary_key.columns)
    column = "package_json" if kind == "prompt" else "definition_json"
    try:
        async with tx.open() as db:
            await db.execute(
                update(table).where(first == identity, second == version).values(**{column: "{}"})
            )
            await db.commit()
        with pytest.raises(ValueError):
            await store.get(scope, kind, identity, version)
        with pytest.raises(ValueError):
            await store.list(scope, CatalogQuery(kind, identity))
    finally:
        async with tx.open() as db:
            await db.execute(
                update(table)
                .where(first == identity, second == version)
                .values(**{column: original.definition_json})
            )
            await db.commit()


async def test_invalid_identity_missing_and_corrupt_registration(catalog, suite_registration):
    store, scope, receipt = catalog
    tx = suite_registration[0]
    with pytest.raises(NotFound):
        await store.get(scope, "profile", "missing-profile", "v1")
    with pytest.raises(ValueError):
        await store.get(scope, "profile", "x", "v1 ")
    with pytest.raises(ValueError):
        await store.list(replace(scope, organization_id=0), CatalogQuery("profile"))
    async with tx.open() as db:
        await db.execute(
            update(evaluation_suites)
            .where(evaluation_suites.c.suite_id == receipt.suite.id)
            .values(receipt_sha256="0" * 64)
        )
        await db.commit()
    with pytest.raises(ValueError):
        await store.get(scope, "suite", receipt.suite.id, receipt.suite.version)
