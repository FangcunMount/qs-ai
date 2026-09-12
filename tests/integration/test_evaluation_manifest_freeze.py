"""New evaluation creation freezes the actual persisted immutable asset manifest."""

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from qs_ai.application.evaluation.release import resolve_generation_assets
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator, create_run
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration


async def test_public_creator_freezes_actual_asset_bytes_with_run_transaction(
    setup_run, persisted_assets, complete_release
):
    tx, run_id, _ = setup_run
    stores = (
        MySQLProfileAssets(tx),
        MySQLPromptAssets(tx),
        MySQLRouteAssets(tx),
        MySQLSchemaAssets(tx),
    )
    manifest = await resolve_generation_assets(complete_release, *stores)
    state = await MySQLRunCreator(tx, *stores).create(
        run_id, complete_release, 1, "user:42", "冻结执行配置", datetime.now(UTC)
    )
    run, _, checkpoint = await rows(tx, run_id)
    definition = json.loads(run["definition_json"])
    assert state.version == checkpoint["version"] == 1
    assert definition["generation_manifest_json"] == manifest.canonical_json()
    assert definition["generation_manifest_fingerprint"] == manifest.fingerprint()
    assert definition["release_fingerprint"] == complete_release.fingerprint()


async def test_manifest_mismatch_and_rollback_never_leave_partial_run(
    setup_run, persisted_assets, complete_release
):
    tx, run_id, _ = setup_run
    stores = (
        MySQLProfileAssets(tx),
        MySQLPromptAssets(tx),
        MySQLRouteAssets(tx),
        MySQLSchemaAssets(tx),
    )
    manifest = await resolve_generation_assets(complete_release, *stores)
    changed = replace(manifest, prompt=replace(manifest.prompt, fingerprint="sha256:" + "0" * 64))
    with pytest.raises(ValueError):
        async with tx.open() as db:
            await create_run(
                db,
                run_id,
                complete_release,
                1,
                "user:42",
                "错误配置",
                datetime.now(UTC),
                generation_manifest=changed,
            )
            await db.commit()
    assert await rows(tx, run_id) == [None, None, None]
    async with tx.open() as db:
        await create_run(
            db,
            run_id,
            complete_release,
            1,
            "user:42",
            "回滚验证",
            datetime.now(UTC),
            generation_manifest=manifest,
        )
    assert await rows(tx, run_id) == [None, None, None]
