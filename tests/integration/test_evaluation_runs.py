import json
import os
from dataclasses import fields
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_run_policies,
    evaluation_runs,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import V6

pytestmark = pytest.mark.integration


@pytest.fixture
async def setup_run():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx = Transactions(database)
    run_id = uuid4()
    execution = load_execution_policy()
    # Placeholder asset refs isolate storage atomicity; this is not an eligible release.
    refs = {
        f.name: FrozenContractRef(f.name, "v1", "sha256:" + "a" * 64)
        for f in fields(EvidenceReleaseIdentity)
    }
    refs.update(
        suite=V6,
        execution_policy=FrozenContractRef(
            execution.policy_id, execution.version, execution.fingerprint
        ),
        gate_policy=load_gate_policy().reference,
    )
    release = EvidenceReleaseIdentity(**refs)
    try:
        yield tx, run_id, release
    finally:
        async with tx.open() as db:
            for table in (evaluation_checkpoints, evaluation_run_policies, evaluation_runs):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()
        await database.close()


async def rows(tx, run_id):
    async with tx.open() as db:
        return [
            (await db.execute(select(table).where(table.c.run_id == str(run_id))))
            .mappings()
            .one_or_none()
            for table in (evaluation_runs, evaluation_run_policies, evaluation_checkpoints)
        ]


async def create(db, run_id, release):
    return await create_run(
        db, run_id, release, 1, "actor:1", "评测迁移验证", datetime(2026, 9, 12, tzinfo=UTC)
    )


async def test_creation_freezes_documents_slots_audit_and_one_initial_version(setup_run):
    tx, run_id, release = setup_run
    async with tx.open() as db:
        state = await create(db, run_id, release)
        await db.commit()
    run, policy, checkpoint = await rows(tx, run_id)
    definition = json.loads(run["definition_json"])
    assert len(definition["slots"]) == 35
    assert definition["preflight"] == {"case_id": "PROMPT-EVAL-008", "status": "pending"}
    assert definition["release_fingerprint"] == release.fingerprint()
    assert definition["execution_policy_json"] == policy["definition_json"]
    assert definition["gate_policy_json"] == load_gate_policy().definition_json
    assert definition["audit"]["requested_by"] == run["requested_by"] == "actor:1"
    assert state.version == checkpoint["version"] == 1
    assert checkpoint["checkpoint_json"] is None
    before = await rows(tx, run_id)
    with pytest.raises(IntegrityError):
        async with tx.open() as db:
            await create(db, run_id, release)
            await db.commit()
    assert await rows(tx, run_id) == before


@pytest.mark.parametrize("conflict", ["policy", "checkpoint", "after_writes"])
async def test_failure_at_each_creation_write_rolls_back_all_new_records(setup_run, conflict):
    tx, run_id, release = setup_run
    async with tx.open() as db:
        if conflict == "policy":
            await db.execute(
                insert(evaluation_run_policies).values(
                    run_id=str(run_id), fingerprint="prior", definition_json="{}"
                )
            )
        elif conflict == "checkpoint":
            await db.execute(insert(evaluation_checkpoints).values(run_id=str(run_id), version=9))
        await db.commit()
    before = await rows(tx, run_id)
    with pytest.raises((IntegrityError, RuntimeError)):
        async with tx.open() as db:
            await create(db, run_id, release)
            raise RuntimeError("injected before commit")
    assert await rows(tx, run_id) == before


async def test_creation_resolves_real_mysql_assets_and_freezes_complete_release(setup_run):
    from dataclasses import replace

    from qs_ai.application.interpretation.manifest import build_generation_manifest
    from qs_ai.bootstrap.import_profiles import baseline_assets as profiles
    from qs_ai.bootstrap.import_prompts import baseline_assets as prompts
    from qs_ai.bootstrap.import_routes import baseline_assets as routes
    from qs_ai.bootstrap.import_schemas import baseline_assets as schemas
    from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
    from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
    from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
    from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
    from qs_ai.infrastructure.persistence.mysql.schema import (
        profile_assets,
        prompt_assets,
        route_assets,
        schema_assets,
    )
    from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
    from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets

    tx, run_id, partial = setup_run
    stores = (
        MySQLProfileAssets(tx),
        MySQLPromptAssets(tx),
        MySQLRouteAssets(tx),
        MySQLSchemaAssets(tx),
    )
    tables = (profile_assets, prompt_assets, route_assets, schema_assets)
    created = []
    try:
        for store, baseline, table in zip(
            stores, (profiles, prompts, routes, schemas), tables, strict=True
        ):
            source, assets = baseline()
            keys = list(table.primary_key.columns)
            for asset in assets:
                inserted = await store.put(asset, source, "integration:run-creation")
                if inserted:
                    condition = (keys[0] == getattr(asset, keys[0].name)) & (
                        keys[1] == getattr(asset, keys[1].name)
                    )
                    created.append((table, condition))
        manifest = await build_generation_manifest(
            *stores,
            profile_id="participant-scale-score-range-default",
            profile_version="v6",
            route_revision="v8",
        )
        generation = {}
        for name in ("profile", "prompt", "generation_route", "input_schema", "output_schema"):
            asset = getattr(manifest, name)
            version = (
                f"{asset.identity}/{asset.version}" if name.endswith("schema") else asset.version
            )
            generation[name] = FrozenContractRef(asset.identity, version, asset.fingerprint)
        semantic = load_semantic_assets()
        # Test route choice only: no claim about the production judge configuration.
        release = replace(
            partial,
            **generation,
            semantic_prompt=semantic.prompt,
            semantic_output_schema=semantic.output_schema,
            semantic_route=generation["generation_route"],
        )
        creator = MySQLRunCreator(tx, *stores)
        state = await creator.create(
            run_id, release, 1, "actor:1", "评测验证", datetime(2026, 9, 12, tzinfo=UTC)
        )
        run, policy, checkpoint = await rows(tx, run_id)
        frozen = json.loads(run["definition_json"])
        assert state.version == checkpoint["version"] == 1
        assert frozen["release_fingerprint"] == release.fingerprint()
        assert frozen["release"]["semantic_prompt"]["fingerprint"] == semantic.prompt.fingerprint
        assert frozen["execution_policy_json"] == policy["definition_json"]
        assert len(frozen["slots"]) == 35
        with pytest.raises(IntegrityError):
            await creator.create(
                run_id, release, 1, "actor:1", "评测验证", datetime(2026, 9, 12, tzinfo=UTC)
            )
        assert (await rows(tx, run_id))[2]["version"] == 1
    finally:
        async with tx.open() as db:
            for table, condition in reversed(created):
                await db.execute(delete(table).where(condition))
            await db.commit()


async def test_start_and_cancel_compete_on_same_run_version(setup_run):
    import asyncio

    from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
    from qs_ai.infrastructure.persistence.mysql.evaluation_progress import transition_requested

    tx, run_id, release = setup_run
    async with tx.open() as db:
        await create(db, run_id, release)
        await db.commit()
    frozen = (await rows(tx, run_id))[0]["definition_json"]

    async def transition(target):
        async with tx.open() as db:
            state = await transition_requested(
                db, run_id, 1, 1, target, "actor:1", "开始或取消", datetime(2026, 9, 12, tzinfo=UTC)
            )
            await db.commit()
            return state

    results = await asyncio.gather(
        transition("collecting"), transition("canceled"), return_exceptions=True
    )
    assert sum(isinstance(x, CheckpointState) for x in results) == 1
    assert sum(isinstance(x, CheckpointConflict) for x in results) == 1
    run, _, checkpoint = await rows(tx, run_id)
    assert run["definition_json"] == frozen
    assert checkpoint["version"] == 2
    assert run["progress_json"]["status"] in ("collecting", "canceled")
    assert len(run["progress_json"]["transitions"]) == 2
    with pytest.raises(CheckpointConflict):
        async with tx.open() as db:
            await transition_requested(
                db, run_id, 2, 1, "collecting", "actor:1", "重放", datetime(2026, 9, 12, tzinfo=UTC)
            )


async def test_initial_transition_rollback_and_wrong_organization_preserve_progress(setup_run):
    from qs_ai.application.evaluation.checkpoints import CheckpointConflict
    from qs_ai.infrastructure.persistence.mysql.evaluation_progress import transition_requested

    tx, run_id, release = setup_run
    async with tx.open() as db:
        await create(db, run_id, release)
        await db.commit()
    before = await rows(tx, run_id)
    with pytest.raises(RuntimeError):
        async with tx.open() as db:
            await transition_requested(
                db, run_id, 1, 1, "collecting", "actor:1", "开始", datetime(2026, 9, 12, tzinfo=UTC)
            )
            raise RuntimeError("before commit")
    assert await rows(tx, run_id) == before
    with pytest.raises(CheckpointConflict):
        async with tx.open() as db:
            await transition_requested(
                db, run_id, 1, 2, "canceled", "actor:1", "取消", datetime(2026, 9, 12, tzinfo=UTC)
            )
            await db.commit()
    assert await rows(tx, run_id) == before
