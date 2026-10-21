"""Complete asset+Run preview is read-only; original caller owns atomic persistence."""

from uuid import uuid4

import pytest
from sqlalchemy import event

from qs_ai.infrastructure.persistence.mysql import solution_assets
from qs_ai.infrastructure.persistence.mysql.asset_records import (
    profile_assets,
    prompt_assets,
    route_assets,
)
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.solution_preparation import plan_prepare_assets
from qs_ai.infrastructure.persistence.mysql.solutions import read_state
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_solutions import workspace as workspace
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
@pytest.mark.parametrize("commit", [False, True])
async def test_complete_preparation_plan_reads_only_then_original_assets_and_run_commit_together(
    workspace, monkeypatch, mode, commit
):
    tx, store, scope, solution_id, create, at = workspace
    await store.apply(scope, solution_id, create, at)
    ids = solution_assets.PreparationIDs.freeze(solution_id, uuid4())
    engine = tx.database.engine
    statements, planning = [], False

    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if planning:
            statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with tx.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            original = db.get_transaction()
            state = await read_state(db, scope, solution_id)
            state["reason"] = "原准备意图"
            planning = True
            try:
                plan = await plan_prepare_assets(
                    db,
                    scope,
                    state,
                    ids.command_id,
                    at,
                    store.settings.governance_models,
                    store.settings.models,
                    execution_mode=mode,
                    identities=ids,
                )
            finally:
                planning = False
            assert statements and all(v.lstrip().upper().startswith("SELECT") for v in statements)
            assert not any("FOR UPDATE" in v.upper() for v in statements)
            assert db.get_transaction() is original and original.is_active
            assert await rows(tx, ids.run_id) == [None, None, None]
            first_digest = plan.digest
            assert len(first_digest) == 32
            with monkeypatch.context() as patch:
                patch.setattr(
                    solution_assets,
                    "uuid5",
                    lambda *_: (_ for _ in ()).throw(
                        AssertionError("No new UUID in original business callback")
                    ),
                )
                result = await solution_assets.prepare_assets(
                    db,
                    scope,
                    state,
                    ids.command_id,
                    at,
                    store.settings.governance_models,
                    store.settings.models,
                    execution_mode=mode,
                    identities=ids,
                )
            assert result == plan.prepared() and plan.digest == first_digest
            assert db.get_transaction() is original and original.is_active
            if commit:
                await db.commit()
        actual = await rows(tx, ids.run_id)
        if commit:
            assert actual[0]["definition_json"] == plan.creation.definition_json
            assert actual[0]["execution_mode"] == mode
            assert actual[1]["definition_json"] == plan.creation.policy.definition_json
            assert actual[2]["version"] == 1 and actual[2]["checkpoint_json"] is None
        else:
            assert actual == [None, None, None]
        async with tx.open() as db:
            for table, expected in (
                (prompt_assets, plan.prompt.asset),
                (route_assets, plan.generation),
                (route_assets, plan.semantic),
                (profile_assets, plan.profile.profile),
            ):
                reader = AssetSnapshotReader(db, table, type(expected))
                ref = {
                    "PromptAsset": plan.prompt.receipt.asset,
                    "ProfileAsset": plan.profile.receipt.manifest.profile,
                }.get(type(expected).__name__)
                identity = ref.identity if ref is not None else expected.route
                version = ref.version if ref is not None else expected.revision
                assert await reader.get(identity, version) == (expected if commit else None)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
