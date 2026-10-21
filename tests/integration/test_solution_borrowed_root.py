"""Borrowed Solution root stages original revision/receipt/assets/Run without committing."""

from uuid import uuid4

import pytest

from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql import solution_assets
from qs_ai.infrastructure.persistence.mysql.solution_assets import PreparationIDs
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
async def test_original_borrowed_root_keeps_solution_receipt_assets_and_run_atomic(
    workspace, monkeypatch, mode, commit
):
    tx, store, scope, solution_id, create, at = workspace
    initial = await store.apply(scope, solution_id, create, at)
    command = PrepareSolution(
        command_id=uuid4(), expected_revision=initial["revision"], reason="原准备意图"
    )
    ids = PreparationIDs.freeze(solution_id, command.command_id)
    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
        original = db.get_transaction()
        with monkeypatch.context() as patch:
            patch.setattr(
                solution_assets,
                "uuid5",
                lambda *_: (_ for _ in ()).throw(
                    AssertionError("No new UUID in borrowed business root")
                ),
            )
            result = await store.apply_in(
                db, scope, solution_id, command, at, preparation_ids=ids, execution_mode=mode
            )
        assert result.wrote and result.value["prepared"]["run_id"] == str(ids.run_id)
        assert db.get_transaction() is original and original.is_active
        assert await rows(tx, ids.run_id) == [None, None, None]
        with pytest.raises(NotFound):
            await store.receipt(scope, command.command_id)
        assert (await store.get(scope, solution_id))["revision"] == initial["revision"]
        if commit:
            await db.commit()
    actual = await rows(tx, ids.run_id)
    current = await store.get(scope, solution_id)
    if commit:
        assert current["revision"] == initial["revision"] + 1
        assert current["prepared"] == result.value["prepared"]
        assert await store.receipt(scope, command.command_id) == result.value
        assert actual[0]["execution_mode"] == mode
        assert actual[2]["version"] == 1
    else:
        assert actual == [None, None, None]
        assert current["revision"] == initial["revision"] and not current["prepared"]
        with pytest.raises(NotFound):
            await store.receipt(scope, command.command_id)
