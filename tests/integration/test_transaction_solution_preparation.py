"""Original Solution/assets/Run and exact witness atomically share the SDK root; no Broker claim."""

from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import delete, text

from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql import solution_assets
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport import solution_preparation_state as preparation
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationInput,
    read_preparation_plan,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_solutions import workspace as workspace
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
@pytest.mark.parametrize(
    "fault",
    ["none", "archive_rejected", "witness_rejected", "client_reply_discarded", "source_changed"],
)
async def test_original_solution_complete_assets_receipt_and_first_wire_share_exact_witness(
    host, workspace, trigger_admin, monkeypatch, mode, fault
):
    _, previous, scope, solution_id, create, at = workspace
    settings = previous.settings.model_copy(
        update={
            "evaluation": previous.settings.evaluation.model_copy(
                update={"candidate_mode_enabled": mode == "candidate_v2"}
            )
        }
    )
    original = MySQLSolutions(host.tx, settings)
    initial = await original.apply(scope, solution_id, create, at)
    command = PrepareSolution(
        command_id=uuid4(), expected_revision=initial["revision"], reason="原准备意图"
    )
    incoming = SolutionPreparationInput(scope, solution_id, command)
    candidate = await preparation.SolutionPreparationPlanner(
        original,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    ).candidate(incoming, at)
    assert candidate is not None
    reservation = await host.store.reserve(incoming.intent.request, candidate.winner)
    assert reservation.acquired
    owned = reservation.execution
    host.executions.append(owned)
    _, _, first = read_preparation_plan(incoming, owned.plan)
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "solution_root_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated Solution archive rejection'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated Solution witness rejection'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        if fault == "source_changed":
            concurrent = PrepareSolution(
                command_id=uuid4(), expected_revision=initial["revision"], reason="并发原准备"
            )
            await original.apply(scope, solution_id, concurrent, at)
        root = preparation.SolutionPreparationRoot(
            original, host.archives, host.store, incoming, owned.plan, candidate
        )
        with monkeypatch.context() as patch:
            patch.setattr(
                solution_assets,
                "uuid5",
                lambda *_: (_ for _ in ()).throw(
                    AssertionError("No UUID generation in original business callback")
                ),
            )
            patch.setattr(
                preparation,
                "prepare",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    AssertionError("No Seal in original business callback")
                ),
            )
            if fault == "client_reply_discarded":
                actual_commit = EventSession.commit

                async def discarded(db):
                    await actual_commit(db)
                    raise ConnectionError("client discarded completed Solution commit reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        actual = await rows(host.tx, incoming.identities.run_id)
        current = await original.get(scope, solution_id)
        if committed:
            assert current["revision"] == initial["revision"] + 1
            assert current["prepared"] == candidate.assets.prepared()
            assert actual[0]["definition_json"] == candidate.assets.creation.definition_json
            assert actual[0]["execution_mode"] == mode
            assert await original.receipt(scope, command.command_id) == root.value
        else:
            with pytest.raises(NotFound):
                await original.receipt(scope, command.command_id)
            if fault == "source_changed":
                assert current["revision"] == initial["revision"] + 1 and actual[0] is not None
                assert await original.receipt(scope, concurrent.command_id) == current
            else:
                assert current["revision"] == initial["revision"] and not current["prepared"]
                assert actual == [None, None, None]
        assert await host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
        # A captured business view may predate a failed archive/witness write;
        # only the outcome/evidence above proves whether it can be returned.
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(
                    evaluation_sequences.c.run_id == str(incoming.identities.run_id)
                )
            )
