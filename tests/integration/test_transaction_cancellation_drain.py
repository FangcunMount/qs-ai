"""Original cancellation drain with actual MySQL roots; not a transport/network proof."""

import json
from datetime import timedelta
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import delete, select, text

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.cancellation_drain_state import (
    CancellationDrainPlanner,
    apply_cancellation_drain,
    read_cancellation_drain_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from tests.integration.test_evaluation_completions import accept, stored
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "client_reply_discarded", "coordinator_advanced"]
)
async def test_original_drain_business_audit_sequence_archive_and_witness_commit_together(
    host, dispatched, trigger_admin, monkeypatch, fault
):
    tx, run_id, completion, *_ = dispatched
    scope, at = ManagementScope(run_id, 1, 42), completion.finished_at
    management = MySQLEvaluationManagement(tx)
    requested = await management.cancel(
        scope, 5, "等待原调用完成后取消", at, discard=False, confirm=True
    )
    assert requested.cancel_draining and not requested.cancellation_json
    async with tx.open() as db:
        await accept(db, dispatched)
        await db.commit()
    before = await rows(tx, run_id)
    original_completion = await stored(tx, run_id)
    source_version = before[2]["version"]
    engine = host.tx.database.engine
    assert engine is not None
    host.tx.database.engine = engine.execution_options(isolation_level="READ COMMITTED")
    trigger = None
    try:
        planner = CancellationDrainPlanner(
            host.tx, signing_key=host.keys["ai.sign"], recipient_key=host.keys["qs.encrypt"]
        )
        operation = await planner.observe(scope, at)
        assert operation is not None
        winner = await planner.candidate(operation, at + timedelta(seconds=1))
        facts, _, first = read_cancellation_drain_plan(operation, winner)
        assert facts.intent.expected_version == requested.version
        assert facts.source_version == source_version
        reservation = await host.store.reserve(operation.intent.request, winner)
        assert reservation.acquired
        owned = reservation.execution
        host.executions.append(owned)
        if fault == "archive_rejected":
            trigger = "drain_fault_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                        "SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated drain archive rejection'"
                    )
                )
        elif fault == "coordinator_advanced":
            # Disposable interference only: advance the competing coordinator
            # fence without fabricating a provider result or witness.
            async with tx.open() as db:
                await save_checkpoint(
                    db, CheckpointState(run_id, source_version + 1, None), source_version
                )
                await db.commit()
        root = EvaluationStateRoot(
            host.tx,
            host.archives,
            host.store,
            operation.intent,
            owned.plan,
            apply_cancellation_drain,
            isolation_level="REPEATABLE READ",
        )
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                original_commit = EventSession.commit

                async def discard(db):
                    await original_commit(db)
                    raise ConnectionError("client discarded completed original drain reply")

                patch.setattr(EventSession, "commit", discard)
            outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await rows(tx, run_id)
        if committed:
            view = await management.get(scope)
            assert view.status == "canceled" and view.version == source_version + 1
            audit = json.loads(view.cancellation_json)
            assert audit["actor"] == scope.actor and audit["reason"] == "等待原调用完成后取消"
            assert (
                current[0]["progress_json"]["cancel_requested"]
                == (before[0]["progress_json"]["cancel_requested"])
            )
        elif fault == "archive_rejected":
            assert current == before
        else:
            assert current[0] == before[0] and current[2]["version"] == source_version + 1
        assert await stored(tx, run_id) == original_completion
        assert await host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
        async with host.tx.open() as db:
            sequence = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            assert (None if sequence is None else (sequence["sequence"], sequence["version"])) == (
                (1, source_version + 1) if committed else None
            )
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(
                        outbox.c.message_id == first.envelope().message_id
                    )
                )
            ).all()
            assert await db.scalar(text("SELECT @@transaction_isolation")) == "READ-COMMITTED"
    finally:
        host.tx.database.engine = engine
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
            )
            await db.commit()
