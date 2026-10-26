"""Actual serial completion entrypoints and MySQL roots; transport here is not a Broker proof."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import delete, text

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
    SerialResponseReference,
    save_serial_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import prepare_step
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_response_receipts
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.serial_semantic_sender import SerialSemanticSender
from qs_ai.infrastructure.workflow_transport.serial_semantic_state import (
    SerialSemanticPlanner,
    SerialSemanticWrite,
    read_serial_semantic_plan,
)
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_worker_dispatch import original as original
from tests.messaging_fixtures import keys as keys

pytestmark = pytest.mark.integration


@pytest.fixture
async def serial(original):
    p = original
    original_recorder = p.host.tx.database.state_events
    generation_gateway = Gateway(p.ready)
    generated = await step(p.ready, generation_gateway)
    prepared = await prepare_step(
        p.tx,
        p.run_id,
        generated.version,
        1,
        "worker:original",
        p.routes,
        p.schemas,
        clock=lambda: AT + timedelta(seconds=2),
    )
    gateway = Gateway(p.ready)
    response = await gateway.generate_messages(
        prepared.messages, prepared.route, prepared.schema, prepared.invocation_id
    )
    reference = SerialResponseReference.from_prepared(1, prepared)
    planner = SerialSemanticPlanner(
        p.host.tx,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
    )

    async def save(result):
        async with p.host.tx.open() as db:
            await db.begin()
            await save_serial_response(db, reference, result)
            await db.commit()

    try:
        yield SimpleNamespace(**locals())
    finally:
        p.host.tx.database.state_events = original_recorder
        async with p.host.tx.open() as db:
            await db.execute(
                delete(evaluation_response_receipts).where(
                    evaluation_response_receipts.c.run_id == str(p.run_id)
                )
            )
            await db.commit()


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.parametrize("fault", ["none", "witness_rejected", "client_reply_discarded"])
async def test_first_serial_response_terminal_receipt_cancel_drain_and_witness_share_original_root(
    serial, trigger_admin, monkeypatch, unknown, cancel, fault
):
    s, p = serial, serial.p
    failure = (
        ClassifiedFailure(
            "semantic_evaluation",
            "result_unknown",
            "provider_result_unknown",
            False,
            True,
            "manual_acknowledgement",
            "Original unknown response",
            (s.prepared.execution_id,),
        )
        if unknown
        else None
    )
    result = EvaluationResponse(None if unknown else s.response, failure, AT + timedelta(seconds=3))
    await s.save(result)
    if cancel:
        await MySQLEvaluationManagement(p.tx).cancel(
            ManagementScope(p.run_id, 1, 42),
            8,
            "Drain original serial semantic",
            AT + timedelta(seconds=4),
            discard=False,
            confirm=True,
        )
    before = await snapshot(p.tx, p.run_id)
    intent = await s.planner.intent(s.reference)
    winner = await s.planner.candidate(s.reference)
    facts, operation = read_serial_semantic_plan(intent, winner)
    assert await snapshot(p.tx, p.run_id) == before and s.gateway.calls == 1
    assert facts.source_version == (9 if cancel else 8)
    assert operation.candidate_id == s.reference.checkpoint.candidate_id
    reserved = await p.host.store.reserve(intent.request, winner)
    assert reserved.acquired
    p.host.executions.append(reserved.execution)
    business = SerialSemanticWrite()
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        winner,
        business,
        isolation_level="REPEATABLE READ",
    )
    trigger = None
    try:
        p.host.tx.database.state_events = SimpleNamespace(
            flush=AsyncMock(
                side_effect=AssertionError("No legacy publication in original completion root")
            )
        )
        if fault == "witness_rejected":
            trigger = "serial_completion_witness_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                        "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated terminal witness refusal'; END IF; END"
                    )
                )
        with monkeypatch.context() as patch:
            from qs_ai.infrastructure.persistence.mysql import evaluation_step
            from qs_ai.infrastructure.workflow_transport import serial_semantic_state

            def no_identity(*args, **kwargs):
                raise AssertionError("No UUID, Seal or provider call in original terminal callback")

            patch.setattr(evaluation_step, "uuid4", no_identity)
            patch.setattr(serial_semantic_state, "uuid4", no_identity)
            patch.setattr(serial_semantic_state, "prepare", no_identity)
            if fault == "client_reply_discarded":
                real_commit = EventSession.commit

                async def discarded(db):
                    await real_commit(db)
                    raise ConnectionError("client discarded successful terminal commit reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(reserved.execution)
        committed = fault != "witness_rejected"
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(reserved.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        if committed:
            assert business.state is not None and business.state.version == facts.source_version + 1
            _, first = read_evaluation_state_plan(intent, winner)
            assert await p.host.archives.read_committed(first.envelope().message_id) == first
            runtime = SimpleNamespace(
                store=p.host.store,
                reserve=AsyncMock(),
                execute=AsyncMock(),
                retry=AsyncMock(),
                replan=AsyncMock(),
            )
            sender = SerialSemanticSender(p.host.tx, p.host.archives, runtime, s.planner)
            duplicate = await sender(s.reference)
            assert duplicate == business.state
            runtime.reserve.assert_not_called()
            runtime.execute.assert_not_called()
        else:
            assert await snapshot(p.tx, p.run_id) == before
        assert s.gateway.calls == 1
    finally:
        if trigger:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))


async def test_cancel_after_first_plan_requires_explicit_new_plan_and_retains_first_candidate(
    serial,
):
    from reliable_messaging.durable import MessageConflict
    from reliable_messaging.transaction import ReplanAuthorization

    s, p = serial, serial.p
    await s.save(EvaluationResponse(s.response, None, AT + timedelta(seconds=3)))
    intent = await s.planner.intent(s.reference)
    first = await s.planner.candidate(s.reference)
    _, original_op = read_serial_semantic_plan(intent, first)
    reserved = await p.host.store.reserve(intent.request, first)
    p.host.executions.append(reserved.execution)
    await MySQLEvaluationManagement(p.tx).cancel(
        ManagementScope(p.run_id, 1, 42),
        8,
        "Cancel after first semantic terminal plan",
        AT + timedelta(seconds=4),
        discard=False,
        confirm=True,
    )
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        first,
        SerialSemanticWrite(),
        isolation_level="REPEATABLE READ",
    )
    assert await root(reserved.execution) == LocalOutcome.ABORTED
    with pytest.raises(MessageConflict):
        await s.planner.verify(s.reference, first)
    next_plan = await s.planner.candidate(s.reference, candidate_id=original_op.candidate_id)
    facts, next_op = read_serial_semantic_plan(intent, next_plan)
    assert next_op.intent == original_op.intent and next_op.candidate_id == original_op.candidate_id
    assert facts.source_version == 9 and next_plan.digest != first.digest
    retried = await p.host.store.replan(ReplanAuthorization(reserved.execution, next_plan))
    assert retried.acquired
    p.host.executions.append(retried.execution)
    business = SerialSemanticWrite()
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        next_plan,
        business,
        isolation_level="REPEATABLE READ",
    )
    assert await root(retried.execution) == LocalOutcome.COMMITTED
    assert business.state.version == 10 and s.gateway.calls == 1
    assert (
        await StoreChecker(p.host.store).check(reserved.execution.references[0])
        == Decision.ROLLBACK
    )
    assert (
        await StoreChecker(p.host.store).check(retried.execution.references[0]) == Decision.COMMIT
    )
