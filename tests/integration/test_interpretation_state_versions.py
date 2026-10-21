"""Two-transition state protocol root only; not the complete claim writer or Broker."""

import hashlib
from dataclasses import asdict, replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import select, text, update

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox, sessions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.interpretation_state_plan import (
    InterpretationStateFacts,
    InterpretationStateIntent,
    freeze_interpretation_state_plan,
    read_interpretation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_root import (
    InterpretationStateRoot,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_projection import interpretation_state
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
async def test_two_domain_transitions_transcribe_only_one_frozen_final_history_on_original_root(
    host, trigger_admin, monkeypatch, fault
):
    receipt = await host.kit.queued()
    request_id, event_id = str(uuid4()), str(uuid4())
    async with host.tx.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == receipt.session_id)
            .values(request_id=request_id)
        )
        await db.commit()
        original = await MySQLUnitOfWork(db).get(receipt.session_id)
    intent = InterpretationStateIntent(
        original.actor.org_id,
        original.id,
        request_id,
        "two-transition-protocol-fixture",
        original.version,
        hashlib.sha256(b"explicit two-transition host fixture").digest(),
    )
    facts = InterpretationStateFacts(
        intent, datetime.now(UTC), b"protocol-fixture", version_delta=2
    )
    projected = replace(original)
    projected.running()
    projected.block("attempts_exhausted")
    value = interpretation_state(event_id, request_id, projected)
    message = prepare(
        pb.INTERPRETATION_STATE,
        event_id,
        request_id,
        pb.MessagingBody(interpretation_state=workflow.StateEvent(**asdict(value))),
        organization_id=intent.organization_id,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
        original_occurred_at=facts.at.isoformat(timespec="microseconds"),
    )
    plan = freeze_interpretation_state_plan(facts, str(uuid4()), message)
    owned = await host.store.reserve(intent.request, plan)
    assert owned.acquired
    host.executions.append(owned.execution)
    calls = 0

    async def write(db, frozen):
        nonlocal calls
        calls += 1
        assert frozen == facts and frozen.version_delta == 2
        uow = MySQLUnitOfWork(db, state_event_id=event_id)
        current = await uow.get(original.id, lock=True)
        assert current == original
        current.running()
        current.block("attempts_exhausted")
        await uow.save(current)

    root = InterpretationStateRoot(host.tx, host.archives, host.store, intent, plan, write)
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "two_version_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated two-version archive refusal'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated two-version witness refusal'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                commit = EventSession.commit

                async def discarded(db):
                    await commit(db)
                    raise ConnectionError("client discarded completed two-version root reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(owned.execution)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert calls == 1
        assert await StoreChecker(host.store).check(owned.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        async with host.tx.open() as db:
            current = await MySQLUnitOfWork(db).get(original.id)
            history = (
                (
                    await db.execute(
                        select(result_outbox).where(result_outbox.c.session_id == original.id)
                    )
                )
                .mappings()
                .all()
            )
        if committed:
            assert current == projected and len(history) == 1
            assert history[0]["version"] == intent.expected_version + 2
            assert history[0]["mq_owned"] and history[0]["event_id"] == event_id
        else:
            assert current == original and not history
        _, first = read_interpretation_state_plan(intent, plan)
        assert await host.archives.read_committed(event_id) == (first if committed else None)
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))
