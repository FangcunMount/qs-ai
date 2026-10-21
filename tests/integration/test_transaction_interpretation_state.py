"""Standalone plan -> original finish/history/archive/witness root; no half/Broker claim."""

import hashlib
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.transaction import BindingError, Decision, LocalOutcome, StoreChecker
from sqlalchemy import text, update

from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.schema import sessions
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
from tests.integration.test_interpretation_finish_root import snapshot
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def original(host):
    await host.kit.queued()
    claim = await host.kit.store.claim(60)
    assert claim is not None
    # A synthetic fixture request binding is not an original command Inbox or QS acceptance.
    request_id = str(uuid4())
    async with host.tx.open() as db:
        await db.execute(
            update(sessions).where(sessions.c.id == claim.session.id).values(request_id=request_id)
        )
        await db.commit()
    before = await snapshot(host.kit, claim)
    yield SimpleNamespace(**locals())


def candidate(p, question):
    event_id, question_id = str(uuid4()), str(uuid4()) if question else None
    context = dict(
        job_id=p.claim.job_id,
        run_id=p.claim.run_id,
        fence=p.claim.fence,
        event_id=event_id,
        question_id=question_id,
        question="first frozen question" if question else None,
        failure_code=None if question else "model_not_connected",
        checkpoint_ref="checkpoint:state-root",
    )
    stable = {
        key: value for key, value in context.items() if key not in ("event_id", "question_id")
    }
    intent = InterpretationStateIntent(
        p.claim.session.actor.org_id,
        p.claim.session.id,
        p.request_id,
        "finish/" + p.claim.run_id,
        p.claim.session.version,
        hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).digest(),
    )
    facts = InterpretationStateFacts(
        intent,
        datetime.now(UTC),
        json.dumps(context, sort_keys=True, separators=(",", ":")).encode(),
    )
    session = replace(p.claim.session)
    if question:
        session.await_answer(question_id)
    else:
        session.block("model_not_connected")
    value = interpretation_state(
        event_id,
        p.request_id,
        session,
        dict(id=question_id, text=context["question"], can_skip=True) if question else None,
    )
    first = prepare(
        pb.INTERPRETATION_STATE,
        event_id,
        p.request_id,
        pb.MessagingBody(interpretation_state=workflow.StateEvent(**asdict(value))),
        organization_id=intent.organization_id,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
        original_occurred_at=facts.at.isoformat(timespec="microseconds"),
    )
    return intent, freeze_interpretation_state_plan(facts, str(uuid4()), first)


@pytest.mark.parametrize("question", [False, True])
@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
async def test_standalone_interpretation_state_history_and_witness_use_original_finish_root(
    original, trigger_admin, monkeypatch, question, fault
):
    p = original
    intent, winner = candidate(p, question)
    reserved = await p.host.store.reserve(intent.request, winner)
    assert reserved.acquired
    p.host.executions.append(reserved.execution)
    facts, first = read_interpretation_state_plan(intent, winner)
    original_context = json.loads(facts.business_context)
    calls = 0

    async def business(db, frozen):
        nonlocal calls
        calls += 1
        source = json.loads(frozen.business_context)
        assert (source["job_id"], source["run_id"], source["fence"]) == (
            p.claim.job_id,
            p.claim.run_id,
            p.claim.fence,
        )
        await p.host.kit.store.finish_in(
            db,
            p.claim,
            WorkflowResult(
                source["checkpoint_ref"],
                question=source["question"],
                failure_code=source["failure_code"],
            ),
            question_id=source["question_id"],
            state_event_id=source["event_id"],
            at=frozen.at,
        )

    root = InterpretationStateRoot(
        p.host.tx, p.host.archives, p.host.store, intent, winner, business
    )
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "interpretation_state_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated interpretation archive refusal'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated interpretation witness refusal'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                actual_commit = EventSession.commit

                async def discarded(db):
                    await actual_commit(db)
                    raise ConnectionError(
                        "client discarded completed interpretation state commit reply"
                    )

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(reserved.execution)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert calls == 1
        assert await StoreChecker(p.host.store).check(reserved.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        after = await snapshot(p.host.kit, p.claim)
        if not committed:
            assert after == p.before
        else:
            assert after[0][0]["version"] == p.claim.session.version + 1
            assert after[2][0]["status"] == "done"
            row = next(r for r in after[5] if r["event_id"] == original_context["event_id"])
            assert row["mq_owned"] is True and row["delivered"] is False and row["attempts"] == 0
            assert row["payload"]["question_id"] == (original_context["question_id"] or "")
        assert await p.host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
        with pytest.raises(BindingError):
            await root(reserved.execution)
        assert calls == 1
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))
