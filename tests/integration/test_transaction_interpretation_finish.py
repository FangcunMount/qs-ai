"""Actual finalization entrypoint and precise MySQL proof; simulated transport only."""

import json
from uuid import uuid4

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import select, text, update

from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.schema import artifacts, jobs, model_calls, sessions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
    InterpretationFinishSender,
)
from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import (
    InterpretationFinishInput,
    InterpretationFinishPlanner,
    InterpretationFinishWrite,
    read_finish_plan,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_root import (
    InterpretationStateRoot,
)
from tests.integration.test_artifact_acceptance import ready as ready_artifact
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_interpretation_finish_root import snapshot
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_interpretation_state import original as original
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys

pytestmark = pytest.mark.integration


def incoming(p, question):
    return InterpretationFinishInput(
        p.claim,
        WorkflowResult(
            "checkpoint:original-finish",
            question="first original question" if question else None,
            can_skip=False,
            failure_code=None if question else "model_not_connected",
        ),
    )


def planner(p):
    return InterpretationFinishPlanner(
        p.host.kit.store,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
    )


@pytest.mark.parametrize("question", [False, True])
@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
async def test_frozen_original_finish_question_failure_and_witness_commit_together(
    original, trigger_admin, monkeypatch, question, fault
):
    p = original
    value = incoming(p, question)
    intent = value.intent(p.request_id)
    winner = await planner(p).candidate(value, intent)
    assert await snapshot(p.host.kit, p.claim) == p.before
    reserved = await p.host.store.reserve(intent.request, winner)
    assert reserved.acquired
    p.host.executions.append(reserved.execution)
    facts, first, question_id = read_finish_plan(value, intent, winner)
    root = InterpretationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        winner,
        InterpretationFinishWrite(p.host.kit.store, value, intent, winner),
    )
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "finish_sdk_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated finish archive refusal'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated finish witness refusal'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                commit = EventSession.commit

                async def discarded(db):
                    await commit(db)
                    raise ConnectionError("client discarded completed interpretation finish reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(reserved.execution)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(reserved.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await snapshot(p.host.kit, p.claim)
        if committed:
            assert current[0][0]["version"] == p.claim.session.version + 1
            assert current[0][0]["status"] == ("awaiting_answer" if question else "blocked")
            assert current[1][0]["checkpoint_ref"] == value.result.checkpoint_ref
            assert current[2][0]["status"] == "done"
            if question:
                assert current[4][0]["id"] == question_id
                assert (
                    current[4][0]["question_seq"]
                    == json.loads(facts.business_context)["question_sequence"]
                )
            history = next(
                row for row in current[5] if row["event_id"] == first.envelope().message_id
            )
            assert history["mq_owned"] and not history["delivered"] and history["attempts"] == 0
        else:
            assert current == p.before
        assert await p.host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))


@pytest.mark.parametrize("question", [False, True])
@pytest.mark.parametrize("reject", [False, True])
async def test_explicit_finish_sender_prepares_first_and_replays_first_archive_after_later_state(
    original, question, reject, monkeypatch
):
    p = original
    value = incoming(p, question)

    class Runtime:
        store = p.host.store
        calls = 0

        async def reserve(self, request, candidate):
            owned = await self.store.reserve(request, candidate)
            if owned.acquired:
                p.host.executions.append(owned.execution)
            return owned

        async def execute(self, owned, local):
            self.calls += 1
            if reject:
                raise TransactionSendError(
                    "isolated_half_refused", Result(LocalOutcome.NOT_STARTED, ())
                )
            raise TransactionSendError("isolated_final_unknown", Result(await local(owned), ()))

    runtime = Runtime()
    sender = InterpretationFinishSender(
        p.host.kit.store,
        p.host.archives,
        runtime,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
    )
    if reject:
        with pytest.raises(TransactionSendError):
            await sender(value.claim, value.result)
        assert await snapshot(p.host.kit, p.claim) == p.before
        return
    assert await sender(value.claim, value.result) is None
    first = await p.host.store.lookup(value.intent(p.request_id).request)
    _, frozen, _ = read_finish_plan(value, value.intent(p.request_id), first.execution.plan)
    assert await p.host.archives.read_committed(frozen.envelope().message_id) == frozen
    async with p.host.tx.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == p.claim.session.id)
            .values(version=p.claim.session.version + 2, goal="a later business transition")
        )
        await db.commit()

    async def no_plan(*args):
        raise AssertionError("First committed finish must not be planned or sealed again")

    monkeypatch.setattr(sender._planner, "candidate", no_plan)
    assert await sender(value.claim, value.result) is None
    assert runtime.calls == 1
    assert await p.host.archives.read_committed(frozen.envelope().message_id) == frozen


@pytest.mark.parametrize("change", ["session", "job", "heartbeat"])
async def test_finish_root_rechecks_source_and_original_live_lease(original, change):
    p = original
    value = incoming(p, False)
    intent = value.intent(p.request_id)
    winner = await planner(p).candidate(value, intent)
    owned = await p.host.store.reserve(intent.request, winner)
    p.host.executions.append(owned.execution)
    if change == "heartbeat":
        await p.host.kit.store.renew(p.claim, 90)
    else:
        async with p.host.tx.open() as db:
            statement = (
                update(sessions).where(sessions.c.id == p.claim.session.id).values(goal="changed")
            )
            if change == "job":
                statement = (
                    update(jobs).where(jobs.c.id == p.claim.job_id).values(answer="foreign input")
                )
            await db.execute(statement)
            await db.commit()
    before = await snapshot(p.host.kit, p.claim)
    root = InterpretationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        winner,
        InterpretationFinishWrite(p.host.kit.store, value, intent, winner),
    )
    committed = change == "heartbeat"
    assert await root(owned.execution) == (
        LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED
    )
    assert await StoreChecker(p.host.store).check(owned.execution.references[0]) == (
        Decision.COMMIT if committed else Decision.ROLLBACK
    )
    if not committed:
        assert await snapshot(p.host.kit, p.claim) == before


@pytest.mark.parametrize("fault", ["none", "client_reply_discarded"])
@pytest.mark.usefixtures("published_configuration")
async def test_published_artifact_finish_keeps_durable_model_response_and_original_identity(
    host, monkeypatch, fault
):
    from qs_ai.application.execution import artifact as artifact_module

    claim, candidate = await ready_artifact(host.kit)
    request_id = str(uuid4())
    async with host.tx.open() as db:
        await db.execute(
            update(sessions).where(sessions.c.id == claim.session.id).values(request_id=request_id)
        )
        await db.commit()
        original_call = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
            .mappings()
            .one()
        )
    value = InterpretationFinishInput(claim, WorkflowResult("", artifact=candidate))
    intent = value.intent(request_id)
    plan = await InterpretationFinishPlanner(
        host.kit.store, signing_key=host.keys["ai.sign"], recipient_key=host.keys["qs.encrypt"]
    ).candidate(value, intent)
    owned = await host.store.reserve(intent.request, plan)
    host.executions.append(owned.execution)
    facts, first, _ = read_finish_plan(value, intent, plan)
    business = InterpretationFinishWrite(host.kit.store, value, intent, plan)
    root = InterpretationStateRoot(host.tx, host.archives, host.store, intent, plan, business)

    def no_uuid(*args):
        raise AssertionError("No Artifact identity generation inside the original executor")

    with monkeypatch.context() as patch:
        patch.setattr(artifact_module, "uuid5", no_uuid)
        if fault == "client_reply_discarded":
            commit = EventSession.commit

            async def discarded(db):
                await commit(db)
                raise ConnectionError("client discarded completed Artifact finish reply")

            patch.setattr(EventSession, "commit", discarded)
        assert await root(owned.execution) == LocalOutcome.COMMITTED
    async with host.tx.open() as db:
        current_call = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
            .mappings()
            .one()
        )
        saved = await db.scalar(
            select(artifacts.c.payload).where(artifacts.c.run_id == claim.run_id)
        )
        assert saved["id"] == candidate.id
        assert saved["content_fingerprint"] == candidate.content_fingerprint
        assert current_call == original_call and current_call["status"] == "response_received"
    assert await StoreChecker(host.store).check(owned.execution.references[0]) == Decision.COMMIT
    assert await host.archives.read_committed(first.envelope().message_id) == first
