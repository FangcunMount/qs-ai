"""Original claim + precise SDK witness; direct root tests, no half/Broker claim."""

from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import select, text, update

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.schema import jobs, sessions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import (
    InterpretationClaimPlanner,
    InterpretationClaimWrite,
    read_claim_plan,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_root import (
    InterpretationStateRoot,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_interpretation_claim_root import rows
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys

pytestmark = pytest.mark.integration


async def candidate(host, exhausted=False, *, snapshot=False):
    receipt = await host.kit.queued()
    request_id = str(uuid4())
    async with host.tx.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == receipt.session_id)
            .values(
                request_id=request_id,
                workflow_version="qs-snapshot-v1" if snapshot else "interpretation-v1",
            )
        )
        if exhausted:
            await db.execute(
                update(jobs)
                .where(jobs.c.session_id == receipt.session_id)
                .values(
                    attempt=3,
                )
            )
        await db.commit()
        job_id = await db.scalar(select(jobs.c.id).where(jobs.c.session_id == receipt.session_id))
    planner = InterpretationClaimPlanner(
        host.kit.store,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    source = await planner.source(job_id, receipt.session_id)
    before = await rows(host.kit, receipt.session_id)
    plan = await planner.candidate(source, 60)
    assert await rows(host.kit, receipt.session_id) == before
    return source, before, plan


@pytest.mark.parametrize("exhausted", [False, True])
@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
async def test_frozen_claim_business_lease_attempt_first_history_archive_and_witness_are_atomic(
    host,
    trigger_admin,
    monkeypatch,
    exhausted,
    fault,
):
    source, before, plan = await candidate(host, exhausted)
    intent = source.intent(60)
    facts, first = read_claim_plan(source, 60, plan)
    owned = await host.store.reserve(intent.request, plan)
    assert owned.acquired
    host.executions.append(owned.execution)
    write = InterpretationClaimWrite(host.kit.store, source, 60, plan)
    root = InterpretationStateRoot(host.tx, host.archives, host.store, intent, plan, write)
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "claim_state_fault_" + uuid4().hex
            sql = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated claim archive refusal'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated claim witness refusal'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(sql))
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                commit = EventSession.commit

                async def discarded(db):
                    await commit(db)
                    raise ConnectionError("client discarded completed original claim reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(owned.execution)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(host.store).check(owned.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        after = await rows(host.kit, source.session.id)
        if committed:
            assert after[0][0]["version"] == facts.target_version
            assert after[0][0]["status"] == ("blocked" if exhausted else "running")
            assert after[1][0]["status"] == ("dead" if exhausted else "leased")
            assert after[1][0]["attempt"] == (3 if exhausted else 1)
            assert len(after[4]) == 1 and after[4][0]["mq_owned"]
            assert after[4][0]["event_id"] == first.envelope().message_id
            assert after[4][0]["version"] == facts.target_version
            assert write.staged.status == ("exhausted" if exhausted else "claimed")
        else:
            assert after == before
        assert await host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))


@pytest.mark.parametrize("damage", ["answer", "request", "not_due"])
async def test_claim_source_conflict_preserves_competitor_and_aborts_exact_execution(
    host,
    damage,
):
    source, _, plan = await candidate(host)
    owned = await host.store.reserve(source.intent(60).request, plan)
    host.executions.append(owned.execution)
    async with host.tx.open() as db:
        if damage == "answer":
            statement = (
                update(jobs).where(jobs.c.id == source.job_id).values(answer="competing answer")
            )
        elif damage == "request":
            statement = (
                update(sessions)
                .where(sessions.c.id == source.session.id)
                .values(request_id=str(uuid4()))
            )
        else:
            statement = (
                update(jobs)
                .where(jobs.c.id == source.job_id)
                .values(
                    available_at=text("TIMESTAMPADD(SECOND, 120, UTC_TIMESTAMP(6))"),
                )
            )
        await db.execute(statement)
        await db.commit()
    competing = await rows(host.kit, source.session.id)
    write = InterpretationClaimWrite(host.kit.store, source, 60, plan)
    root = InterpretationStateRoot(
        host.tx, host.archives, host.store, source.intent(60), plan, write
    )
    assert await root(owned.execution) == LocalOutcome.ABORTED
    assert write.staged is None
    assert await rows(host.kit, source.session.id) == competing
    assert await StoreChecker(host.store).check(owned.execution.references[0]) == Decision.ROLLBACK


async def test_claim_readonly_source_cannot_adopt_a_job_from_another_session(host):
    source, _, _ = await candidate(host)
    other = await host.kit.queued()
    planner = InterpretationClaimPlanner(
        host.kit.store,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    with pytest.raises(LeaseLost):
        await planner.source(source.job_id, other.session_id)


async def test_real_capacity_deferral_cannot_commit_prepared_running_event(host):
    first, _, _ = await candidate(host, snapshot=True)
    claim = await host.kit.store.claim(60)
    assert claim is not None and claim.session.id == first.session.id
    source, before, plan = await candidate(host, snapshot=True)
    assert source.session.uses_qs_snapshot
    owned = await host.store.reserve(source.intent(60).request, plan)
    host.executions.append(owned.execution)
    write = InterpretationClaimWrite(host.kit.store, source, 60, plan)
    root = InterpretationStateRoot(
        host.tx, host.archives, host.store, source.intent(60), plan, write
    )
    assert await root(owned.execution) == LocalOutcome.ABORTED
    assert write.staged is None
    assert await rows(host.kit, source.session.id) == before
    assert await StoreChecker(host.store).check(owned.execution.references[0]) == Decision.ROLLBACK
    _, first_event = read_claim_plan(source, 60, plan)
    assert await host.archives.read_committed(first_event.envelope().message_id) is None


@pytest.mark.parametrize("exhausted", [False, True])
@pytest.mark.parametrize("reject", [False, True])
async def test_claim_sender_halves_precede_original_writes_and_duplicate_never_hands_off_again(
    host,
    exhausted,
    reject,
):
    from reliable_messaging.rocketmq_transaction import TransactionSendError
    from reliable_messaging.transaction import Result

    from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
        InterpretationClaimSender,
    )

    source, before, plan = await candidate(host, exhausted)

    class Runtime:
        store = host.store
        calls = 0

        async def reserve(self, request, candidate):
            owned = await self.store.reserve(request, candidate)
            if owned.acquired:
                host.executions.append(owned.execution)
            return owned

        async def execute(self, owned, local):
            self.calls += 1
            assert await rows(host.kit, source.session.id) == before
            if reject:
                raise TransactionSendError(
                    "isolated_half_refused", Result(LocalOutcome.NOT_STARTED, ())
                )
            outcome = await local(owned)
            raise TransactionSendError("isolated_final_reply_discarded", Result(outcome, ()))

    runtime = Runtime()
    sender = InterpretationClaimSender(
        host.kit.store,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    if reject:
        with pytest.raises(TransactionSendError):
            await sender(source, 60)
        assert await rows(host.kit, source.session.id) == before
    else:
        claim = await sender(source, 60)
        assert (claim is None) is exhausted
        if not exhausted:
            assert claim.session.id == source.session.id and claim.fence == source.fence + 1
        committed = await rows(host.kit, source.session.id)
        assert await sender(source, 60) is None  # No second admission for a committed request.
        assert await rows(host.kit, source.session.id) == committed
    assert runtime.calls == 1


async def test_committed_claim_with_expired_lease_does_not_handoff_to_model(host):
    from reliable_messaging.transaction import Result

    from qs_ai.infrastructure.persistence.mysql.schema import execution_leases
    from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
        InterpretationClaimSender,
    )

    source, _, _ = await candidate(host)

    class Runtime:
        store = host.store

        async def reserve(self, request, candidate):
            owned = await self.store.reserve(request, candidate)
            host.executions.append(owned.execution)
            return owned

        async def execute(self, owned, local):
            outcome = await local(owned)
            assert outcome == LocalOutcome.COMMITTED
            async with host.tx.open() as db:
                await db.execute(
                    update(execution_leases)
                    .where(
                        execution_leases.c.thread_id == source.session.id,
                    )
                    .values(expires_at=text("TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))"))
                )
                await db.commit()
            return Result(outcome, ())

    sender = InterpretationClaimSender(
        host.kit.store,
        host.archives,
        Runtime(),
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    assert await sender(source, 60) is None
    observed = await host.store.lookup(source.intent(60).request)
    assert await StoreChecker(host.store).check(observed.execution.references[0]) == Decision.COMMIT
