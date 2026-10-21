"""Real original history/root CAS only; no full plan, witness, Sender or Broker claim."""

import hashlib
from uuid import uuid4

import pytest
from reliable_messaging.durable import MessageConflict
from sqlalchemy import select, update

from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox, sessions
from qs_ai.infrastructure.workflow_transport.frozen_start_events import (
    InterpretationStateTranscription,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def history(kit):
    await kit.queued()
    claim = await kit.store.claim(30)
    assert claim is not None
    async with kit.transactions.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == claim.session.id)
            .values(request_id=str(uuid4()))
        )
        await db.commit()
    await kit.store.finish(
        claim, WorkflowResult("checkpoint:history", question="original first question")
    )
    async with kit.transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.session_id == claim.session.id)
                )
            )
            .mappings()
            .one()
        )
    yield kit, dict(row)


@pytest.mark.parametrize("commit", [False, True])
async def test_standalone_history_claim_borrows_original_root_and_rolls_back_or_commits(
    history, commit
):
    kit, row = history
    expected = pb.StateEvent(**row["payload"])
    digest = hashlib.sha256(b"synthetic host-plan identity" + row["event_id"].encode()).digest()
    recorder = InterpretationStateTranscription(digest, expected)
    async with kit.transactions.open() as db:
        await db.begin()
        root = db.get_transaction()
        await recorder.record_interpretation(db, row)
        await recorder.flush(db)
        assert db.get_transaction() is root and root.is_active
        assert (
            await db.scalar(
                select(result_outbox.c.mq_owned).where(result_outbox.c.event_id == row["event_id"])
            )
            is True
        )
        if commit:
            await db.commit()
    async with kit.transactions.open() as db:
        current = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == row["event_id"])
                )
            )
            .mappings()
            .one()
        )
    assert current["mq_owned"] is commit
    assert (
        current["payload"] == row["payload"]
        and current["attempts"] == 0
        and current["delivered"] is False
    )


@pytest.mark.parametrize("field", ["attempts", "delivered"])
async def test_old_delivery_responsibility_cannot_be_adopted_as_a_new_state_transaction(
    history, field
):
    kit, row = history
    async with kit.transactions.open() as db:
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.event_id == row["event_id"])
            .values(**{field: 1 if field == "attempts" else True})
        )
        await db.commit()
    async with kit.transactions.open() as db:
        current = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == row["event_id"])
                )
            )
            .mappings()
            .one()
        )
    recorder = InterpretationStateTranscription(
        hashlib.sha256(b"synthetic host-plan identity").digest(), pb.StateEvent(**row["payload"])
    )
    async with kit.transactions.open() as db, db.begin():
        with pytest.raises(MessageConflict):
            await recorder.record_interpretation(db, current)
    async with kit.transactions.open() as db:
        owned = await db.scalar(
            select(result_outbox.c.mq_owned).where(result_outbox.c.event_id == row["event_id"])
        )
    assert owned is False
