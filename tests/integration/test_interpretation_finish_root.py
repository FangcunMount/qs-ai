"""Original interpretation root ownership and staged result history; no SDK/Broker claim."""

from uuid import uuid4

import pytest
from reliable_messaging.transaction import BindingError
from sqlalchemy import select, update

from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.infrastructure.persistence.mysql.schema import (
    execution_leases,
    jobs,
    questions,
    result_outbox,
    runs,
    sessions,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


async def snapshot(kit, claim):
    async with kit.transactions.open() as db:
        return [
            list((await db.execute(statement)).mappings())
            for statement in (
                select(sessions).where(sessions.c.id == claim.session.id),
                select(runs).where(runs.c.id == claim.run_id),
                select(jobs).where(jobs.c.id == claim.job_id),
                select(execution_leases).where(execution_leases.c.thread_id == claim.session.id),
                select(questions).where(questions.c.session_id == claim.session.id),
                select(result_outbox).where(result_outbox.c.session_id == claim.session.id),
            )
        ]


@pytest.mark.parametrize("question", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_borrowed_finish_commits_or_rolls_back_state_question_job_and_first_result(
    kit, question, commit
):
    await kit.queued()
    claim = await kit.store.claim(30)
    assert claim is not None
    # Bind only this fixture's previously unbound session; do not invent a command Inbox.
    async with kit.transactions.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == claim.session.id)
            .values(request_id=str(uuid4()))
        )
        await db.commit()
    before = await snapshot(kit, claim)
    question_id, event_id = str(uuid4()), str(uuid4())
    result = WorkflowResult(
        "checkpoint:first-finish",
        question="first frozen question" if question else None,
        failure_code=None if question else "model_not_connected",
    )
    async with kit.transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        root = db.get_transaction()
        assert (
            await kit.store.finish_in(
                db,
                claim,
                result,
                question_id=question_id if question else None,
                state_event_id=event_id,
            )
            is None
        )
        assert db.get_transaction() is root and root.is_active
        if commit:
            await db.commit()
    current = await snapshot(kit, claim)
    if not commit:
        assert current == before
    else:
        assert current[0][0]["version"] == claim.session.version + 1
        assert current[0][0]["status"] == ("awaiting_answer" if question else "blocked")
        assert current[1][0]["checkpoint_ref"] == result.checkpoint_ref
        assert current[2][0]["status"] == "done"
        assert len(current[4]) == (1 if question else 0)
        if question:
            assert current[4][0]["id"] == question_id
        original = next(row for row in current[5] if row["event_id"] == event_id)
        assert original["version"] == claim.session.version + 1
        assert original["payload"]["event_id"] == event_id
        assert original["payload"]["question_id"] == (question_id if question else "")
        assert original["mq_owned"] is False  # History staging is not SDK publication.


@pytest.mark.parametrize("nested", [False, True])
async def test_borrowed_finish_refuses_inactive_or_nested_real_session_without_business_write(
    kit, nested
):
    await kit.queued()
    claim = await kit.store.claim(30)
    assert claim is not None
    before = await snapshot(kit, claim)
    async with kit.transactions.open() as db:
        if nested:
            async with db.begin(), db.begin_nested():
                with pytest.raises(BindingError):
                    await kit.store.finish_in(db, claim, WorkflowResult("checkpoint:first-finish"))
        else:
            assert db.get_transaction() is None
            with pytest.raises(BindingError):
                await kit.store.finish_in(db, claim, WorkflowResult("checkpoint:first-finish"))
    assert await snapshot(kit, claim) == before
