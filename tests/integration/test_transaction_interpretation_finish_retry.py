"""Real explicit finalization retry and original roots; simulated MQ, no provider."""

from unittest.mock import AsyncMock

import pytest
from reliable_messaging.transaction import (
    Decision,
    Result,
    RetryAuthorization,
    StaleExecutionError,
    StoreChecker,
)
from sqlalchemy import update

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.schema import jobs
from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
    InterpretationFinishSender,
)
from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import read_finish_plan
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_interpretation_finish_root import snapshot
from tests.integration.test_transaction_interpretation_finish import incoming, planner
from tests.integration.test_transaction_interpretation_state import original as original
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


async def prior(p, *, question=True, abort=True):
    value = incoming(p, question)
    intent = value.intent(p.request_id)
    plan = await planner(p).candidate(value, intent)
    previous = await p.host.store.reserve(intent.request, plan)
    p.host.executions.append(previous.execution)
    if abort:
        assert await p.host.store.abort(previous.execution)

    class Runtime:
        store = p.host.store
        calls = 0

        async def retry(self, authorization):
            current = await self.store.retry(authorization)
            p.host.executions.append(current.execution)
            return current

        async def execute(self, owned, local):
            self.calls += 1
            return Result(await local(owned), ())

    runtime = Runtime()
    sender = InterpretationFinishSender(
        p.host.kit.store,
        p.host.archives,
        runtime,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
    )
    sender._planner.candidate = AsyncMock(side_effect=AssertionError("Retry must reuse first plan"))
    return value, intent, previous.execution, sender, runtime


@pytest.mark.parametrize("question", [False, True])
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_exact_aborted_finalization_reuses_first_wire_question_identity_and_original_root(
    original,
    monkeypatch,
    question,
    lost_reply,
):
    p = original
    value, intent, old, sender, runtime = await prior(p, question=question)
    with monkeypatch.context() as patch:
        if lost_reply:
            commit = EventSession.commit

            async def discarded(db):
                await commit(db)
                raise ConnectionError("client discarded completed finalization reply")

            patch.setattr(EventSession, "commit", discarded)
        await sender.retry(value.claim, value.result, old)
    current = await p.host.store.lookup(intent.request)
    assert current.execution.plan == old.plan and current.execution.fence == old.fence + 1
    assert await StoreChecker(p.host.store).check(old.references[0]) == Decision.ROLLBACK
    assert (
        await StoreChecker(p.host.store).check(current.execution.references[0]) == Decision.COMMIT
    )
    facts, first, question_id = read_finish_plan(
        value, intent, old.plan, route_resolver=legacy_routes()
    )
    assert await p.host.archives.read_committed(first.envelope().message_id) == first
    after = await snapshot(p.host.kit, value.claim)
    assert after[0][0]["version"] == value.claim.session.version + 1
    assert after[2][0]["status"] == "done" and runtime.calls == 1
    if question:
        assert after[4][0]["id"] == question_id
    await sender(value.claim, value.result)  # ordinary duplicate is evidence lookup only
    assert runtime.calls == 1 and await snapshot(p.host.kit, value.claim) == after
    sender._planner.candidate.assert_not_called()


async def test_prepared_finalization_is_not_retry_permission(original):
    p = original
    value, intent, old, sender, runtime = await prior(p, abort=False)
    before = await snapshot(p.host.kit, value.claim)
    with pytest.raises(StaleExecutionError):
        await sender.retry(value.claim, value.result, old)
    assert runtime.calls == 0 and await snapshot(p.host.kit, value.claim) == before
    assert await StoreChecker(p.host.store).check(old.references[0]) == Decision.UNKNOWN


async def test_late_finalization_retry_cannot_overtake_current_winner(original):
    p = original
    value, intent, old, sender, runtime = await prior(p)
    winner = await p.host.store.retry(RetryAuthorization(old))
    p.host.executions.append(winner.execution)
    before = await snapshot(p.host.kit, value.claim)
    with pytest.raises(StaleExecutionError):
        await sender.retry(value.claim, value.result, old)
    assert runtime.calls == 0 and await snapshot(p.host.kit, value.claim) == before
    assert (await p.host.store.lookup(intent.request)).execution.fence == winner.execution.fence


async def test_competing_original_input_refuses_half_preparation_or_new_fence(original):
    p = original
    value, intent, old, sender, runtime = await prior(p)
    async with p.host.tx.open() as db:
        await db.execute(
            update(jobs).where(jobs.c.id == value.claim.job_id).values(answer="changed")
        )
        await db.commit()
    before = await snapshot(p.host.kit, value.claim)
    with pytest.raises(LeaseLost):
        await sender.retry(value.claim, value.result, old)
    assert runtime.calls == 0 and len(p.host.executions) == 1
    assert await snapshot(p.host.kit, value.claim) == before
    assert await StoreChecker(p.host.store).check(old.references[0]) == Decision.ROLLBACK
