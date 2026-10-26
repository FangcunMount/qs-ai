"""Real explicit old-Aborted CAS and original roots; transport remains simulated."""

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

from qs_ai.infrastructure.persistence.mysql.schema import sessions
from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
    InterpretationClaimPending,
    InterpretationClaimSender,
)
from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import read_claim_plan
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_interpretation_claim_root import rows
from tests.integration.test_transaction_interpretation_claim import candidate
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys

pytestmark = pytest.mark.integration


async def original(host, exhausted=False, *, abort=True, conflict=False):
    source, before, plan = await candidate(host, exhausted)
    old = await host.store.reserve(source.intent(60).request, plan)
    host.executions.append(old.execution)
    if abort:
        assert await host.store.abort(old.execution)

    class Runtime:
        store = host.store
        calls = 0

        async def retry(self, authorization):
            value = await self.store.retry(authorization)
            host.executions.append(value.execution)
            return value

        async def execute(self, owned, local):
            self.calls += 1
            if conflict:
                async with host.tx.open() as db:
                    await db.execute(
                        update(sessions)
                        .where(sessions.c.id == source.session.id)
                        .values(
                            goal="competing original retry source",
                        )
                    )
                    await db.commit()
            return Result(await local(owned), ())

    runtime = Runtime()
    sender = InterpretationClaimSender(
        host.kit.store,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    sender._planner.candidate = AsyncMock(
        side_effect=AssertionError("Explicit retry must not Seal")
    )
    return source, before, old.execution, sender, runtime


@pytest.mark.parametrize("exhausted", [False, True])
async def test_explicit_retry_preserves_old_abort_and_first_wire_on_a_new_exact_fence(
    host, exhausted
):
    source, before, old, sender, runtime = await original(host, exhausted)
    result = await sender.retry(source, 60, old)
    assert (result is None) is exhausted
    current = await host.store.lookup(source.intent(60).request)
    assert current.execution.fence == old.fence + 1 and current.execution.plan == old.plan
    _, first = read_claim_plan(source, 60, old.plan)
    assert await host.archives.read_committed(first.envelope().message_id) == first
    assert await StoreChecker(host.store).check(old.references[0]) == Decision.ROLLBACK
    assert await StoreChecker(host.store).check(current.execution.references[0]) == Decision.COMMIT
    after = await rows(host.kit, source.session.id)
    assert after[0][0]["version"] == source.session.version + (2 if exhausted else 1)
    assert after[1][0]["attempt"] == (3 if exhausted else 1)
    assert runtime.calls == 1 and len(after[4]) == 1
    sender._planner.candidate.assert_not_called()


async def test_prepared_old_claim_is_not_explicit_retry_permission(host):
    source, before, old, sender, runtime = await original(host, abort=False)
    with pytest.raises(StaleExecutionError):
        await sender.retry(source, 60, old)
    assert await rows(host.kit, source.session.id) == before
    assert runtime.calls == 0
    assert await StoreChecker(host.store).check(old.references[0]) == Decision.UNKNOWN


async def test_late_old_authorization_cannot_overtake_current_retry_winner(host):
    source, before, old, sender, runtime = await original(host)
    winner = await host.store.retry(RetryAuthorization(old))
    host.executions.append(winner.execution)
    with pytest.raises(StaleExecutionError):
        await sender.retry(source, 60, old)
    current = await host.store.lookup(source.intent(60).request)
    assert current.execution.fence == winner.execution.fence
    assert await rows(host.kit, source.session.id) == before and runtime.calls == 0


async def test_source_conflict_after_new_retry_fence_rolls_back_without_changing_old_abort(host):
    source, before, old, sender, runtime = await original(host, conflict=True)
    with pytest.raises(InterpretationClaimPending):
        await sender.retry(source, 60, old)
    current = await host.store.lookup(source.intent(60).request)
    assert current.execution.fence == old.fence + 1
    assert await StoreChecker(host.store).check(old.references[0]) == Decision.ROLLBACK
    assert (
        await StoreChecker(host.store).check(current.execution.references[0]) == Decision.ROLLBACK
    )
    after = await rows(host.kit, source.session.id)
    assert after[0][0]["goal"] == "competing original retry source"
    assert after[0][0]["version"] == before[0][0]["version"] and after[1:] == before[1:]
