"""Actual bounded MySQL scan and original roots; transport is simulated explicitly."""

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, LocalOutcome, Result, State, StoreChecker

from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
    TransactionalInterpretationExecutionStore,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_interpretation_claim_root import rows
from tests.integration.test_transaction_interpretation_claim import candidate
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


def adapter(host, *, half_unknown=False):
    class Runtime:
        routing = legacy_routes()

        store = host.store
        calls = 0

        def stats(self):
            return {"ready": True}

        async def reserve(self, request, plan):
            value = await self.store.reserve(request, plan)
            if value.acquired:
                host.executions.append(value.execution)
            return value

        async def retry(self, authorization):
            value = await self.store.retry(authorization)
            host.executions.append(value.execution)
            return value

        async def execute(self, owned, local):
            self.calls += 1
            if half_unknown:
                raise TransactionSendError(
                    "simulated half unknown", Result(LocalOutcome.NOT_STARTED, ())
                )
            return Result(await local(owned), ())

    runtime = Runtime()
    value = TransactionalInterpretationExecutionStore(
        host.kit.store,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    return value, runtime


@pytest.mark.parametrize("exhausted", [False, True])
@pytest.mark.parametrize("old_abort", [False, True])
async def test_original_due_scan_commits_once_and_only_explicitly_retries_exact_old_abort(
    host,
    exhausted,
    old_abort,
):
    source, before, plan = await candidate(host, exhausted)
    previous = None
    if old_abort:
        previous = await host.store.reserve(source.intent(60).request, plan)
        host.executions.append(previous.execution)
        assert await host.store.abort(previous.execution)
    worker, runtime = adapter(host)
    claim = await worker.claim(60)
    assert (claim is None) is exhausted and runtime.calls == 1
    current = await host.store.lookup(source.intent(60).request)
    assert current.state == State.COMMITTED
    assert await StoreChecker(host.store).check(current.execution.references[0]) == Decision.COMMIT
    if previous is not None:
        assert current.execution.plan == previous.execution.plan
        assert current.execution.fence == previous.execution.fence + 1
        assert (
            await StoreChecker(host.store).check(previous.execution.references[0])
            == Decision.ROLLBACK
        )
    after = await rows(host.kit, source.session.id)
    assert after[0][0]["version"] == source.session.version + (2 if exhausted else 1)
    assert after[1][0]["attempt"] == (3 if exhausted else 1)
    assert len(after[4]) == 1
    assert await worker.claim(60) is None and runtime.calls == 1


async def test_pending_old_execution_does_not_authorize_claim_or_original_job_mutation(host):
    source, before, plan = await candidate(host)
    previous = await host.store.reserve(source.intent(60).request, plan)
    host.executions.append(previous.execution)
    worker, runtime = adapter(host)
    assert await worker.claim(60) is None and runtime.calls == 0
    assert await rows(host.kit, source.session.id) == before
    assert (
        await StoreChecker(host.store).check(previous.execution.references[0]) == Decision.UNKNOWN
    )


async def test_half_unknown_never_runs_local_claim_or_no_event_quota_maintenance(host):
    source, before, plan = await candidate(host, snapshot=True)
    worker, runtime = adapter(host, half_unknown=True)
    with pytest.raises(TransactionSendError):
        await worker.claim(60)
    assert runtime.calls == 1 and await rows(host.kit, source.session.id) == before
    current = await host.store.lookup(source.intent(60).request)
    assert current.state == State.PREPARED
    assert await StoreChecker(host.store).check(current.execution.references[0]) == Decision.UNKNOWN
