"""Actual original dispatch roots/MySQL; controlled half outcome is not Broker fault proof."""

from datetime import timedelta

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import update

from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
    CandidateDispatchPending,
    CandidateDispatchRecovery,
    CandidateDispatchSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_state import (
    CandidateDispatchPlanner,
    CandidateDispatchWrite,
    candidate_dispatch_intent,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
    WorkerDispatchPending,
    WorkerDispatchRecovery,
    WorkerDispatchSender,
)
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
    WorkerDispatchPlanner,
    WorkerDispatchWrite,
    worker_dispatch_intent,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_worker_dispatch import original as original

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("fault", ["none", "half_rejected", "final_unknown", "already_committed"])
async def test_host_dispatch_recovery_never_repeats_commit_and_retains_original_model_budget(
    original, candidate, fault
):
    p = original
    if candidate:
        async with p.tx.open() as db:
            await db.execute(
                update(evaluation_runs)
                .where(evaluation_runs.c.run_id == str(p.run_id))
                .values(execution_mode="candidate_v2")
            )
            await db.commit()
        planner = CandidateDispatchPlanner(
            p.host.tx,
            p.schemas,
            signing_key=p.host.keys["ai.sign"],
            recipient_key=p.host.keys["qs.encrypt"],
            limit=3,
            clock=lambda: AT,
        )
        intent = candidate_dispatch_intent(p.run_id, 3, 1)
    else:
        planner = WorkerDispatchPlanner(
            p.host.tx,
            p.schemas,
            signing_key=p.host.keys["ai.sign"],
            recipient_key=p.host.keys["qs.encrypt"],
            clock=lambda: AT,
        )
        intent = worker_dispatch_intent(p.run_id, 3, 1, "worker:original")
    first, inputs = await planner.candidate(p.run_id, 3, 1, "worker:original")
    old = await p.host.store.reserve(intent.request, first)
    assert old.acquired
    p.host.executions.append(old.execution)
    if fault == "already_committed":
        business = CandidateDispatchWrite() if candidate else WorkerDispatchWrite(p.schemas)
        root = EvaluationStateRoot(
            p.host.tx,
            p.host.archives,
            p.host.store,
            intent,
            first,
            business,
            isolation_level="REPEATABLE READ",
        )
        assert await root(old.execution) == LocalOutcome.COMMITTED
    else:
        assert await p.host.store.abort(old.execution)
    before, calls = await rows(p.tx, p.run_id), p.gateway.calls
    pool = ModelCapacity({inputs.route.provider: ProviderCapacity(2, 1)}, 1)

    class DatabaseOnlyRuntime:
        store = p.host.store
        local_calls = 0
        replans = 0

        async def reserve(self, request, winner):
            raise AssertionError("Aborted dispatch must use the public Replan gate")

        async def replan(self, authorization):
            self.replans += 1
            assert pool.try_acquire(inputs.route.provider, evaluation=True) is None
            result = await self.store.replan(authorization)
            if result.acquired:
                p.host.executions.append(result.execution)
            return result

        async def execute(self, owned, local):
            if fault == "half_rejected":
                assert await self.store.abort(owned)
                raise TransactionSendError(
                    "controlled_half_rejected", Result(LocalOutcome.NOT_STARTED, ())
                )
            self.local_calls += 1
            result = Result(await local(owned), ())
            if fault == "final_unknown":
                raise TransactionSendError("client_discarded_final_reply", result)
            return result

    owner = DatabaseOnlyRuntime()
    planner.clock = lambda: AT + timedelta(seconds=10)
    if candidate:
        strict = CandidateDispatchSender(
            p.host.tx, p.host.archives, owner, "worker:original", planner, capacity=pool
        )
        dispatch = CandidateDispatchRecovery(strict)
    else:
        strict = WorkerDispatchSender(
            p.host.tx, p.host.archives, owner, p.schemas, "worker:original", planner, capacity=pool
        )
        dispatch = WorkerDispatchRecovery(strict)
    if fault == "already_committed":
        with pytest.raises((CandidateDispatchPending, WorkerDispatchPending)):
            await dispatch(p.run_id, 3, 1)
        assert owner.replans == owner.local_calls == 0
        assert await rows(p.tx, p.run_id) == before
    elif fault == "half_rejected":
        with pytest.raises(TransactionSendError):
            await dispatch(p.run_id, 3, 1)
        assert owner.replans == 1 and owner.local_calls == 0
        assert await rows(p.tx, p.run_id) == before
    else:
        prepared = await dispatch(p.run_id, 3, 1)
        assert prepared.capacity_token is not None
        assert prepared.cp.claimed_at == AT + timedelta(seconds=10)
        assert owner.replans == owner.local_calls == 1
        assert pool.try_acquire(inputs.route.provider, evaluation=True) is None
        prepared.capacity_token.release()
        assert (
            await StoreChecker(p.host.store).check(p.host.executions[-1].references[0])
            == Decision.COMMIT
        )
        with pytest.raises((CandidateDispatchPending, WorkerDispatchPending)):
            await dispatch(p.run_id, 3, 1)
        assert owner.replans == owner.local_calls == 1
    assert p.gateway.calls == calls
    token = pool.try_acquire(inputs.route.provider, evaluation=True)
    assert token is not None
    token.release()
    assert await StoreChecker(p.host.store).check(old.execution.references[0]) == (
        Decision.COMMIT if fault == "already_committed" else Decision.ROLLBACK
    )
