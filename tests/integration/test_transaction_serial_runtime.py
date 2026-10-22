"""Actual paired serial graph/MySQL roots; controlled transport is not Broker fault proof."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import delete

from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.bootstrap.evaluation_transactions import transaction_evaluation_worker
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor
from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
    read_serial_response_reference,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_response_receipts
from qs_ai.infrastructure.workflow_transport.serial_generation_sender import SerialGenerationSender
from qs_ai.infrastructure.workflow_transport.serial_generation_state import SerialGenerationPlanner
from qs_ai.infrastructure.workflow_transport.serial_recovery_runtime import SerialRecoveryRuntime
from qs_ai.infrastructure.workflow_transport.serial_runtime import SerialRuntime
from qs_ai.infrastructure.workflow_transport.serial_semantic_sender import SerialSemanticSender
from qs_ai.infrastructure.workflow_transport.serial_semantic_state import SerialSemanticPlanner
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import WorkerDispatchSender
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import WorkerDispatchPlanner
from qs_ai.infrastructure.workflows.evaluation import execute_step
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_worker_dispatch import original as original

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ["generation", "semantic"])
@pytest.mark.parametrize("fault", ["none", "terminal_unavailable", "client_final_discarded"])
@pytest.mark.parametrize("entry", ["graph", "worker"])
async def test_paired_serial_graph_and_first_response_recovery_keep_original_model_invocation(
    original, kind, fault, entry
):
    p = original
    version = (await step(p.ready, p.gateway)).version if kind == "semantic" else 3
    calls = p.gateway.calls
    keys = dict(signing_key=p.host.keys["ai.sign"], recipient_key=p.host.keys["qs.encrypt"])

    class OriginalRootRuntime:
        store = p.host.store
        executed = 0

        async def reserve(self, request, winner):
            value = await self.store.reserve(request, winner)
            if value.acquired:
                p.host.executions.append(value.execution)
            return value

        async def retry(self, authorization):
            value = await self.store.retry(authorization)
            if value.acquired:
                p.host.executions.append(value.execution)
            return value

        async def execute(self, owned, local):
            self.executed += 1
            if fault == "terminal_unavailable" and self.executed == 2:
                assert await self.store.abort(owned)
                raise TransactionSendError(
                    "controlled terminal unavailable", Result(LocalOutcome.NOT_STARTED, ())
                )
            result = Result(await local(owned), ())
            if fault == "client_final_discarded":
                raise TransactionSendError("client discarded final result", result)
            return result

    transport = OriginalRootRuntime()
    provider = executable_route(p.routes.get.return_value).provider
    capacity = ModelCapacity({provider: ProviderCapacity(2, 1)}, 1)
    runtime = SerialRuntime(
        WorkerDispatchSender(
            p.host.tx,
            p.host.archives,
            transport,
            p.schemas,
            "worker:original",
            WorkerDispatchPlanner(
                p.host.tx, p.schemas, clock=lambda: AT + timedelta(seconds=2), **keys
            ),
            capacity=capacity,
        ),
        SerialGenerationSender(
            p.host.tx,
            p.host.archives,
            transport,
            SerialGenerationPlanner(p.host.tx, p.routes, **keys),
        ),
        SerialSemanticSender(
            p.host.tx, p.host.archives, transport, SerialSemanticPlanner(p.host.tx, **keys)
        ),
    )
    p.host.tx.database.state_events = SimpleNamespace(
        flush=AsyncMock(
            side_effect=AssertionError(
                "Paired serial graph cannot publish through legacy commit hook"
            )
        )
    )

    async def call():
        if entry == "worker":
            worker = transaction_evaluation_worker(
                p.host.tx,
                p.host.archives,
                transport,
                p.gateway,
                p.routes,
                p.schemas,
                "worker:original",
                candidate_limit=1,
                capacity=capacity,
                recovery_cursor=RecoveryCursor(),
                clock=lambda: AT + timedelta(seconds=3),
                **keys,
            )
            assert await worker.once()
            return SimpleNamespace(version=(await rows(p.tx, p.run_id))[2]["version"])
        return await execute_step(
            p.host.tx,
            p.run_id,
            version,
            1,
            "worker:original",
            p.gateway,
            p.routes,
            p.schemas,
            clock=lambda: AT + timedelta(seconds=3),
            serial_runtime=runtime,
        )

    try:
        if fault == "terminal_unavailable":
            with pytest.raises(TransactionSendError):
                await call()
            _, _, current = await rows(p.tx, p.run_id)
            assert current["version"] == version + 2
            async with p.tx.open() as db:
                reference = await read_serial_response_reference(
                    db, p.run_id, 1, current["checkpoint_json"]["invocation_id"]
                )
            assert reference is not None
            recovery = SerialRecoveryRuntime(
                p.host.tx, p.host.archives, transport, p.routes, recover_aborted=True, **keys
            )
            cp = reference.checkpoint
            assert await recovery(
                p.run_id,
                reference.version,
                1,
                cp.invocation_id,
                cp.lease_expires_at,
                cp.lease_expires_at,
                "system:recovery",
            )
        else:
            assert (await call()).version == version + 3
        _, _, current = await rows(p.tx, p.run_id)
        assert current["version"] == version + 3 and current["checkpoint_json"] is None
        assert p.gateway.calls == calls + 1
        available = capacity.try_acquire(provider, evaluation=True)
        assert available is not None
        available.release()
        expected = (
            [Decision.COMMIT, Decision.ROLLBACK, Decision.COMMIT]
            if fault == "terminal_unavailable"
            else [Decision.COMMIT, Decision.COMMIT]
        )
        assert [
            await StoreChecker(p.host.store).check(e.references[0]) for e in p.host.executions
        ] == expected
        p.host.tx.database.state_events.flush.assert_not_called()
    finally:
        async with p.tx.open() as db:
            await db.execute(
                delete(evaluation_response_receipts).where(
                    evaluation_response_receipts.c.run_id == str(p.run_id)
                )
            )
            await db.commit()
