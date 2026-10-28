"""Original two-CAS serial root and model-permit handoff; no actual Broker claim."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    BindingError,
    Decision,
    LocalOutcome,
    Result,
    StoreChecker,
)
from sqlalchemy import delete, text

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
    WorkerDispatchPending,
    WorkerDispatchSender,
)
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
    WorkerDispatchPlanner,
    WorkerDispatchWrite,
    inputs_digest,
    read_worker_dispatch_plan,
    worker_dispatch_intent,
)
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


@pytest.fixture
async def original(host, ready):
    tx, run_id, _, routes, schemas = ready
    gateway = Gateway(ready)
    previous_recorder = host.tx.database.state_events
    try:
        yield SimpleNamespace(**locals())
    finally:
        host.tx.database.state_events = previous_recorder
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
            )
            await db.commit()


def planner(p):
    return WorkerDispatchPlanner(
        p.host.tx,
        p.schemas,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
        clock=lambda: AT + timedelta(seconds=1),
        route_resolver=legacy_routes(),
    )


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
async def test_original_preparation_dispatch_ledger_and_witness_share_both_cas_and_root(
    original, trigger_admin, monkeypatch, semantic, fault
):
    p = original
    version = (await step(p.ready, p.gateway)).version if semantic else 3
    before, calls = await snapshot(p.tx, p.run_id), p.gateway.calls
    candidate, inputs = await planner(p).candidate(p.run_id, version, 1, "worker:original")
    intent = worker_dispatch_intent(p.run_id, version, 1, "worker:original")
    facts, operation, first = read_worker_dispatch_plan(
        intent, candidate, route_resolver=legacy_routes()
    )
    assert await snapshot(p.tx, p.run_id) == before and p.gateway.calls == calls
    assert operation.model_inputs_digest == inputs_digest(inputs)
    reserved = await p.host.store.reserve(intent.request, candidate)
    assert reserved.acquired
    p.host.executions.append(reserved.execution)
    business = WorkerDispatchWrite(p.schemas)
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        candidate,
        business,
        isolation_level="REPEATABLE READ",
        route_resolver=legacy_routes(),
    )
    trigger = None
    try:
        p.host.tx.database.state_events = SimpleNamespace(
            flush=AsyncMock(
                side_effect=AssertionError("Legacy commit hook cannot publish new state")
            )
        )
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "worker_dispatch_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated worker archive refusal'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated worker witness refusal'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        with monkeypatch.context() as patch:
            # Identity generation, Seal and the model are absent from the original callback.
            from qs_ai.infrastructure.persistence.mysql import evaluation_step
            from qs_ai.infrastructure.workflow_transport import worker_dispatch_state

            def no_identity():
                raise AssertionError("No UUID generation in original root")

            patch.setattr(evaluation_step, "uuid4", no_identity)
            patch.setattr(worker_dispatch_state, "uuid4", no_identity)
            patch.setattr(worker_dispatch_state, "prepare", no_identity)
            if fault == "client_reply_discarded":
                real_commit = EventSession.commit

                async def discarded(db):
                    await real_commit(db)
                    raise ConnectionError("client discarded completed serial dispatch commit reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(reserved.execution)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(reserved.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await snapshot(p.tx, p.run_id)
        if committed:
            prepared = business.prepared
            assert prepared is not None
            assert (
                prepared.cp.phase == "prepared" and prepared.state.checkpoint.phase == "dispatching"
            )
            assert prepared.cp == operation.checkpoint and prepared.state.version == version + 2
            assert (prepared.messages, prepared.route, prepared.schema) == (
                inputs.messages,
                inputs.route,
                inputs.schema,
            )
            assert prepared.capacity_token is None
            assert current[0][2]["version"] == version + 2
            assert len(current[1][0]) == len(before[1][0]) + 1
            assert current[1][1:] == before[1][1:]
        else:
            assert current == before  # Captured prepared values never prove commit.
        assert p.gateway.calls == calls
        assert await p.host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
        with pytest.raises(BindingError):
            await root(reserved.execution)
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize("reject", [False, True])
async def test_explicit_sender_transfers_permit_once_only_after_precise_local_commit(
    original, semantic, reject
):
    p = original
    version = (await step(p.ready, p.gateway)).version if semantic else 3
    before, calls = await snapshot(p.tx, p.run_id), p.gateway.calls
    first_planner = planner(p)
    preview, inputs = await first_planner.candidate(p.run_id, version, 1, "worker:original")
    pool = ModelCapacity({inputs.route.provider: ProviderCapacity(2, 1)}, 1)

    class Runtime:
        routing = legacy_routes()

        store = p.host.store
        calls = 0

        async def reserve(self, request, candidate):
            reserved = await self.store.reserve(request, candidate)
            if reserved.acquired:
                p.host.executions.append(reserved.execution)
            return reserved

        async def execute(self, owned, local):
            assert pool.try_acquire(inputs.route.provider, evaluation=True) is None
            if reject:
                raise TransactionSendError(
                    "isolated_worker_half_rejected", Result(LocalOutcome.NOT_STARTED, ())
                )
            self.calls += 1
            result = Result(await local(owned), ())
            raise TransactionSendError("isolated_worker_final_unknown", result)

    runtime = Runtime()
    dispatch = WorkerDispatchSender(
        p.host.tx,
        p.host.archives,
        runtime,
        p.schemas,
        "worker:original",
        first_planner,
        capacity=pool,
    )
    if reject:
        with pytest.raises(TransactionSendError):
            await dispatch(p.run_id, version, 1)
        assert runtime.calls == 0 and await snapshot(p.tx, p.run_id) == before
        token = pool.try_acquire(inputs.route.provider, evaluation=True)
        assert token is not None
        token.release()
    else:
        value = await dispatch(p.run_id, version, 1)
        assert value.state.version == version + 2 and value.capacity_token is not None
        assert pool.try_acquire(inputs.route.provider, evaluation=True) is None
        value.capacity_token.release()
        token = pool.try_acquire(inputs.route.provider, evaluation=True)
        assert token is not None
        token.release()
        with pytest.raises(WorkerDispatchPending):
            await dispatch(p.run_id, version, 1)
        assert runtime.calls == 1
        assert (
            await StoreChecker(p.host.store).check(p.host.executions[-1].references[0])
            == Decision.COMMIT
        )
    assert p.gateway.calls == calls


async def test_later_original_cancellation_wins_before_dispatch_without_provider_call(original):
    p = original
    before = await snapshot(p.tx, p.run_id)
    candidate, _ = await planner(p).candidate(p.run_id, 3, 1, "worker:original")
    intent = worker_dispatch_intent(p.run_id, 3, 1, "worker:original")
    reserved = await p.host.store.reserve(intent.request, candidate)
    p.host.executions.append(reserved.execution)
    await MySQLEvaluationManagement(p.tx).cancel(
        ManagementScope(p.run_id, 1, 42),
        3,
        "original competing cancellation",
        AT + timedelta(seconds=2),
        discard=False,
        confirm=True,
    )
    expected = await snapshot(p.tx, p.run_id)
    business = WorkerDispatchWrite(p.schemas)
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        candidate,
        business,
        isolation_level="REPEATABLE READ",
        route_resolver=legacy_routes(),
    )
    assert await root(reserved.execution) == LocalOutcome.ABORTED
    assert await snapshot(p.tx, p.run_id) == expected and expected != before
    assert p.gateway.calls == 0 and business.prepared is None


@pytest.mark.parametrize("semantic", [False, True])
async def test_readonly_inputs_use_one_primary_snapshot_and_seal_after_source_cleanup(
    original, monkeypatch, semantic
):
    from sqlalchemy import event

    from qs_ai.infrastructure.workflow_transport import worker_dispatch_state

    p = original
    version = (await step(p.ready, p.gateway)).version if semantic else 3
    before, calls, statements = await snapshot(p.tx, p.run_id), p.gateway.calls, []
    p.schemas.get = AsyncMock(
        side_effect=AssertionError("No second schema connection in planning or root")
    )
    active = 0
    sealed = False
    engine = p.host.tx.database.engine.sync_engine
    original_seal = worker_dispatch_state.prepare

    def checkout(*args):
        nonlocal active
        active += 1

    def checkin(*args):
        nonlocal active
        active -= 1

    def record(_connection, _cursor, statement, *_):
        statements.append(statement.strip().upper())

    def seal(*args, **kwargs):
        nonlocal sealed
        assert active == 0
        sealed = True
        return original_seal(*args, **kwargs)

    event.listen(engine, "checkout", checkout)
    event.listen(engine, "checkin", checkin)
    event.listen(engine, "before_cursor_execute", record)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(worker_dispatch_state, "prepare", seal)
            candidate, inputs = await planner(p).candidate(p.run_id, version, 1, "worker:original")
    finally:
        event.remove(engine, "checkout", checkout)
        event.remove(engine, "checkin", checkin)
        event.remove(engine, "before_cursor_execute", record)
    assert active == 0 and sealed and statements
    assert all(s.startswith("SELECT ") for s in statements)
    assert not any("FOR UPDATE" in s for s in statements)
    assert await snapshot(p.tx, p.run_id) == before and p.gateway.calls == calls
    p.schemas.get.assert_not_called()
    intent = worker_dispatch_intent(p.run_id, version, 1, "worker:original")
    _, operation, _ = read_worker_dispatch_plan(intent, candidate, route_resolver=legacy_routes())
    assert operation.model_inputs_digest == inputs_digest(inputs)
