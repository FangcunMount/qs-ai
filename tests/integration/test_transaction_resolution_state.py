"""Original manual resolution and capacity share the exact SDK root; no Broker claim."""

import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import grpc
import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import delete, text

from qs_ai.application.evaluation.capacity import CapacityExceeded
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationManagementStore, ManagementScope
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import run_release
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_capacity_reservations,
    evaluation_checkpoints,
    evaluation_runs,
)
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.evaluation_governance_dispatch import (
    GovernanceStateDispatcher,
)
from qs_ai.infrastructure.workflow_transport.resolution_state import (
    ResolutionStatePlanner,
    ResolutionStateRoot,
    read_resolution_plan,
)
from qs_ai.infrastructure.workflow_transport.resolution_state_plan import ResolutionStateOperation
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
    TransactionalEvaluationManagement,
)
from tests.integration.test_evaluation_recovery import pending, recover
from tests.integration.test_evaluation_resolution import decision
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys
from tests.test_grpc_commands import Aborted, Context

pytestmark = pytest.mark.integration


@pytest.fixture
async def unknown(host, ready):
    tx, run_id, *_ = ready
    gateway = Gateway(ready)
    # Seed a genuine semantic unknown, retaining its original provider ledger.
    version = (await step(ready, gateway)).version
    state = await recover(ready, await pending(ready, dispatched=True, version=version))
    scope = ManagementScope(run_id, 1, 42)
    original = MySQLEvaluationManagement(host.tx)
    before, provider_calls = await snapshot(tx, run_id), gateway.calls
    try:
        yield SimpleNamespace(**locals())
    finally:
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
            )


@pytest.mark.parametrize("kind", ["authorize_replacement", "cancel_run"])
@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
async def test_original_resolution_audit_unknown_evidence_and_witness_commit_or_rollback_together(
    unknown, trigger_admin, monkeypatch, kind, fault
):
    p = unknown
    value = replace(decision(), actor=p.scope.actor, decision=kind)
    operation = ResolutionStateOperation(p.scope, uuid4(), p.state.version, value, True)
    winner = await ResolutionStatePlanner(
        p.original, signing_key=p.host.keys["ai.sign"], recipient_key=p.host.keys["qs.encrypt"]
    ).candidate(operation, value.resolved_at)
    reserved = await p.host.store.reserve(operation.intent.request, winner)
    assert reserved.acquired
    owned = reserved.execution
    p.host.executions.append(owned)
    _, first = read_resolution_plan(operation, winner)
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "resolution_root_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated resolution archive rejection'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated resolution witness rejection'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        root = ResolutionStateRoot(p.original, p.host.archives, p.host.store, operation, winner)
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                actual_commit = EventSession.commit

                async def discarded(db):
                    await actual_commit(db)
                    raise ConnectionError("client discarded completed resolution commit reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await snapshot(p.tx, p.run_id)
        assert current[1] == p.before[1] and p.gateway.calls == p.provider_calls
        if committed:
            run, _, checkpoint = await rows(p.tx, p.run_id)
            assert checkpoint["version"] == p.state.version + 1
            assert run["progress_json"]["status"] == (
                "collecting" if kind == "authorize_replacement" else "canceled"
            )
            audit = run["progress_json"]["result_unknown_resolutions"][-1]
            assert audit["resolved_at"] == value.resolved_at.isoformat()
            assert audit["execution_id"] == value.execution_id and audit["actor"] == p.scope.actor
            assert root.view.version == p.state.version + 1
        else:
            assert current == p.before
        assert await p.host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))


@pytest.mark.parametrize("reject", [False, True])
async def test_actual_resolve_handler_waits_for_preparation_and_never_invokes_a_provider(
    unknown, reject
):
    from contextlib import asynccontextmanager

    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.transport.grpc.evaluation import EvaluationManagement

    p = unknown

    class DatabaseOnlyRuntime:
        store = p.host.store
        calls = 0

        async def reserve(self, request, winner):
            reserved = await self.store.reserve(request, winner)
            if reserved.acquired:
                p.host.executions.append(reserved.execution)
            return reserved

        async def execute(self, owned, local):
            if reject:
                raise TransactionSendError(
                    "isolated_half_refused", Result(LocalOutcome.NOT_STARTED, ())
                )
            self.calls += 1
            result = Result(await local(owned), ())
            # Final RPC uncertainty preserves the exact local committed result.
            raise TransactionSendError("isolated_final_unknown", result)

    runtime = DatabaseOnlyRuntime()
    dispatch = GovernanceStateDispatcher(
        p.host.tx,
        p.host.archives,
        runtime,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
        resolution_management=p.original,
    )
    sender = TransactionalEvaluationManagement(p.original, dispatch)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return sender

        yield Scope()

    request = pb.UnknownResolutionCommand(
        scope=pb.EvaluationQuery(run_id=str(p.run_id), organization_id=1, operator_user_id=42),
        expected_version=p.state.version,
        execution_id="execution:dead",
        decision="authorize_replacement",
        reason="原人工批准",
        acknowledged_duplicate_call_and_cost_risk=True,
        confirm=True,
    )
    service = EvaluationManagement(container)
    if reject:
        with pytest.raises(Aborted) as failure:
            await service.ResolveUnknown(request, Context())
        assert failure.value.args[0] == grpc.StatusCode.UNAVAILABLE
        assert runtime.calls == 0 and await snapshot(p.tx, p.run_id) == p.before
        assert await StoreChecker(p.host.store).check(p.host.executions[-1].references[0]) == (
            Decision.UNKNOWN
        )
    else:
        response = await service.ResolveUnknown(request, Context())
        assert response.status == "collecting" and response.version == p.state.version + 1
        assert runtime.calls == 1
        assert await StoreChecker(p.host.store).check(p.host.executions[-1].references[0]) == (
            Decision.COMMIT
        )
        assert (await snapshot(p.tx, p.run_id))[1] == p.before[1]
    assert p.gateway.calls == p.provider_calls


@pytest.mark.parametrize("competing", ["resolution", "capacity"])
async def test_prepared_resolution_cannot_override_later_decision_or_bypass_host_capacity(
    unknown, competing
):
    p = unknown
    value = replace(decision(), actor=p.scope.actor)
    operation = ResolutionStateOperation(p.scope, uuid4(), p.state.version, value, True)
    winner = await ResolutionStatePlanner(
        p.original, signing_key=p.host.keys["ai.sign"], recipient_key=p.host.keys["qs.encrypt"]
    ).candidate(operation, value.resolved_at)
    reserved = await p.host.store.reserve(operation.intent.request, winner)
    assert reserved.acquired
    owned = reserved.execution
    p.host.executions.append(owned)
    _, first = read_resolution_plan(operation, winner)
    other = None
    try:
        if competing == "resolution":
            await MySQLEvaluationManagement(p.tx).resolve(
                p.scope, p.state.version, replace(value, decision="cancel_run"), confirm=True
            )
        else:
            # A real second Run wins the same host's sole active slot after
            # planning; use the existing release and normal original Start.
            other = ManagementScope(uuid4(), p.scope.organization_id, 42)
            release = run_release(json.loads(p.before[0][0]["definition_json"]))
            async with p.tx.open() as db:
                await create_run(
                    db,
                    other.run_id,
                    release,
                    other.organization_id,
                    other.actor,
                    "并发容量准入",
                    value.resolved_at,
                )
                await db.commit()
            await MySQLEvaluationManagement(p.tx).start(
                other, 1, "原宿主并发准入", value.resolved_at, confirm=True
            )
        expected = await snapshot(p.tx, p.run_id)
        root = ResolutionStateRoot(p.original, p.host.archives, p.host.store, operation, winner)
        assert await root(owned) == LocalOutcome.ABORTED
        assert isinstance(
            root.business_error,
            CheckpointConflict if competing == "resolution" else CapacityExceeded,
        )
        assert await StoreChecker(p.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await snapshot(p.tx, p.run_id) == expected
        assert expected[1] == p.before[1] and p.gateway.calls == p.provider_calls
        assert await p.host.archives.read_committed(first.envelope().message_id) is None
        if other is not None:
            assert (await MySQLEvaluationManagement(p.tx).get(other)).status == "collecting"
    finally:
        if other is not None:
            async with p.tx.open() as db:
                for table in (
                    evaluation_capacity_reservations,
                    evaluation_checkpoints,
                    evaluation_runs,
                ):
                    await db.execute(delete(table).where(table.c.run_id == str(other.run_id)))
                await db.commit()
