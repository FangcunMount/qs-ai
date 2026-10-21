"""Original governance roots/MySQL proofs; synthetic reviews, no Broker or model claim."""

from datetime import timedelta
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import delete, select, text

from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.evaluation_governance_state import (
    GovernanceStateOperation,
    GovernanceStatePlanner,
    GovernanceStateRoot,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_finalization import reviewed
from tests.integration.test_evaluation_reopening import eligible_semantics as eligible_semantics
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_semantic_completions import judge as judge
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "client_reply_discarded", "source_changed"]
)
async def test_original_governance_business_history_sequence_archive_and_witness_are_atomic(
    host, eligible_semantics, reviewable, trigger_admin, monkeypatch, kind, fault
):
    tx, scope, _, review = reviewable
    # The existing fixture creates accepted synthetic evidence and all 70 reviews.
    version = await reviewed(reviewable)
    at = review.reviewed_at + timedelta(seconds=1)
    management = MySQLEvaluationManagement(tx)
    if kind == "reopen":
        previous = await management.finalize(scope, version, False, "原拒绝证据", at, confirm=True)
        version, at = previous.version, at + timedelta(seconds=1)
    before = await rows(tx, scope.run_id)
    evidence = await outputs(tx, scope.run_id)
    operation = GovernanceStateOperation(
        kind,
        scope,
        uuid4(),
        version,
        "原治理事务冻结验收",
        True,
        False if kind == "finalize" else None,
    )
    engine = host.tx.database.engine
    assert engine is not None
    # The host default is deliberately RC; planning and execution must each
    # establish their own RR snapshot before their first authoritative read.
    host.tx.database.engine = engine.execution_options(isolation_level="READ COMMITTED")
    trigger = None
    try:
        plan = await GovernanceStatePlanner(
            host.tx,
            signing_key=host.keys["ai.sign"],
            recipient_key=host.keys["qs.encrypt"],
        ).candidate(operation, at)
        reserved = await host.store.reserve(operation.intent.request, plan)
        assert reserved.acquired
        owned = reserved.execution
        host.executions.append(owned)
        _, first = read_evaluation_state_plan(operation.intent, owned.plan)
        if fault == "archive_rejected":
            trigger = "governance_fault_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                        "SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated governance archive rejection'"
                    )
                )
        elif fault == "source_changed":
            if kind == "finalize":
                await management.finalize(scope, version, False, "并发原治理提交", at, confirm=True)
            else:
                await management.reopen(scope, version, "并发原治理提交", at, confirm=True)
        root = GovernanceStateRoot(host.tx, host.archives, host.store, operation, owned.plan)
        with monkeypatch.context() as scoped_patch:
            if fault == "client_reply_discarded":
                original_commit = EventSession.commit

                async def discard(db):
                    await original_commit(db)
                    raise ConnectionError("client discarded completed governance root reply")

                scoped_patch.setattr(EventSession, "commit", discard)
            outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await rows(tx, scope.run_id)
        if committed or fault == "source_changed":
            assert current[2]["version"] == version + 1
            assert current[0]["progress_json"]["status"] == (
                "rejected" if kind == "finalize" else "awaiting_review"
            )
            assert (
                current[0]["progress_json"]["transitions"][:-1]
                == before[0]["progress_json"]["transitions"]
            )
        else:
            assert current == before
        assert await outputs(tx, scope.run_id) == evidence
        assert await host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
        async with host.tx.open() as db:
            sequence = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(scope.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            assert (None if sequence is None else (sequence["sequence"], sequence["version"])) == (
                (1, version + 1) if committed else None
            )
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(
                        outbox.c.message_id == first.envelope().message_id
                    )
                )
            ).all()
            assert await db.scalar(text("SELECT @@transaction_isolation")) == "READ-COMMITTED"
    finally:
        # Restore the original engine reference without disposing the shared pool.
        host.tx.database.engine = engine
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(
                    evaluation_sequences.c.run_id == str(scope.run_id)
                )
            )


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
@pytest.mark.parametrize("reject_before_local", [False, True])
async def test_rpc_handler_and_original_governance_root_require_preparation_before_business(
    host, eligible_semantics, reviewable, kind, reject_before_local
):
    from contextlib import asynccontextmanager

    from reliable_messaging.rocketmq_transaction import TransactionSendError
    from reliable_messaging.transaction import Result

    from qs_ai.application.evaluation.management import EvaluationManagementStore
    from qs_ai.contracts.workflow import workflow_pb2 as workflow
    from qs_ai.infrastructure.workflow_transport.evaluation_governance_dispatch import (
        GovernanceStateDispatcher,
    )
    from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
        TransactionalEvaluationManagement,
    )
    from qs_ai.transport.grpc.evaluation import EvaluationManagement
    from tests.test_grpc_commands import Aborted, Context

    tx, scope, _, review = reviewable
    version = await reviewed(reviewable)
    original = MySQLEvaluationManagement(tx)
    if kind == "reopen":
        previous = await original.finalize(
            scope,
            version,
            False,
            "原拒绝证据",
            review.reviewed_at + timedelta(seconds=1),
            confirm=True,
        )
        version = previous.version
    before = await rows(tx, scope.run_id)

    # This exercises the actual handler and database roots, deliberately without
    # SDK halves/Broker. The separate published-SDK profile owns that proof.
    class DatabaseOnlyRuntime:
        store = host.store
        local_calls = 0

        async def reserve(self, request, candidate):
            value = await self.store.reserve(request, candidate)
            if value.acquired:
                host.executions.append(value.execution)
            return value

        async def execute(self, execution, local):
            if reject_before_local:
                raise TransactionSendError(
                    "isolated_preparation_refusal", Result(LocalOutcome.NOT_STARTED, ())
                )
            self.local_calls += 1
            return Result(await local(execution), ())

    runtime = DatabaseOnlyRuntime()
    send = GovernanceStateDispatcher(
        host.tx,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    management = TransactionalEvaluationManagement(MySQLEvaluationManagement(host.tx), send)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return management

        yield Scope()

    service = EvaluationManagement(container)
    request = (
        workflow.EvaluationFinalizeCommand
        if kind == "finalize"
        else workflow.EvaluationReopenCommand
    )(
        scope=workflow.EvaluationQuery(
            run_id=str(scope.run_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
        ),
        expected_version=version,
        reason="原RPC治理事务验收",
        confirm=True,
    )
    if kind == "finalize":
        request.expected_passed = False
    method = service.Finalize if kind == "finalize" else service.ReopenReview
    try:
        if reject_before_local:
            import grpc

            with pytest.raises(Aborted) as caught:
                await method(request, Context())
            assert caught.value.args[0] == grpc.StatusCode.UNAVAILABLE
            assert runtime.local_calls == 0 and await rows(tx, scope.run_id) == before
        else:
            reply = await method(request, Context())
            assert reply.version == version + 1 and runtime.local_calls == 1
            assert reply.status == ("rejected" if kind == "finalize" else "awaiting_review")
            stored = await original.get(scope)
            assert reply.finalization_json == stored.finalization_json
            assert reply.reopenings_json == stored.reopenings_json
        owned = host.executions[-1]
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.UNKNOWN if reject_before_local else Decision.COMMIT
        )
        from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
            EvaluationStateFacts,
        )
        from qs_ai.infrastructure.workflow_transport.transaction_archive import decode_branch

        facts = EvaluationStateFacts.decode(decode_branch(owned.plan.branches[0]).context)
        operation, _ = GovernanceStateOperation.from_facts(facts)
        _, first = read_evaluation_state_plan(operation.intent, owned.plan)
        assert await host.archives.read_committed(first.envelope().message_id) == (
            None if reject_before_local else first
        )
    finally:
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(
                    evaluation_sequences.c.run_id == str(scope.run_id)
                )
            )
