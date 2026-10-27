"""Actual original review roots and RPC/MySQL; no Broker or network-loss claim."""

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import delete, select, text

from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.review_state import ReviewStatePlanner, ReviewStateRoot
from qs_ai.infrastructure.workflow_transport.review_state_plan import (
    ReviewStateInput,
    read_review_state_plan,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_semantic_completions import judge as judge
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("count", [1, 35])
@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "client_reply_discarded", "source_changed"]
)
async def test_review_business_first_clocks_sequence_archive_and_witness_share_original_root(
    host, reviewable, trigger_admin, monkeypatch, count, fault
):
    tx, scope, version, value = reviewable
    values = tuple(
        replace(value, candidate_id=f"candidate:{i}", reason="核" * 333)
        for i in range(1, count + 1)
    )
    incoming = ReviewStateInput(scope, uuid4(), version, values)
    before, evidence = await rows(tx, scope.run_id), await outputs(tx, scope.run_id)
    engine = host.tx.database.engine
    assert engine is not None
    host.tx.database.engine = engine.execution_options(isolation_level="READ COMMITTED")
    trigger = None
    try:
        candidate = await ReviewStatePlanner(
            host.tx,
            signing_key=host.keys["ai.sign"],
            recipient_key=host.keys["qs.encrypt"],
            route_resolver=legacy_routes(),
        ).candidate(incoming, value.reviewed_at)
        reserved = await host.store.reserve(incoming.intent.request, candidate)
        assert reserved.acquired
        owned = reserved.execution
        host.executions.append(owned)
        _, _, first = read_review_state_plan(incoming, owned.plan, route_resolver=legacy_routes())
        if fault == "archive_rejected":
            trigger = "review_fault_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                        "SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated review archive rejection'"
                    )
                )
        if fault == "source_changed":
            async with tx.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                await accept_reviews(db, scope, version, values)
                await db.commit()
        # A losing preparation clock must never replace the durable first clock.
        later = replace(
            incoming,
            values=tuple(
                replace(v, reviewed_at=v.reviewed_at + timedelta(seconds=10)) for v in values
            ),
        )
        root = ReviewStateRoot(
            host.tx, host.archives, host.store, later, owned.plan, route_resolver=legacy_routes()
        )
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                original_commit = EventSession.commit

                async def discarded(db):
                    await original_commit(db)
                    raise ConnectionError("client discarded completed review commit reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await rows(tx, scope.run_id)
        if committed or fault == "source_changed":
            assert current[2]["version"] == version + 1
            actual = current[0]["progress_json"]["human_reviews"]
            assert len(actual) == count
            assert [r["reviewed_at"] for r in actual] == [v.reviewed_at.isoformat() for v in values]
            assert [r["reason"] for r in actual] == [v.reason for v in values]
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


@pytest.mark.parametrize("count", [1, 35])
@pytest.mark.parametrize("reject_before_local", [False, True])
async def test_actual_review_rpc_requires_preparation_before_original_business(
    host, reviewable, count, reject_before_local
):
    from contextlib import asynccontextmanager

    from reliable_messaging.rocketmq_transaction import TransactionSendError
    from reliable_messaging.transaction import Result

    from qs_ai.application.evaluation.management import EvaluationManagementStore
    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.infrastructure.workflow_transport.evaluation_governance_dispatch import (
        GovernanceStateDispatcher,
    )
    from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
        TransactionalEvaluationManagement,
    )
    from qs_ai.transport.grpc.evaluation import EvaluationManagement
    from tests.test_grpc_commands import Aborted, Context

    tx, scope, version, _ = reviewable
    before = await rows(tx, scope.run_id)

    class DatabaseOnlyRuntime:
        routing = legacy_routes()

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
                    "isolated_half_refused", Result(LocalOutcome.NOT_STARTED, ())
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
    store = TransactionalEvaluationManagement(MySQLEvaluationManagement(host.tx), send)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return store

        yield Scope()

    service = EvaluationManagement(container)
    request = pb.EvaluationReviewCommand(
        scope=pb.EvaluationQuery(
            run_id=str(scope.run_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
        ),
        expected_version=version,
        role="assessment_semantics",
        reviews=[
            pb.CandidateReviewItem(
                candidate_id=f"candidate:{i}", decision="reject", reason="核" * 333
            )
            for i in range(1, count + 1)
        ],
    )
    try:
        if reject_before_local:
            with pytest.raises(Aborted):
                await service.Review(request, Context())
            assert runtime.local_calls == 0 and await rows(tx, scope.run_id) == before
            assert (
                await StoreChecker(host.store).check(host.executions[-1].references[0])
                == Decision.UNKNOWN
            )
        else:
            response = await service.Review(request, Context())
            assert runtime.local_calls == 1 and response.version == version + 1
            current = await rows(tx, scope.run_id)
            assert len(current[0]["progress_json"]["human_reviews"]) == count
            assert (
                await StoreChecker(host.store).check(host.executions[-1].references[0])
                == Decision.COMMIT
            )
    finally:
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(
                    evaluation_sequences.c.run_id == str(scope.run_id)
                )
            )
