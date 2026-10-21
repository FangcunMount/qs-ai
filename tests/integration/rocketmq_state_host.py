"""Explicit infra profile: real worker, released SDK/Broker and original state roots.

The event observer is a fixture, not QS business, a real provider, system login
or packet-level/process-exit fault acceptance. Ordinary collection excludes this file.
"""

import asyncio
import hashlib
import json
import sys
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.rocketmq import Config, RocketMQSubscriber
from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
    TransactionSendError,
)
from reliable_messaging.transaction import (
    Decision,
    FinalOutcome,
    Limits,
    LocalOutcome,
    StoreChecker,
    execution_key,
)
from reliable_messaging.transport import RouteBinding, consumer_group_v2, physical_topic_v2
from sqlalchemy import delete, event, select, text

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationManagementStore
from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import witness
from qs_ai.infrastructure.workflow_transport.correction_state import read_correction_plan
from qs_ai.infrastructure.workflow_transport.correction_state_plan import CorrectionStateOperation
from qs_ai.infrastructure.workflow_transport.evaluation_governance_dispatch import (
    GovernanceStateDispatcher,
    GovernanceTransactionPending,
)
from qs_ai.infrastructure.workflow_transport.evaluation_governance_state import (
    GovernanceStateOperation,
)
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_dispatch import (
    PreflightStateDispatcher,
    PreflightTransactionPending,
)
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
    PreflightOperation,
    preflight_intent,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS, authenticate, parse_body
from qs_ai.infrastructure.workflow_transport.resolution_state import read_resolution_plan
from qs_ai.infrastructure.workflow_transport.resolution_state_plan import ResolutionStateOperation
from qs_ai.infrastructure.workflow_transport.review_state_plan import (
    ReviewStateInput,
    read_review_state_plan,
)
from qs_ai.infrastructure.workflow_transport.solution_preparation_dispatch import (
    SolutionPreparationDispatcher,
)
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationInput,
    read_preparation_plan,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
    TransactionalEvaluationManagement,
)
from qs_ai.infrastructure.workflow_transport.transactional_solutions import TransactionalSolutions
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from qs_ai.transport.grpc.solutions import response as solution_response
from tests.integration.rocketmq_admission_host import required_broker as required_broker
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_finalization import passing_semantics as passing_semantics
from tests.integration.test_evaluation_finalization import reviewed
from tests.integration.test_evaluation_reopening import eligible_semantics as eligible_semantics
from tests.integration.test_evaluation_resolution import decision as resolution_decision
from tests.integration.test_evaluation_review_corrections import intent as correction_intent
from tests.integration.test_evaluation_review_corrections import signatures as signatures
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows as evaluation_rows
from tests.integration.test_evaluation_step import ready as resolution_readiness
from tests.integration.test_semantic_completions import judge as judge
from tests.integration.test_solutions import workspace as workspace
from tests.integration.test_transaction_evaluation_admission import (
    host as host,
)
from tests.integration.test_transaction_evaluation_admission import (
    keys as keys,
)
from tests.integration.test_transaction_evaluation_admission import (
    kit as kit,
)
from tests.integration.test_transaction_evaluation_admission import (
    original as original,
)
from tests.integration.test_transaction_evaluation_admission import (
    requested as requested,
)
from tests.integration.test_transaction_evaluation_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_evaluation_admission import (
    setup_run as setup_run,
)
from tests.integration.test_transaction_resolution_state import unknown as resolution_seed
from tests.integration.test_transaction_solution_dispatch import service_request
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_grpc_commands import Context

pytestmark = pytest.mark.integration
_CASES = {}
_OBSERVER = "qs-m3-ai-state-observer"


def passed(name):
    _CASES[name] = True


@pytest.fixture(scope="session", autouse=True)
def state_receipt(required_broker, request):
    # Reuse the fixed client/CA/private log guards. This profile has its own
    # required receipt; the older module's empty receipt is never acceptance.
    yield
    print(
        "M3_AI_STATE_RECEIPT "
        + json.dumps(
            {
                "cases": _CASES,
                "passed": len(_CASES),
                "failed": request.session.testsfailed,
                "skipped": len(
                    request.config.pluginmanager.get_plugin("terminalreporter").stats.get(
                        "skipped", []
                    )
                ),
                "root": "original_mysql_sqlalchemy",
                "sdk": "0.4.0a1",
                "client": "5.1.2",
                "python": sys.version.split()[0],
                "system_login_not_claimed": True,
                "paired_qs_business_not_claimed": True,
                "real_provider_not_claimed": True,
                "process_exit_not_claimed": True,
                "database_commit_network_fault_not_claimed": True,
            }
        ),
        flush=True,
    )


class WorkerRuntime(TransactionRuntime):
    def __init__(self, sender, host):
        super().__init__(sender, admitted=True)
        self.host, self.results, self.calls, self.pause = host, [], 0, False
        self.entered, self.release, self.completed = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )

    async def reserve(self, request, candidate):
        value = await super().reserve(request, candidate)
        if value.acquired:
            self.host.executions.append(value.execution)
        return value

    async def execute(self, owned, local):
        async def original(exact):
            key = execution_key(
                exact.plan.owner, exact.plan.operation_id, exact.writer_epoch, exact.fence
            )
            async with self.host.tx.open() as db:
                halves = (
                    await db.scalars(
                        select(witness.c.half_outcome).where(witness.c.execution_key == key)
                    )
                ).all()
            assert len(halves) == len(exact.references) and all(
                bytes(s) == b"prepared" for s in halves
            )
            passed("halves_before_worker_root")
            self.entered.set()
            if self.pause:
                await self.release.wait()
            self.calls += 1
            return await local(exact)

        try:
            result = await super().execute(owned, original)
        except TransactionSendError as error:
            self.results.append(error.result)
            self.completed.set()
            raise
        self.results.append(result)
        self.completed.set()
        return result


@pytest.fixture
async def broker(original, required_broker):
    host, scope, management = original
    await management.start(
        scope, 1, "explicit isolated worker state seed", datetime.now(UTC), confirm=True
    )
    config = required_broker
    physical = physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
    credentials, observer_credentials = config["ai"], config["observer"]
    messages, failures, statements = {}, [], []
    empty_poll = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous = host.tx.database.state_events
    recorder = SimpleNamespace(
        flush=AsyncMock(
            side_effect=AssertionError("New worker state cannot use legacy commit hook")
        )
    )

    def count_sql(*_):
        statements.append(True)

    event.listen(host.tx.database.engine.sync_engine, "before_cursor_execute", count_sql)
    try:
        sender = TransactionalSender(
            TransactionConfig(
                config["endpoint"],
                credentials["username"],
                credentials["password"],
                "ai.mysql",
                (TransactionRoute(EVENTS, physical, "m3-1"),),
                Limits(sends=4, check_workers=1, check_queue=16),
            ),
            host.store,
        )
        runtime = WorkerRuntime(sender, host)
        dispatcher = PreflightStateDispatcher(
            host.tx,
            host.archives,
            runtime,
            signing_key=host.keys["ai.sign"],
            recipient_key=host.keys["qs.encrypt"],
        )
        gateway = SimpleNamespace(
            generate_messages=AsyncMock(side_effect=AssertionError("Preflight cannot call a model"))
        )
        worker = EvaluationWorker(
            host.tx,
            gateway,
            Mock(),
            Mock(),
            "worker:real-sdk-state",
            enabled=True,
            preflight_dispatch=dispatcher,
        )
        assert not statements and sender._client is None
        passed("constructor_inert")
    finally:
        event.remove(host.tx.database.engine.sync_engine, "before_cursor_execute", count_sql)

    async def observe(delivery):
        header = authenticate(
            delivery.wire,
            EVENTS,
            decrypt_keys={"qs.encrypt": host.keys["qs.encrypt"]},
            trusted_signers={"ai.sign": TrustedSigner("qs-ai", host.keys["ai.sign"])},
        )
        stored = await host.archives.read_committed(header.message_id)
        assert stored is not None and stored.wire == delivery.wire
        parse_body(header, stored.body)
        digest = hashlib.sha256(delivery.wire).hexdigest()
        assert header.message_id not in messages or messages[header.message_id] == digest
        messages[header.message_id] = digest

    async def failed(_):
        failures.append("unexpected_physical_failure")
        raise RuntimeError("Unexpected isolated observer failure")

    async def invalid(_, code):
        failures.append("unexpected_invalid_wire")
        raise RuntimeError("Unexpected isolated observer wire")

    observer = RocketMQSubscriber(
        Config(
            config["endpoint"],
            observer_credentials["username"],
            observer_credentials["password"],
            (
                RouteBinding(
                    EVENTS,
                    _OBSERVER,
                    physical,
                    consumer_group_v2(EVENTS, _OBSERVER),
                    "TRANSACTION",
                    "ai.mysql",
                    "m3-1",
                ),
            ),
            max_in_flight=1,
        ),
        EVENTS,
        _OBSERVER,
        handler=observe,
        failed_handler=failed,
        invalid_handler=invalid,
        retry_cap=1,
    )
    intent = preflight_intent(scope.run_id, 2, scope.organization_id)

    async def complete():
        async with asyncio.timeout(60):
            while True:
                assert not failures
                reserved = await host.store.lookup(intent.request)
                if reserved is not None and reserved.state.value == "committed":
                    facts, value = read_evaluation_state_plan(intent, reserved.execution.plan)
                    if value.envelope().message_id in messages:
                        assert (
                            await StoreChecker(host.store).check(reserved.execution.references[0])
                            == Decision.COMMIT
                        )
                        operation = PreflightOperation.from_facts(facts)
                        async with host.tx.open() as db:
                            row = (
                                (
                                    await db.execute(
                                        select(evaluation_runs).where(
                                            evaluation_runs.c.run_id == intent.run_id
                                        )
                                    )
                                )
                                .mappings()
                                .one()
                            )
                            assert (
                                row["progress_json"]["preflight"]["evaluated_at"]
                                == operation.evidence.evaluated_at.isoformat()
                            )
                            assert not (
                                await db.scalars(
                                    select(outbox.c.message_id).where(
                                        outbox.c.message_id == value.envelope().message_id
                                    )
                                )
                            ).all()
                        for label in (
                            "worker_original_root",
                            "first_preflight_evidence",
                            "no_new_publish_outbox",
                            "fresh_checker",
                            "first_wire",
                        ):
                            passed(label)
                        gateway.generate_messages.assert_not_called()
                        recorder.flush.assert_not_called()
                        passed("no_model_call")
                        return reserved
                await asyncio.sleep(0.05)

    resources = AsyncExitStack()
    resources.push_async_callback(observer.stop, grace_seconds=20)
    resources.push_async_callback(runtime.close, grace_seconds=20)
    try:
        host.tx.database.state_events = recorder
        await observer.start()
        actual_receive = observer._driver.client.receive

        def receive(*args, **kwargs):
            values = actual_receive(*args, **kwargs)
            if not values:
                loop.call_soon_threadsafe(empty_poll.set)
            return values

        observer._driver.client.receive = receive
        await runtime.start()
        yield SimpleNamespace(**locals())
    finally:
        runtime.release.set()
        try:
            await resources.aclose()
            async with host.tx.open() as db:
                assert await db.scalar(text("SELECT 1")) == 1
            passed("host_pool_retained")
        finally:
            host.tx.database.state_events = previous


async def test_actual_worker_preflight_half_gate_and_duplicate_preserve_original_evidence(broker):
    broker.runtime.pause = True
    work = asyncio.create_task(broker.worker.once())
    try:
        await asyncio.wait_for(broker.runtime.entered.wait(), timeout=30)
        broker.empty_poll.clear()
        await asyncio.wait_for(broker.empty_poll.wait(), timeout=20)
        assert not broker.messages
        async with broker.host.tx.open() as db:
            assert (
                await db.scalar(
                    select(evaluation_checkpoints.c.version).where(
                        evaluation_checkpoints.c.run_id == broker.intent.run_id
                    )
                )
                == 2
            )
        passed("half_invisible")
        broker.runtime.release.set()
        assert await work
    finally:
        broker.runtime.release.set()
        await asyncio.gather(work, return_exceptions=True)
    await broker.complete()
    broker.dispatcher._planner.candidate = AsyncMock(
        side_effect=AssertionError("Committed hint cannot plan again")
    )
    await broker.dispatcher(broker.scope.run_id, 2, broker.scope.organization_id)
    assert broker.runtime.calls == 1
    broker.dispatcher._planner.candidate.assert_not_called()
    passed("duplicate_no_replan")


async def test_actual_worker_preflight_lost_final_reply_preserves_local_commit(broker, monkeypatch):
    actual_end = broker.sender._client.end_transaction

    def discard(*args, **kwargs):
        actual_end(*args, **kwargs)
        raise TimeoutError("client discarded completed final state reply")

    monkeypatch.setattr(broker.sender._client, "end_transaction", discard)
    assert await broker.worker.once()
    await broker.complete()
    result = broker.runtime.results[-1]
    assert result.local == LocalOutcome.COMMITTED and all(
        b.final == FinalOutcome.UNKNOWN for b in result.branches
    )
    assert broker.runtime.calls == 1
    passed("lost_final_reply")


async def test_actual_worker_preflight_source_conflict_rolls_back_prepared_half(broker):
    broker.runtime.pause = True
    work = asyncio.create_task(broker.worker.once())
    try:
        await asyncio.wait_for(broker.runtime.entered.wait(), timeout=30)
        await broker.management.cancel(
            broker.scope,
            2,
            "explicit isolated competing cancellation",
            datetime.now(UTC),
            discard=False,
            confirm=True,
        )
        broker.runtime.release.set()
        with pytest.raises(PreflightTransactionPending):
            await work
        result = broker.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(b.final == FinalOutcome.ROLLBACK_CONFIRMED for b in result.branches)
    finally:
        broker.runtime.release.set()
        await asyncio.gather(work, return_exceptions=True)
    reserved = await broker.host.store.lookup(broker.intent.request)
    assert reserved is not None
    assert (
        await StoreChecker(broker.host.store).check(reserved.execution.references[0])
        == Decision.ROLLBACK
    )
    broker.empty_poll.clear()
    await asyncio.wait_for(broker.empty_poll.wait(), timeout=20)
    assert not broker.messages
    broker.gateway.generate_messages.assert_not_called()
    broker.recorder.flush.assert_not_called()
    passed("source_conflict_rollback")


@pytest.fixture
async def governance_broker(host, required_broker):
    """Actual transport with no pre-seeded Run; review fixtures own business evidence."""
    config = required_broker
    physical = physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
    credentials, observer_credentials = config["ai"], config["observer"]
    messages, failures = {}, []
    empty_poll = asyncio.Event()
    loop = asyncio.get_running_loop()
    sender = TransactionalSender(
        TransactionConfig(
            config["endpoint"],
            credentials["username"],
            credentials["password"],
            "ai.mysql",
            (TransactionRoute(EVENTS, physical, "m3-1"),),
            Limits(sends=4, check_workers=1, check_queue=16),
        ),
        host.store,
    )
    runtime = WorkerRuntime(sender, host)
    dispatcher = GovernanceStateDispatcher(
        host.tx,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    previous = host.tx.database.state_events
    recorder = SimpleNamespace(
        flush=AsyncMock(
            side_effect=AssertionError("Governance must not publish through old commit hook")
        )
    )

    async def observe(delivery):
        header = authenticate(
            delivery.wire,
            EVENTS,
            decrypt_keys={"qs.encrypt": host.keys["qs.encrypt"]},
            trusted_signers={"ai.sign": TrustedSigner("qs-ai", host.keys["ai.sign"])},
        )
        value = await host.archives.read_committed(header.message_id)
        assert value is not None and value.wire == delivery.wire
        parse_body(header, value.body)
        digest = hashlib.sha256(delivery.wire).hexdigest()
        assert header.message_id not in messages or messages[header.message_id] == digest
        messages[header.message_id] = digest

    async def failed(_):
        failures.append("unexpected_physical_failure")
        raise RuntimeError("Unexpected governance observer failure")

    async def invalid(_, code):
        failures.append("unexpected_invalid_wire")
        raise RuntimeError("Unexpected governance observer wire")

    observer = RocketMQSubscriber(
        Config(
            config["endpoint"],
            observer_credentials["username"],
            observer_credentials["password"],
            (
                RouteBinding(
                    EVENTS,
                    _OBSERVER,
                    physical,
                    consumer_group_v2(EVENTS, _OBSERVER),
                    "TRANSACTION",
                    "ai.mysql",
                    "m3-1",
                ),
            ),
            max_in_flight=1,
        ),
        EVENTS,
        _OBSERVER,
        handler=observe,
        failed_handler=failed,
        invalid_handler=invalid,
        retry_cap=1,
    )

    async def consumed(operation):
        reserved = await host.store.lookup(operation.intent.request)
        assert reserved is not None
        _, value = read_evaluation_state_plan(operation.intent, reserved.execution.plan)
        message_id = value.envelope().message_id
        async with asyncio.timeout(60):
            while message_id not in messages:
                assert not failures
                await asyncio.sleep(0.05)
        assert messages[message_id] == hashlib.sha256(value.wire).hexdigest()
        assert (
            await StoreChecker(host.store).check(reserved.execution.references[0])
            == Decision.COMMIT
        )
        assert await host.archives.read_committed(message_id) == value
        async with host.tx.open() as db:
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(outbox.c.message_id == message_id)
                )
            ).all()
        recorder.flush.assert_not_called()
        return reserved

    resources = AsyncExitStack()
    resources.push_async_callback(observer.stop, grace_seconds=20)
    resources.push_async_callback(runtime.close, grace_seconds=20)
    try:
        host.tx.database.state_events = recorder
        await observer.start()
        actual_receive = observer._driver.client.receive

        def receive(*args, **kwargs):
            values = actual_receive(*args, **kwargs)
            if not values:
                loop.call_soon_threadsafe(empty_poll.set)
            return values

        observer._driver.client.receive = receive
        await runtime.start()
        yield SimpleNamespace(**locals())
    finally:
        runtime.release.set()
        try:
            await resources.aclose()
            async with host.tx.open() as db:
                assert await db.scalar(text("SELECT 1")) == 1
        finally:
            host.tx.database.state_events = previous


async def clean_governance_sequence(host, run_id):
    async with host.tx.open() as db, db.begin():
        await db.execute(
            delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
        )


async def test_actual_governance_finalize_reopen_halves_and_lost_final_reply(
    governance_broker, eligible_semantics, reviewable, monkeypatch
):
    b = governance_broker
    tx, scope, _, review = reviewable
    version = await reviewed(reviewable)
    try:
        for kind in ("finalize", "reopen"):
            operation = GovernanceStateOperation(
                kind,
                scope,
                uuid4(),
                version,
                "真实SDK治理根验收",
                True,
                False if kind == "finalize" else None,
            )
            b.runtime.pause = True
            b.runtime.entered.clear()
            b.runtime.release.clear()
            task = asyncio.create_task(b.dispatcher(operation, datetime.now(UTC)))
            try:
                await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
                owned = b.host.executions[-1]
                _, first = read_evaluation_state_plan(operation.intent, owned.plan)
                b.empty_poll.clear()
                await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
                assert first.envelope().message_id not in b.messages
                assert (await evaluation_rows(tx, scope.run_id))[2]["version"] == version
                passed("governance_half_invisible")
                with monkeypatch.context() as patch:
                    if kind == "reopen":
                        actual_end = b.sender._client.end_transaction

                        def discard(*args, _actual_end=actual_end, **kwargs):
                            _actual_end(*args, **kwargs)
                            raise TimeoutError("client discarded completed governance final reply")

                        patch.setattr(b.sender._client, "end_transaction", discard)
                    b.runtime.release.set()
                    response = await task
                assert response.version == version + 1
                assert response.status == ("rejected" if kind == "finalize" else "awaiting_review")
                await b.consumed(operation)
                if kind == "reopen":
                    result = b.runtime.results[-1]
                    assert result.local == LocalOutcome.COMMITTED
                    assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
                    passed("governance_final_unknown_keeps_commit")
                version += 1
            finally:
                b.runtime.release.set()
                await asyncio.gather(task, return_exceptions=True)
        assert b.runtime.calls == 2
        passed("governance_finalize_reopen_original_roots")
    finally:
        await clean_governance_sequence(b.host, scope.run_id)


async def test_actual_review_full35_rpc_half_gate_first_clocks_and_wire(
    governance_broker, reviewable
):
    b = governance_broker
    tx, scope, version, value = reviewable
    operation_id = uuid4()
    store = TransactionalEvaluationManagement(
        MySQLEvaluationManagement(b.host.tx), b.dispatcher, new_id=lambda: operation_id
    )

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return store

        yield Scope()

    request = workflow.EvaluationReviewCommand(
        scope=workflow.EvaluationQuery(
            run_id=str(scope.run_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
        ),
        expected_version=version,
        role="assessment_semantics",
        reviews=[
            workflow.CandidateReviewItem(
                candidate_id=f"candidate:{i}", decision="reject", reason="核" * 333
            )
            for i in range(1, 36)
        ],
    )
    assert request.ByteSize() > 8192
    incoming = ReviewStateInput(
        scope,
        operation_id,
        version,
        tuple(
            replace(value, candidate_id=f"candidate:{i}", decision="reject", reason="核" * 333)
            for i in range(1, 36)
        ),
    )
    b.runtime.pause = True
    task = asyncio.create_task(EvaluationManagement(container).Review(request, Context()))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, first_facts, first = read_review_state_plan(incoming, owned.plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        assert (await evaluation_rows(tx, scope.run_id))[2]["version"] == version
        passed("review_half_invisible")
        b.runtime.release.set()
        response = await task
        assert response.version == version + 1 and response.status == "awaiting_review"
        reserved = await b.consumed(incoming)
        current = await evaluation_rows(tx, scope.run_id)
        actual = current[0]["progress_json"]["human_reviews"]
        assert len(actual) == 35 and len(json.loads(response.reviews_json)) == 35
        assert [r["reviewed_at"] for r in actual] == [
            v.isoformat() for v in first_facts.reviewed_at
        ]
        assert (
            await b.host.archives.read_committed(first.envelope().message_id)
        ).wire == first.wire
        assert reserved.execution.plan.digest == owned.plan.digest and b.runtime.calls == 1
        passed("review_full35_original_root")
        passed("review_first_clocks_and_wire")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await clean_governance_sequence(b.host, scope.run_id)


async def test_actual_review_source_conflict_rolls_back_prepared_half(
    governance_broker, reviewable
):
    b = governance_broker
    tx, scope, version, value = reviewable
    incoming = ReviewStateInput(scope, uuid4(), version, (value,))
    b.runtime.pause = True
    task = asyncio.create_task(b.dispatcher(incoming, value.reviewed_at))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, _, first = read_review_state_plan(incoming, owned.plan)
        async with tx.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            await accept_reviews(db, scope, version, incoming.values)
            await db.commit()
        competing = await evaluation_rows(tx, scope.run_id)
        b.runtime.release.set()
        with pytest.raises(CheckpointConflict):
            await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(branch.final == FinalOutcome.ROLLBACK_CONFIRMED for branch in result.branches)
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await evaluation_rows(tx, scope.run_id) == competing
        assert await b.host.archives.read_committed(first.envelope().message_id) is None
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("review_source_conflict_rollback")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await clean_governance_sequence(b.host, scope.run_id)


async def test_actual_correction_first_wire_final_unknown_and_replay_after_finalization(
    governance_broker, signatures, monkeypatch
):
    b = governance_broker
    tx, scope, version, _, _, _ = signatures
    operation = CorrectionStateOperation(**correction_intent(signatures))
    b.runtime.pause = True
    task = asyncio.create_task(b.dispatcher(operation, operation.value.reviewed_at))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        facts, first = read_correction_plan(operation, owned.plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        assert (await evaluation_rows(tx, scope.run_id))[2]["version"] == version
        passed("correction_half_invisible")
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed correction final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            response = await task
        assert response.version == version + 1
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        passed("correction_final_unknown_keeps_commit")
        await b.consumed(operation)
        frozen, _ = CorrectionStateOperation.from_facts(facts)
        current = await evaluation_rows(tx, scope.run_id)
        record = current[0]["progress_json"]["review_corrections"][0]
        assert record["review"]["reviewed_at"] == frozen.value.reviewed_at.isoformat()
        assert record["command_id"] == operation.command_id
        assert await b.host.archives.read_committed(first.envelope().message_id) == first
        passed("correction_first_clock_wire_and_witness")
        b.runtime.pause = False
        final = GovernanceStateOperation(
            "finalize", scope, uuid4(), response.version, "真实SDK更正后定稿", True, True
        )
        finalized = await b.dispatcher(final, operation.value.reviewed_at + timedelta(seconds=2))
        assert finalized.status == "approved"
        await b.consumed(final)
        later = replace(
            operation,
            value=replace(
                operation.value, reviewed_at=operation.value.reviewed_at + timedelta(seconds=5)
            ),
        )
        assert await b.dispatcher(later, later.value.reviewed_at) == finalized
        assert b.runtime.calls == 2 and len(b.host.executions) == 2
        previous = await b.host.store.lookup(operation.intent.request)
        assert previous.execution.plan.digest == owned.plan.digest
        assert await b.host.archives.read_committed(first.envelope().message_id) == first
        passed("correction_replay_after_finalization_no_new_send")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await clean_governance_sequence(b.host, scope.run_id)


async def test_actual_correction_source_conflict_rolls_back_prepared_half(
    governance_broker, signatures
):
    b = governance_broker
    tx, scope, _, _, _, original = signatures
    args = correction_intent(signatures)
    operation = CorrectionStateOperation(**args)
    b.runtime.pause = True
    task = asyncio.create_task(b.dispatcher(operation, operation.value.reviewed_at))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, first = read_correction_plan(operation, owned.plan)
        await original.correct_review(**{**args, "command_id": str(uuid4())})
        competing = await evaluation_rows(tx, scope.run_id)
        b.runtime.release.set()
        with pytest.raises(CheckpointConflict):
            await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(branch.final == FinalOutcome.ROLLBACK_CONFIRMED for branch in result.branches)
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await evaluation_rows(tx, scope.run_id) == competing
        assert await b.host.archives.read_committed(first.envelope().message_id) is None
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("correction_source_conflict_rollback")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await clean_governance_sequence(b.host, scope.run_id)


async def test_actual_solution_prepare_rpc_half_gate_first_receipt_and_final_unknown(
    governance_broker, workspace, monkeypatch
):
    b = governance_broker
    _, previous, scope, solution_id, create, at = workspace
    # The old fixture writes through its own session before the new SDK owner;
    # the new owner's commit recorder stays poisoned for the complete test.
    initial = await previous.apply(scope, solution_id, create, at)
    settings = previous.settings.model_copy(
        update={
            "evaluation": previous.settings.evaluation.model_copy(
                update={"candidate_mode_enabled": True}
            )
        }
    )
    original = MySQLSolutions(b.host.tx, settings)
    sender = TransactionalSolutions(
        original,
        SolutionPreparationDispatcher(
            original,
            b.host.archives,
            b.runtime,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
        ),
    )
    command = PrepareSolution(
        command_id=uuid4(), expected_revision=initial["revision"], reason="真实SDK方案准备"
    )
    incoming = SolutionPreparationInput(scope, solution_id, command)
    service, request = service_request(SimpleNamespace(**locals()))
    b.runtime.pause = True
    task = asyncio.create_task(service.Prepare(request, Context()))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, facts, first = read_preparation_plan(incoming, owned.plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        assert await original.get(scope, solution_id) == initial
        assert await evaluation_rows(b.host.tx, incoming.identities.run_id) == [None, None, None]
        passed("solution_half_invisible")
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed Solution final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            response = await task
        value = json.loads(response.data_json)
        assert value["revision"] == initial["revision"] + 1
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        passed("solution_final_unknown_keeps_commit")
        await b.consumed(incoming)
        actual = await evaluation_rows(b.host.tx, incoming.identities.run_id)
        assert actual[0]["execution_mode"] == facts.execution_mode == "candidate_v2"
        assert solution_response(await original.receipt(scope, command.command_id)) == response
        assert await b.host.archives.read_committed(first.envelope().message_id) == first
        passed("solution_original_assets_receipt_wire_and_witness")
        original.settings = previous.settings
        replay = await sender.apply(scope, solution_id, command, at + timedelta(hours=1))
        assert solution_response(replay) == response
        assert await evaluation_rows(b.host.tx, incoming.identities.run_id) == actual
        assert b.runtime.calls == len(b.host.executions) == 1
        b.recorder.flush.assert_not_called()
        passed("solution_first_receipt_replay_no_new_send")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await clean_governance_sequence(b.host, incoming.identities.run_id)


async def test_actual_solution_source_conflict_rolls_back_prepared_half(
    governance_broker, workspace
):
    b = governance_broker
    _, previous, scope, solution_id, create, at = workspace
    initial = await previous.apply(scope, solution_id, create, at)
    original = MySQLSolutions(b.host.tx, previous.settings)
    sender = TransactionalSolutions(
        original,
        SolutionPreparationDispatcher(
            original,
            b.host.archives,
            b.runtime,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
        ),
    )
    command = PrepareSolution(
        command_id=uuid4(), expected_revision=initial["revision"], reason="真实SDK冲突方案"
    )
    incoming = SolutionPreparationInput(scope, solution_id, command)
    b.runtime.pause = True
    task = asyncio.create_task(sender.apply(scope, solution_id, command, at))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, _, first = read_preparation_plan(incoming, owned.plan)
        competing = await previous.apply(
            scope, solution_id, replace(command, command_id=uuid4(), reason="并发旧准备"), at
        )
        current = await evaluation_rows(b.host.tx, incoming.identities.run_id)
        b.runtime.release.set()
        from qs_ai.domain.governance.prompt_draft import DraftConflict

        with pytest.raises(DraftConflict):
            await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(branch.final == FinalOutcome.ROLLBACK_CONFIRMED for branch in result.branches)
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await original.get(scope, solution_id) == competing
        assert await evaluation_rows(b.host.tx, incoming.identities.run_id) == current
        assert await b.host.archives.read_committed(first.envelope().message_id) is None
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("solution_source_conflict_rollback")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await clean_governance_sequence(b.host, incoming.identities.run_id)


@pytest.fixture
async def resolution_ready(setup_run, persisted_assets):
    # Do not replace the legacy profile's ready fixture; each original graph
    # retains its own expected fixture shape and complete teardown.
    async with aclosing(resolution_readiness.__wrapped__(setup_run, persisted_assets)) as values:
        async for value in values:
            yield value


@pytest.fixture
async def resolution_unknown(host, resolution_ready):
    async with aclosing(resolution_seed.__wrapped__(host, resolution_ready)) as values:
        async for value in values:
            yield value


async def test_actual_resolution_rpc_half_gate_first_audit_and_final_unknown(
    governance_broker, resolution_unknown, monkeypatch
):
    b, p = governance_broker, resolution_unknown
    operation_id = uuid4()
    dispatcher = GovernanceStateDispatcher(
        b.host.tx,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
        resolution_management=p.original,
    )
    sender = TransactionalEvaluationManagement(p.original, dispatcher, new_id=lambda: operation_id)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return sender

        yield Scope()

    request = workflow.UnknownResolutionCommand(
        scope=workflow.EvaluationQuery(
            run_id=str(p.run_id),
            organization_id=p.scope.organization_id,
            operator_user_id=p.scope.operator_user_id,
        ),
        expected_version=p.state.version,
        execution_id="execution:dead",
        decision="authorize_replacement",
        reason="真实SDK原人工裁决",
        acknowledged_duplicate_call_and_cost_risk=True,
        confirm=True,
    )
    operation = ResolutionStateOperation(
        p.scope,
        operation_id,
        p.state.version,
        replace(resolution_decision(), actor=p.scope.actor, reason=request.reason),
        True,
    )
    b.runtime.pause = True
    task = asyncio.create_task(EvaluationManagement(container).ResolveUnknown(request, Context()))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        facts, first = read_resolution_plan(operation, owned.plan)
        frozen, _ = ResolutionStateOperation.from_facts(facts)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        assert await evaluation_rows(p.tx, p.run_id) == p.before[0]
        passed("resolution_half_invisible")
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed resolution final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            response = await task
        assert response.status == "collecting" and response.version == p.state.version + 1
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        passed("resolution_final_unknown_keeps_commit")
        await b.consumed(operation)
        run = (await evaluation_rows(p.tx, p.run_id))[0]
        audit = run["progress_json"]["result_unknown_resolutions"][-1]
        assert audit["resolved_at"] == frozen.value.resolved_at.isoformat()
        assert audit["actor"] == p.scope.actor and audit["execution_id"] == request.execution_id
        assert await b.host.archives.read_committed(first.envelope().message_id) == first
        assert p.gateway.calls == p.provider_calls
        passed("resolution_original_audit_wire_and_witness")
        before = await evaluation_rows(p.tx, p.run_id)
        with pytest.raises(GovernanceTransactionPending):
            await dispatcher(operation, frozen.value.resolved_at + timedelta(seconds=5))
        assert await evaluation_rows(p.tx, p.run_id) == before
        assert p.gateway.calls == p.provider_calls
        assert b.runtime.calls == len(b.host.executions) == 1
        b.recorder.flush.assert_not_called()
        passed("resolution_repeated_invocation_no_new_send_or_model")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_actual_resolution_source_conflict_rolls_back_prepared_half(
    governance_broker, resolution_unknown
):
    b, p = governance_broker, resolution_unknown
    value = replace(resolution_decision(), actor=p.scope.actor)
    operation = ResolutionStateOperation(p.scope, uuid4(), p.state.version, value, True)
    dispatcher = GovernanceStateDispatcher(
        b.host.tx,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
        resolution_management=p.original,
    )
    b.runtime.pause = True
    task = asyncio.create_task(dispatcher(operation, value.resolved_at))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, first = read_resolution_plan(operation, owned.plan)
        await MySQLEvaluationManagement(p.tx).resolve(
            p.scope, p.state.version, replace(value, decision="cancel_run"), confirm=True
        )
        competing = await evaluation_rows(p.tx, p.run_id)
        b.runtime.release.set()
        with pytest.raises(CheckpointConflict):
            await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(branch.final == FinalOutcome.ROLLBACK_CONFIRMED for branch in result.branches)
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await evaluation_rows(p.tx, p.run_id) == competing
        assert await b.host.archives.read_committed(first.envelope().message_id) is None
        assert p.gateway.calls == p.provider_calls
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("resolution_source_conflict_rollback")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def _actual_serial_dispatch(b, ready, monkeypatch, *, semantic):
    from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
    from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
        WorkerDispatchPending,
        WorkerDispatchSender,
    )
    from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
        WorkerDispatchPlanner,
        inputs_digest,
        read_worker_dispatch_plan,
        worker_dispatch_intent,
    )
    from tests.integration.test_evaluation_step import AT, Gateway, step
    from tests.integration.test_evaluation_unknowns import snapshot

    tx, run_id, _, _, schemas = ready
    gateway = Gateway(ready)
    version = (await step(ready, gateway)).version if semantic else 3
    before, calls = await snapshot(tx, run_id), gateway.calls
    planner = WorkerDispatchPlanner(
        b.host.tx,
        schemas,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
        clock=lambda: AT + timedelta(seconds=1),
    )
    _, expected = await planner.candidate(run_id, version, 1, "worker:real-serial")
    pool = ModelCapacity({expected.route.provider: ProviderCapacity(2, 1)}, 1)
    dispatch = WorkerDispatchSender(
        b.host.tx, b.host.archives, b.runtime, schemas, "worker:real-serial", planner, capacity=pool
    )
    intent = worker_dispatch_intent(run_id, version, 1, "worker:real-serial")
    b.runtime.pause = True
    task = asyncio.create_task(dispatch(run_id, version, 1))
    value = None
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, operation, first = read_worker_dispatch_plan(intent, owned.plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        assert await snapshot(tx, run_id) == before and gateway.calls == calls
        assert pool.try_acquire(expected.route.provider, evaluation=True) is None
        passed(
            "serial_dispatch_half_invisible_semantic"
            if semantic
            else "serial_dispatch_half_invisible_generation"
        )
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed serial dispatch final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            value = await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        passed("serial_dispatch_final_unknown_handoff")
        assert value.cp == operation.checkpoint and value.cp.phase == "prepared"
        assert value.state.checkpoint.phase == "dispatching" and value.state.version == version + 2
        assert operation.model_inputs_digest == inputs_digest(expected)
        assert (value.messages, value.route, value.schema) == (
            expected.messages,
            expected.route,
            expected.schema,
        )
        await b.consumed(SimpleNamespace(intent=intent))
        after = await snapshot(tx, run_id)
        assert after[0][2]["version"] == version + 2
        assert after[1][1:] == before[1][1:]
        assert len(after[1][0]) == len(before[1][0]) + 1
        ledger = next(row for row in after[1][0] if row["execution_id"] == value.execution_id)
        assert ledger["invocation_id"] == value.invocation_id
        assert ledger["checkpoint_json"]["phase"] == "dispatching"
        assert ledger["checkpoint_json"]["claimed_at"] == value.cp.claimed_at.isoformat()
        assert gateway.calls == calls
        passed("serial_dispatch_two_cas_first_inputs_and_witness")
        assert value.capacity_token is not None
        assert pool.try_acquire(expected.route.provider, evaluation=True) is None
        value.capacity_token.release()
        token = pool.try_acquire(expected.route.provider, evaluation=True)
        assert token is not None
        token.release()
        passed("serial_dispatch_original_permit_transfer")
        with pytest.raises(WorkerDispatchPending):
            await dispatch(run_id, version, 1)
        assert await snapshot(tx, run_id) == after and gateway.calls == calls
        assert b.runtime.calls == len(b.host.executions) == 1
        passed("serial_dispatch_repeat_no_new_model_or_send")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        if value is not None and value.capacity_token is not None:
            value.capacity_token.release()
        await clean_governance_sequence(b.host, run_id)


async def test_actual_serial_generation_dispatch_half_gate_first_inputs_and_final_unknown(
    governance_broker, resolution_ready, monkeypatch
):
    await _actual_serial_dispatch(governance_broker, resolution_ready, monkeypatch, semantic=False)


async def test_actual_serial_semantic_dispatch_half_gate_first_inputs_and_final_unknown(
    governance_broker, resolution_ready, monkeypatch
):
    await _actual_serial_dispatch(governance_broker, resolution_ready, monkeypatch, semantic=True)


async def test_actual_serial_dispatch_source_conflict_rolls_back_without_model_call(
    governance_broker, resolution_ready
):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
        WorkerDispatchPending,
        WorkerDispatchSender,
    )
    from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
        WorkerDispatchPlanner,
        read_worker_dispatch_plan,
        worker_dispatch_intent,
    )
    from tests.integration.test_evaluation_step import AT
    from tests.integration.test_evaluation_unknowns import snapshot

    b, ready = governance_broker, resolution_ready
    tx, run_id, _, _, schemas = ready
    planner = WorkerDispatchPlanner(
        b.host.tx,
        schemas,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
        clock=lambda: AT + timedelta(seconds=1),
    )
    dispatch = WorkerDispatchSender(
        b.host.tx, b.host.archives, b.runtime, schemas, "worker:real-serial", planner
    )
    intent = worker_dispatch_intent(run_id, 3, 1, "worker:real-serial")
    b.runtime.pause = True
    task = asyncio.create_task(dispatch(run_id, 3, 1))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, _, first = read_worker_dispatch_plan(intent, owned.plan)
        await MySQLEvaluationManagement(tx).cancel(
            ManagementScope(run_id, 1, 42),
            3,
            "original cancellation wins before serial dispatch",
            AT + timedelta(seconds=2),
            discard=False,
            confirm=True,
        )
        competing = await snapshot(tx, run_id)
        b.runtime.release.set()
        with pytest.raises((WorkerDispatchPending, TransactionSendError)):
            await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(branch.final == FinalOutcome.ROLLBACK_CONFIRMED for branch in result.branches)
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await snapshot(tx, run_id) == competing
        assert await b.host.archives.read_committed(first.envelope().message_id) is None
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("serial_dispatch_source_conflict_rollback")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await clean_governance_sequence(b.host, run_id)
