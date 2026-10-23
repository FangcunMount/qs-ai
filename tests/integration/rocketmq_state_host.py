"""Explicit infra profile: real worker, released SDK/Broker and original state roots.

The event observer is a fixture, not QS business, a real provider, system login
or packet-level/process-exit fault acceptance. Ordinary collection excludes this file.
"""

import asyncio
import hashlib
import importlib.metadata as metadata
import json
import sys
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from dishka import Provider, Scope, provide
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
from sqlalchemy import delete, event, select, text, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationManagementStore
from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.application.interpretation.ports import EvidenceSource
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.lifecycle import RuntimeState, supervise
from qs_ai.bootstrap.messaging import MessagingRuntime
from qs_ai.bootstrap.providers.evaluation import EvaluationProvider
from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.bootstrap.transaction_services import TransactionServices
from qs_ai.config import Settings
from qs_ai.contracts.workflow import messaging_pb2 as messaging_pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import event_receipts, witness
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
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    authenticate,
    parse_body,
    prepare,
)
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
                "sdk": metadata.version("fangcun-reliable-messaging"),
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

    async def retry(self, authorization):
        value = await super().retry(authorization)
        if value.acquired:
            self.host.executions.append(value.execution)
        return value

    async def replan(self, authorization):
        value = await super().replan(authorization)
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


def default_transport_settings(dsn, binding, config, files, credentials, routes, *, paused):
    return Settings(
        database_url=dsn.replace("mysql://", "mysql+asyncmy://", 1),
        evaluation={"enabled": True},
        generation={"endpoint": "https://model.invalid/responses"},
        model_api_key="isolated-not-used",
        grpc={"access_address": "127.0.0.1:1"},
        messaging={
            "enabled": True,
            "provider": "rocketmq",
            "publish_enabled": not paused,
            "signing_key_file": files["ai.sign"],
            "qs_recipient_key_file": files["qs.encrypt"],
            "decrypt_key_files": {"ai.encrypt": files["ai.encrypt"]},
            "qs_signer_files": {"qs.sign": files["qs.sign"]},
            "rocketmq": {
                "endpoint": config["endpoint"],
                "credentials_file": str(credentials),
                "routes_file": str(routes),
                "ca_file": config["ca_file"],
            },
            "transactions": {
                "ai.mysql": {
                    "store_id": binding.store_id,
                    "writer_epoch": binding.writer_epoch,
                    "route_revision": "m3-1",
                }
            },
        },
    )


@pytest.fixture
def native_limit_audit(required_broker):
    # Bounded private control material is loaded before the host asyncio loop.
    if required_broker.get("native_limit") is not True:
        return None
    import stat
    from pathlib import Path

    from qs_ai.infrastructure.workflow_transport.native_limit import parse_native_limit_evidence

    path = Path(required_broker["native_limit_audit"])
    assert path.is_absolute()
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
    assert info.st_size <= 65536
    return parse_native_limit_evidence(
        path.read_bytes(), bytes.fromhex(required_broker["native_limit_sha256"])
    )


@pytest.mark.parametrize("paused", [False, True])
async def test_actual_default_transport_and_worker_share_sender_root_and_shutdown(
    governance_broker, original, required_broker, tmp_path, monkeypatch, paused, native_limit_audit
):
    """Actual default DI/SDK/subscriptions; QS ACK input is controlled, not paired QS."""
    from qs_ai.infrastructure.workflow_transport.messaging import ACKS
    from tests.integration.test_delivery import certificates
    from tests.integration.test_mq_storage import received

    b = governance_broker
    host, scope, management = original
    config = required_broker
    # This seed has no new transport responsibility. All writes after installing
    # default assembly must use its exact original-root transaction adapters.
    host.tx.database.state_events = None
    try:
        await management.start(
            scope, 1, "isolated default worker seed", datetime.now(UTC), confirm=True
        )
    finally:
        host.tx.database.state_events = b.recorder
    certificates(tmp_path)
    files = {}
    for name, private in (
        ("ai.sign", True),
        ("qs.encrypt", False),
        ("ai.encrypt", True),
        ("qs.sign", False),
    ):
        path = tmp_path / (name + ".json")
        path.write_text(
            host.keys[name].export_private() if private else host.keys[name].export_public()
        )
        path.chmod(0o600)
        files[name] = str(path)
    credentials, routes = tmp_path / "credentials.json", tmp_path / "routes.json"
    credentials.write_text(json.dumps(config["ai"]))
    routes.write_text(json.dumps({"version": 2, "routes": config["routes"]}))
    credentials.chmod(0o600)
    routes.chmod(0o600)
    settings = default_transport_settings(
        host.kit.dsn, host.store.binding, config, files, credentials, routes, paused=paused
    )
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", config["ca_file"])

    class Overrides(Provider):
        @provide(scope=Scope.APP, override=True)
        def database(self) -> Database:
            return host.tx.database  # Fixture owns disposal, never the borrowed container.

        @provide(scope=Scope.APP, override=True)
        def source(self) -> EvidenceSource:
            return host.kit.source

    container = create_container(settings, EvaluationProvider(), Overrides())
    runtime, supervisor = None, None
    state, stop = RuntimeState(), asyncio.Event()
    intent = preflight_intent(scope.run_id, 2, scope.organization_id)
    tracked = False
    native_inserted = False
    try:
        services = await container.get(TransactionServices | None)
        assert services is not None and services.runtime is not b.runtime
        runtime = await MessagingRuntime.create(
            container,
            settings,
            (tmp_path / "ca.pem").read_bytes(),
            (tmp_path / "ai.pem").read_bytes(),
            (tmp_path / "ai.key").read_bytes(),
        )
        assert runtime.transaction_services is services and runtime.tx.database is host.tx.database
        assert runtime.relay is None and not runtime.publishers
        passed("default_assembly_shared_primary_sender")
        passed("default_assembly_no_legacy_publish_path")
        supervisor = asyncio.create_task(supervise(runtime.components(state, 20), state, stop))
        async with asyncio.timeout(30):
            while not state.ready:
                if supervisor.done():
                    await supervisor
                await asyncio.sleep(0.01)
        assert runtime.publisher_ready.is_set() and len(runtime.subscribers) == 2
        assert services.runtime._sender.stats()[
            "ready"
        ]  # Checker lives even when host admission is paused.
        passed("default_assembly_idle_checker_subscriptions")

        if config.get("protocol_protection") is True:
            from qs_ai.bootstrap.transaction_messaging import TransactionAdmissionClosed
            from qs_ai.bootstrap.transaction_protection import ProtocolProtectionError
            from qs_ai.infrastructure.persistence.mysql.transaction_schema import binding

            assert services.runtime._monitor is not None
            assert services.runtime._monitor._task.get_loop() is asyncio.get_running_loop()
            passed("protection_sampler_uses_existing_host_loop")
            before = await evaluation_rows(host.tx, scope.run_id)
            async with host.tx.open() as db:
                changed = await db.execute(
                    update(binding)
                    .where(binding.c.owner == b"ai.mysql", binding.c.writer_epoch == b"m3-1")
                    .values(writer_epoch=b"native-invalid-binding")
                )
                assert changed.rowcount == 1
                await db.commit()
            try:
                async with asyncio.timeout(15):
                    while True:
                        status = services.runtime.protection_stats()
                        if status["protected"] and status["reason"] == "binding_conflict":
                            break
                        await asyncio.sleep(0.05)
                assert not status["available"] and status["observation_errors"] > 0
                passed("protection_actual_primary_binding_conflict_latches")
                stats = services.runtime._sender.stats()
                # Database health may close SDK admission without terminating its
                # bounded Checker workers or maintenance/client ownership.
                assert not stats["stopping"] and not stats["stopped"]
                passed("protection_idle_checker_stays_live")
                with pytest.raises(TransactionAdmissionClosed):
                    services.runtime.set_admission(True)
                with pytest.raises(ProtocolProtectionError):
                    services.runtime.review_protection(
                        status["generation"], "native-binding-review"
                    )
                passed("protection_plain_open_and_unhealthy_review_refused")
                async with container() as request:
                    worker = await request.get(EvaluationWorker)
                    with pytest.raises(TransactionAdmissionClosed):
                        await worker.once()
                assert await evaluation_rows(host.tx, scope.run_id) == before
                passed("protection_worker_cannot_reach_original_business_root")
            finally:
                async with host.tx.open() as db:
                    restored = await db.execute(
                        update(binding)
                        .where(
                            binding.c.owner == b"ai.mysql",
                            binding.c.writer_epoch == b"native-invalid-binding",
                        )
                        .values(writer_epoch=b"m3-1")
                    )
                    assert restored.rowcount == 1
                    await db.commit()
            async with asyncio.timeout(15):
                while True:
                    if (
                        services.runtime.protection_stats()["fresh"]
                        and services.runtime._sender.stats()["ready"]
                    ):
                        break
                    await asyncio.sleep(0.05)
            assert services.runtime.protection_stats()["protected"]
            passed("protection_healthy_sampling_does_not_unlatch")
            with pytest.raises(ProtocolProtectionError):
                services.runtime.review_protection(
                    status["generation"] + 1, "native-binding-review"
                )
            services.runtime.review_protection(status["generation"], "native-binding-review")
            services.runtime.set_admission(not paused)
            assert services.runtime.stats()["ready"] is not paused
            passed("protection_named_review_preserves_explicit_host_admission")
        async with container() as request:
            worker = await request.get(EvaluationWorker)
            assert worker.transactions.database is host.tx.database
            assert worker._serial_runtime.dispatch._runtime is services.runtime
            if paused:
                from qs_ai.bootstrap.transaction_messaging import TransactionAdmissionClosed

                before = await evaluation_rows(host.tx, scope.run_id)
                with pytest.raises(TransactionAdmissionClosed):
                    await worker.once()
                assert await evaluation_rows(host.tx, scope.run_id) == before
                assert await host.store.lookup(intent.request) is None
                passed("default_assembly_pause_no_new_writes")
            else:
                assert await worker.once()
                owned = await b.consumed(SimpleNamespace(intent=intent))
                host.executions.append(owned.execution)
                tracked = True
                passed("default_assembly_original_worker_preflight")
                passed("default_assembly_exact_archive_witness")
                _, event_value = read_evaluation_state_plan(intent, owned.execution.plan)
                event_header = event_value.envelope()
                ack = prepare(
                    messaging_pb.EVENT_ACKNOWLEDGEMENT,
                    str(uuid4()),
                    event_header.aggregate_key,
                    messaging_pb.MessagingBody(
                        event_acknowledgement=messaging_pb.MessagingEventAcknowledgement(
                            event_id=event_header.message_id,
                            event_kind=event_header.kind,
                            event_body_sha256=event_header.body_sha256,
                            outcome=messaging_pb.MessagingEventAcknowledgement.STORED,
                        )
                    ),
                    organization_id=str(event_value.organization_id),
                    signing_key=host.keys["qs.sign"],
                    recipient_key=host.keys["ai.encrypt"],
                )
                acknowledgement = next(s for s in runtime.subscribers if s.route.topic == ACKS)
                await acknowledgement.handler(received(ack))
                async with host.tx.open() as db:
                    receipt = (
                        (
                            await db.execute(
                                select(event_receipts).where(
                                    event_receipts.c.ack_id == ack.envelope.message_id.encode()
                                )
                            )
                        )
                        .mappings()
                        .one()
                    )
                    assert receipt["event_id"] == event_header.message_id.encode()
                    assert receipt["outcome"] == messaging_pb.MessagingEventAcknowledgement.STORED
                passed("default_assembly_authenticated_business_ack")
        if config.get("native_limit") is True:
            from sqlalchemy.ext.asyncio import async_sessionmaker

            from qs_ai.bootstrap.transaction_messaging import TransactionAdmissionClosed
            from qs_ai.bootstrap.transaction_protection import ProtocolProtectionError
            from qs_ai.infrastructure.persistence.mysql.native_limit import MySQLNativeLimits
            from qs_ai.infrastructure.persistence.mysql.native_limit_schema import notices

            assert config.get("protocol_protection") is not True
            evidence = native_limit_audit
            assert evidence is not None
            assert len(evidence.indices) == 1
            assert host.tx.database.engine is not None
            native = MySQLNativeLimits(
                async_sessionmaker(host.tx.database.engine, expire_on_commit=False),
                host.store.binding,
                "m3-1",
            )
            native_inserted = True
            for _ in range(2):
                await native.record(evidence)
            async with host.tx.open() as db:
                records = (await db.execute(select(notices))).mappings().all()
            assert len(records) == 1 and records[0]["evidence_sha256"] == evidence.digest
            passed("native_actual_sys_index_idempotent_original_primary_governance")
            before = await evaluation_rows(host.tx, scope.run_id)
            half_count = services.runtime._sender.stats()["half_prepared_results"]

            async def assert_native_blocked():
                async with asyncio.timeout(15):
                    while True:
                        status = services.runtime.protection_stats()
                        if status["fresh"] and status["native_limit_active"]:
                            break
                        await asyncio.sleep(0.05)
                assert status["protected"] and status["reason"] == "native_transaction_limit"
                assert status["sampler_running"] and services.runtime._sender.stats()["ready"]
                assert not services.runtime._sender.stats()["stopped"]
                assert services.runtime._monitor._task.get_loop() is asyncio.get_running_loop()
                with pytest.raises(TransactionAdmissionClosed):
                    services.runtime.set_admission(True)
                with pytest.raises(ProtocolProtectionError):
                    services.runtime.review_protection(
                        status["generation"], "native-limit-no-resolution"
                    )
                saved = await host.store.lookup(intent.request)
                if saved is not None:
                    # A valid original plan/execution must be denied before any
                    # SDK duplicate decision or local executor invocation.
                    with pytest.raises(TransactionAdmissionClosed):
                        await services.runtime.reserve(intent.request, saved.execution.plan)

                    async def never_business(_):
                        pytest.fail("Native-protected executor reached business")

                    with pytest.raises(TransactionAdmissionClosed):
                        await services.runtime.execute(saved.execution, never_business)
                async with container() as request:
                    worker = await request.get(EvaluationWorker)
                    if saved is None:
                        with pytest.raises(TransactionAdmissionClosed):
                            await worker.once()
                    else:
                        # This fixture has placeholder model assets/candidate
                        # mode and covers only the first preflight. Advancing an
                        # already accepted Run would enter another business
                        # stage with unsupported inputs. The exact valid-plan
                        # Reserve/Execute gates above cover this committed case;
                        # the pending parameter exercises the real preflight.
                        assert worker._serial_runtime.dispatch._runtime is services.runtime
                assert await evaluation_rows(host.tx, scope.run_id) == before

            await assert_native_blocked()
            assert services.runtime._sender.stats()["half_prepared_results"] == half_count
            passed("native_live_admission_and_pending_worker_preflight_without_new_half")
            passed("native_idle_sdk_checker_readiness_existing_host_loop")
            passed("native_plain_open_and_unresolved_review_refused")
            stop.set()
            await supervisor
            await runtime.close()
            await container.close()
            assert not services.runtime.protection_stats()["sampler_running"]
            assert services.runtime.stats()["stopped"]
            passed("native_original_sampler_sdk_shutdown_joined")
            # New configured-open default DI owns a new SDK/client lifecycle;
            # it borrows the same original primary and sees the persistent deny.
            settings = default_transport_settings(
                host.kit.dsn, host.store.binding, config, files, credentials, routes, paused=False
            )
            container = create_container(settings, EvaluationProvider(), Overrides())
            services = await container.get(TransactionServices | None)
            assert services is not None
            runtime = await MessagingRuntime.create(
                container,
                settings,
                (tmp_path / "ca.pem").read_bytes(),
                (tmp_path / "ai.pem").read_bytes(),
                (tmp_path / "ai.key").read_bytes(),
            )
            state, stop = RuntimeState(), asyncio.Event()
            supervisor = asyncio.create_task(supervise(runtime.components(state, 20), state, stop))
            async with asyncio.timeout(30):
                while not state.ready:
                    if supervisor.done():
                        await supervisor
                    await asyncio.sleep(0.01)
            await assert_native_blocked()
            assert services.runtime._sender.stats()["half_prepared_results"] == 0
            passed("native_new_configured_open_default_assembly_keeps_durable_deny")
            passed("native_original_facts_witness_and_host_primary_preserved")
            assert await native.read_native_limit()
            passed("native_no_runtime_governance_resolution_or_deletion")
    finally:
        stop.set()
        if supervisor is not None:
            await supervisor
        if runtime is not None:
            await runtime.close()
        await container.close()
        if config.get("protocol_protection") is True and runtime is not None:
            assert not services.runtime.protection_stats()["sampler_running"]
            assert services.runtime.stats()["stopped"]
            passed("protection_actual_sampler_shutdown_retains_primary_pool")
        if native_inserted:
            # Test isolation only, AFTER both actual default lifecycles joined.
            # This disposable shared test schema resets its SDK binding between
            # parametrized roots. Never expose this as production resolution.
            assert not services.runtime.protection_stats()["sampler_running"]
            assert services.runtime.stats()["stopped"]
            async with host.tx.open() as db:
                await db.execute(
                    delete(notices).where(
                        notices.c.owner == host.store.binding.owner.encode(),
                        notices.c.store_id == host.store.binding.store_id.encode(),
                        notices.c.writer_epoch == host.store.binding.writer_epoch.encode(),
                        notices.c.route_revision == b"m3-1",
                    )
                )
                await db.commit()
        owned = await host.store.lookup(intent.request)
        if owned is not None and not tracked:
            host.executions.append(owned.execution)
        async with host.tx.open() as db:
            assert await db.scalar(text("SELECT 1")) == 1
            if owned is not None:
                ids = [r.message_id.encode() for r in owned.execution.references]
                await db.execute(delete(event_receipts).where(event_receipts.c.event_id.in_(ids)))
            await db.commit()
        await clean_governance_sequence(host, scope.run_id)
    passed("default_assembly_close_preserves_pool")


@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_actual_default_serial_factory_commits_dispatch_response_and_terminal(
    governance_broker, resolution_ready, kind
):
    """Complete paired serial graph with real SDK/Broker and a synthetic model."""
    from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
    from qs_ai.application.interpretation.route_assets import executable_route
    from qs_ai.bootstrap.evaluation_transactions import transaction_evaluation_worker
    from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor
    from qs_ai.infrastructure.persistence.mysql.schema import evaluation_response_receipts
    from tests.integration.test_evaluation_step import AT, Gateway, step

    b = governance_broker
    tx, run_id, _, routes, schemas = resolution_ready
    gateway = Gateway(resolution_ready)
    version = (await step(resolution_ready, gateway)).version if kind == "semantic" else 3
    before = gateway.calls
    first = len(b.host.executions)
    provider = executable_route(routes.get.return_value).provider
    capacity = ModelCapacity({provider: ProviderCapacity(2, 1)}, 1)
    worker = transaction_evaluation_worker(
        b.host.tx,
        b.host.archives,
        b.runtime,
        gateway,
        routes,
        schemas,
        "worker:default-paired",
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
        candidate_limit=1,
        capacity=capacity,
        recovery_cursor=RecoveryCursor(),
        clock=lambda: AT + timedelta(seconds=2),
    )
    try:
        async with asyncio.timeout(60):
            assert await worker.once()
        _, _, current = await evaluation_rows(tx, run_id)
        assert current["version"] == version + 3 and current["checkpoint_json"] is None
        assert gateway.calls == before + 1
        executions = b.host.executions[first:]
        assert len(executions) == 2
        for owned in executions:
            for reference in owned.references:
                assert await StoreChecker(b.host.store).check(reference) == Decision.COMMIT
                value = await b.host.archives.read_committed(reference.message_id)
                assert value is not None
                async with asyncio.timeout(60):
                    while reference.message_id not in b.messages:
                        assert not b.failures
                        await asyncio.sleep(0.01)
                assert b.messages[reference.message_id] == hashlib.sha256(value.wire).hexdigest()
                async with tx.open() as db:
                    assert not await db.scalar(
                        select(outbox.c.message_id).where(
                            outbox.c.message_id == reference.message_id
                        )
                    )
        async with tx.open() as db:
            response = (
                (
                    await db.execute(
                        select(evaluation_response_receipts).where(
                            evaluation_response_receipts.c.run_id == str(run_id)
                        )
                    )
                )
                .mappings()
                .all()
            )
        assert len(response) == 1
        token = capacity.try_acquire(provider, evaluation=True)
        assert token is not None
        token.release()
        b.recorder.flush.assert_not_called()
        passed("default_serial_actual_factory_" + kind)
        passed("default_serial_first_response_original_witness_no_legacy")
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_response_receipts).where(
                    evaluation_response_receipts.c.run_id == str(run_id)
                )
            )
            await db.commit()
        await clean_governance_sequence(b.host, run_id)


async def _wait_recovery_barrier(task, event):
    async with asyncio.timeout(30):
        while not event.is_set():
            if task.done():
                await task
                raise AssertionError("Recovery ended before its original root barrier")
            await asyncio.sleep(0.05)


async def _recovery_half_gate(b, tx, run_id, invoke, monkeypatch, label, unchanged):
    """Actual halves and root; deliberately discarded client reply is not packet fault proof."""
    from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
    from qs_ai.infrastructure.workflow_transport.transaction_archive import decode_branch
    from tests.integration.test_evaluation_unknowns import snapshot

    before = await snapshot(tx, run_id)
    b.runtime.entered.clear()
    b.runtime.release.clear()
    b.runtime.pause = True
    count = len(b.runtime.results)
    task = asyncio.create_task(invoke())
    try:
        await _wait_recovery_barrier(task, b.runtime.entered)
        owned = b.host.executions[-1]
        facts = EvaluationStateFacts.decode(decode_branch(owned.plan.branches[0]).context)
        _, archive = read_evaluation_state_plan(facts.intent, owned.plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert archive.envelope().message_id not in b.messages
        assert await snapshot(tx, run_id) == before
        unchanged()
        passed(label + "_half_invisible")
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed recovery final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            result = await asyncio.wait_for(task, timeout=30)
        assert len(b.runtime.results) == count + 1
        outcome = b.runtime.results[-1]
        assert outcome.local == LocalOutcome.COMMITTED
        assert outcome.branches and all(x.final == FinalOutcome.UNKNOWN for x in outcome.branches)
        passed(label + "_committed_final_unknown")
        await b.consumed(SimpleNamespace(intent=facts.intent))
        unchanged()
        passed(label + "_no_new_provider")
        passed("recovery_first_wire_authenticated")
        passed("recovery_committed_archive_witness")
        passed("recovery_no_legacy_commit_hook")
        return result, facts
    finally:
        b.runtime.pause = False
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def _clean_recovery(b, tx, run_id):
    from qs_ai.infrastructure.persistence.mysql.schema import (
        evaluation_response_receipts,
        evaluation_slot_claims,
    )

    async with tx.open() as db:
        for table in (evaluation_response_receipts, evaluation_slot_claims):
            await db.execute(delete(table).where(table.c.run_id == str(run_id)))
        await db.commit()
    await clean_governance_sequence(b.host, run_id)


def _provider_unchanged(gateway, calls):
    current = gateway.calls if isinstance(gateway.calls, int) else len(gateway.calls)
    assert current == calls


@pytest.mark.parametrize("mode", ["prepared", "generation", "semantic"])
async def test_actual_serial_expiry_recovery_halves_and_original_unknown(
    governance_broker, resolution_ready, monkeypatch, mode
):
    from qs_ai.infrastructure.workflow_transport.serial_recovery_runtime import (
        SerialRecoveryRuntime,
    )
    from tests.integration.test_evaluation_recovery import EXPIRY, pending
    from tests.integration.test_evaluation_step import Gateway, step

    b = governance_broker
    tx, run_id, _, routes, _ = resolution_ready
    gateway = Gateway(resolution_ready)
    version = (await step(resolution_ready, gateway)).version if mode == "semantic" else 3
    cp = await pending(resolution_ready, dispatched=mode != "prepared", version=version)
    runtime = SerialRecoveryRuntime(
        b.host.tx,
        b.host.archives,
        b.runtime,
        routes,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
        recover_aborted=True,
    )
    calls = gateway.calls
    try:
        result, facts = await _recovery_half_gate(
            b,
            tx,
            run_id,
            lambda: runtime(
                run_id, cp.version, 1, "invocation:dead", EXPIRY, EXPIRY, "system:recovery"
            ),
            monkeypatch,
            "recovery_serial_" + mode,
            lambda: _provider_unchanged(gateway, calls),
        )
        # This codec binds the exact expired checkpoint in expected_version;
        # source_version is reserved for operations whose coordinator advanced.
        assert result is True and facts.intent.expected_version == cp.version
        assert facts.source_version is None
        run, _, current = await evaluation_rows(tx, run_id)
        assert current["version"] == cp.version + 1 and current["checkpoint_json"] is None
        assert run["progress_json"]["status"] == ("collecting" if mode == "prepared" else "blocked")
        assert run["progress_json"]["recoveries"][-1]["cause_code"] == (
            "expired_preparation_released"
            if mode == "prepared"
            else "expired_dispatch_result_unknown"
        )
    finally:
        await _clean_recovery(b, tx, run_id)


async def _candidate_recovery_seed(b, ready, mode, *, response=False, other=False):
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import prepare_candidate
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
        read_candidate_response,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
        EvaluationResponse,
        save_response,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_step import prepare_step
    from qs_ai.infrastructure.workflows.evaluation import execute_step
    from tests.integration.test_evaluation_candidate_execution import ConcurrentGateway
    from tests.integration.test_evaluation_step import AT

    tx, run_id, _, routes, schemas = ready
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(execution_mode="candidate_v2")
        )
        await db.commit()
    gateway = ConcurrentGateway(ready)
    gateway.release.set()
    version = 3
    if mode == "semantic":
        version = (
            await execute_step(
                tx,
                run_id,
                version,
                1,
                "worker:seed",
                gateway,
                routes,
                schemas,
                clock=lambda: AT,
                candidate_limit=3,
            )
        ).version
    if mode == "prepared":
        async with tx.open() as db:
            claim = await prepare_candidate(
                db,
                run_id,
                1,
                "worker:dead",
                str(uuid4()),
                str(uuid4()),
                AT,
                AT + timedelta(minutes=1),
                limit=3,
            )
            await db.commit()
    else:
        prepared = await prepare_step(
            tx,
            run_id,
            version,
            1,
            "worker:dead",
            routes,
            schemas,
            clock=lambda: AT + timedelta(seconds=2),
            candidate_limit=3,
        )
        claim = prepared.claim
        assert claim is not None
    first = None
    if response:
        output = await gateway.generate_messages(
            prepared.messages, prepared.route, prepared.schema, prepared.invocation_id
        )
        result = EvaluationResponse(output, None, AT + timedelta(seconds=3))
        async with tx.open() as db:
            await save_response(db, 1, claim, result, at=result.finished_at)
            await db.commit()
        async with tx.open() as db:
            first = await read_candidate_response(db, 1, claim)
        assert first is not None
    if other:
        async with tx.open() as db:
            await prepare_candidate(
                db,
                run_id,
                1,
                "worker:other",
                str(uuid4()),
                str(uuid4()),
                AT + timedelta(seconds=2),
                AT + timedelta(minutes=2),
                limit=3,
            )
            await db.commit()
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("mode", ["prepared", "generation", "semantic"])
async def test_actual_candidate_expiry_preserves_other_claim_and_original_unknown(
    governance_broker, resolution_ready, monkeypatch, mode
):
    from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import active_claims
    from qs_ai.infrastructure.workflow_transport.candidate_recovery_runtime import (
        CandidateRecoveryRuntime,
    )

    b = governance_broker
    p = await _candidate_recovery_seed(b, resolution_ready, mode, other=True)
    runtime = CandidateRecoveryRuntime(
        b.host.tx,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
        recover_aborted=True,
    )
    calls = len(p.gateway.calls)
    try:
        result, facts = await _recovery_half_gate(
            b,
            p.tx,
            p.run_id,
            lambda: runtime(1, p.claim, p.claim.checkpoint.lease_expires_at),
            monkeypatch,
            "recovery_candidate_" + mode,
            lambda: _provider_unchanged(p.gateway, calls),
        )
        assert result is True
        run, _, current = await evaluation_rows(p.tx, p.run_id)
        assert current["version"] == facts.source_version + 1
        assert run["progress_json"]["status"] == "collecting"
        async with p.tx.open() as db:
            remaining = await active_claims(db, p.run_id, lock=False)
        assert len(remaining) == 1 and remaining[0].checkpoint.owner == "worker:other"
    finally:
        await _clean_recovery(b, p.tx, p.run_id)


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_actual_recovery_reuses_first_response_without_provider(
    governance_broker, resolution_ready, monkeypatch, candidate, kind
):
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
        read_candidate_response,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
        read_serial_response,
    )
    from qs_ai.infrastructure.workflow_transport.candidate_recovery_runtime import (
        CandidateRecoveryRuntime,
    )
    from qs_ai.infrastructure.workflow_transport.serial_recovery_runtime import (
        SerialRecoveryRuntime,
    )

    b = governance_broker
    if candidate:
        p = await _candidate_recovery_seed(b, resolution_ready, kind, response=True)
        runtime = CandidateRecoveryRuntime(
            b.host.tx,
            b.host.archives,
            b.runtime,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
            recover_aborted=True,
        )

        def invoke():
            return runtime(1, p.claim, p.claim.checkpoint.lease_expires_at)

        label = "recovery_first_candidate_" + kind
        calls = len(p.gateway.calls)
    else:
        p = await _serial_terminal_seed(b, resolution_ready, semantic=kind == "semantic")
        runtime = SerialRecoveryRuntime(
            b.host.tx,
            b.host.archives,
            b.runtime,
            p.routes,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
            recover_aborted=True,
        )
        cp = p.reference.checkpoint

        def invoke():
            return runtime(
                p.run_id,
                p.prepared.state.version,
                1,
                cp.invocation_id,
                cp.lease_expires_at,
                cp.lease_expires_at,
                "system:recovery",
            )

        label = "recovery_first_serial_" + kind
        calls = p.gateway.calls
    try:
        result, facts = await _recovery_half_gate(
            b,
            p.tx,
            p.run_id,
            invoke,
            monkeypatch,
            label,
            lambda: _provider_unchanged(p.gateway, calls),
        )
        assert result is True
        _, _, current = await evaluation_rows(p.tx, p.run_id)
        assert current["version"] == facts.source_version + 1 and current["checkpoint_json"] is None
        async with p.tx.open() as db:
            if candidate:
                assert await read_candidate_response(db, 1, p.claim) == p.first
            else:
                assert await read_serial_response(db, p.reference) == p.result
    finally:
        await _clean_recovery(b, p.tx, p.run_id)


async def test_actual_cancel_drain_keeps_original_request_and_completed_response(
    governance_broker, resolution_ready, monkeypatch
):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.infrastructure.workflow_transport.cancellation_drain_runtime import (
        CancellationDrainRuntime,
    )

    b = governance_broker
    p = await _serial_terminal_seed(b, resolution_ready)
    scope = ManagementScope(p.run_id, 1, 42)
    management = MySQLEvaluationManagement(p.tx)
    at = p.result.finished_at
    try:
        requested = await management.cancel(
            scope,
            p.prepared.state.version,
            "original cancel request",
            at,
            discard=False,
            confirm=True,
        )
        assert requested.cancel_draining
        await p.sender(p.reference)
        await b.consumed(SimpleNamespace(intent=p.intent))
        runtime = CancellationDrainRuntime(
            b.host.tx,
            b.host.archives,
            b.runtime,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
            recover_aborted=True,
        )
        calls = p.gateway.calls
        result, facts = await _recovery_half_gate(
            b,
            p.tx,
            p.run_id,
            lambda: runtime(scope, at + timedelta(seconds=1)),
            monkeypatch,
            "recovery_cancel_drain",
            lambda: _provider_unchanged(p.gateway, calls),
        )
        assert result is True and facts.intent.expected_version == requested.version
        assert facts.source_version > requested.version
        view = await management.get(scope)
        assert view.status == "canceled" and view.version == facts.source_version + 1
        audit = json.loads(view.cancellation_json)
        assert audit["actor"] == scope.actor and audit["reason"] == "original cancel request"
    finally:
        await _clean_recovery(b, p.tx, p.run_id)


@pytest.mark.parametrize("candidate", [False, True])
async def test_actual_aborted_dispatch_requires_explicit_replan_and_one_capacity_handoff(
    governance_broker, resolution_ready, monkeypatch, candidate
):
    from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
    from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
        CandidateDispatchPending,
        CandidateDispatchRecovery,
        CandidateDispatchSender,
    )
    from qs_ai.infrastructure.workflow_transport.candidate_dispatch_state import (
        CandidateDispatchPlanner,
    )
    from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
        WorkerDispatchPending,
        WorkerDispatchRecovery,
        WorkerDispatchSender,
    )
    from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import WorkerDispatchPlanner
    from tests.integration.test_evaluation_step import AT
    from tests.integration.test_evaluation_unknowns import snapshot

    b = governance_broker
    tx, run_id, _, _, schemas = resolution_ready
    if candidate:
        async with tx.open() as db:
            await db.execute(
                update(evaluation_runs)
                .where(evaluation_runs.c.run_id == str(run_id))
                .values(execution_mode="candidate_v2")
            )
            await db.commit()
        planner = CandidateDispatchPlanner(
            b.host.tx,
            schemas,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
            limit=3,
            clock=lambda: AT,
        )
    else:
        planner = WorkerDispatchPlanner(
            b.host.tx,
            schemas,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
            clock=lambda: AT,
        )
    _, inputs = await planner.candidate(run_id, 3, 1, "worker:recovery")
    pool = ModelCapacity({inputs.route.provider: ProviderCapacity(2, 1)}, 1)
    if candidate:
        strict = CandidateDispatchSender(
            b.host.tx, b.host.archives, b.runtime, "worker:recovery", planner, capacity=pool
        )
        recovery = CandidateDispatchRecovery(strict)
    else:
        strict = WorkerDispatchSender(
            b.host.tx,
            b.host.archives,
            b.runtime,
            schemas,
            "worker:recovery",
            planner,
            capacity=pool,
        )
        recovery = WorkerDispatchRecovery(strict)
    before = await snapshot(tx, run_id)
    b.runtime.pause = True
    old_task = asyncio.create_task(strict(run_id, 3, 1))
    token = None
    try:
        await _wait_recovery_barrier(old_task, b.runtime.entered)
        old = b.host.executions[-1]
        assert await b.host.store.abort(old)
        b.runtime.release.set()
        with pytest.raises((TransactionSendError, CandidateDispatchPending, WorkerDispatchPending)):
            await old_task
        assert b.runtime.results[-1].local == LocalOutcome.ABORTED
        assert await snapshot(tx, run_id) == before
        assert await StoreChecker(b.host.store).check(old.references[0]) == Decision.ROLLBACK
        # A new dispatch lease is an explicit host policy after exact old abort,
        # never a retry of a committed model dispatch or its provider invocation.
        planner.clock = lambda: AT + timedelta(seconds=10)

        def unchanged():
            assert pool.try_acquire(inputs.route.provider, evaluation=True) is None

        label = "recovery_dispatch_candidate" if candidate else "recovery_dispatch_serial"
        prepared, _ = await _recovery_half_gate(
            b, tx, run_id, lambda: recovery(run_id, 3, 1), monkeypatch, label, unchanged
        )
        token = prepared.capacity_token
        assert token is not None and prepared.cp.claimed_at == AT + timedelta(seconds=10)
        assert b.host.executions[-1].plan.digest != old.plan.digest
        with pytest.raises((CandidateDispatchPending, WorkerDispatchPending)):
            await recovery(run_id, 3, 1)
        assert len(b.runtime.results) == 2
        # Deliberately stop at the original model handoff: this is not a
        # provider invocation or default serial/candidate composition claim.
    finally:
        b.runtime.pause = False
        b.runtime.release.set()
        await asyncio.gather(old_task, return_exceptions=True)
        if token is not None:
            token.release()
        available = pool.try_acquire(inputs.route.provider, evaluation=True)
        assert available is not None
        available.release()
        await _clean_recovery(b, tx, run_id)


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


async def _seed_original_interpretation_finish(b, kind):
    from qs_ai.application.interpretation.ports import WorkflowResult
    from qs_ai.infrastructure.persistence.mysql.schema import model_calls, sessions
    from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import (
        InterpretationFinishInput,
    )
    from tests.integration.test_artifact_acceptance import ready as ready_artifact
    from tests.integration.test_interpretation_finish_root import snapshot

    # Explicit legacy fixture seeding happens before the operation under test.
    # The real SDK path must override the failing old recorder on its own root.
    previous = b.host.tx.database.state_events
    b.host.tx.database.state_events = None
    try:
        if kind == "artifact":
            claim, candidate = await ready_artifact(b.host.kit)
            result = WorkflowResult("", artifact=candidate)
        else:
            await b.host.kit.queued()
            claim = await b.host.kit.store.claim(120)
            assert claim is not None
            result = WorkflowResult(
                "checkpoint:broker-first",
                question="original frozen question" if kind == "question" else None,
                can_skip=False,
                failure_code=None if kind == "question" else "model_not_connected",
            )
        await b.host.kit.store.renew(claim, 120)
        request_id = str(uuid4())
        async with b.host.tx.open() as db:
            await db.execute(
                update(sessions)
                .where(sessions.c.id == claim.session.id)
                .values(request_id=request_id)
            )
            await db.commit()
            call = (
                (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
                .mappings()
                .one_or_none()
            )
    finally:
        b.host.tx.database.state_events = previous
    incoming = InterpretationFinishInput(claim, result)
    return SimpleNamespace(
        incoming=incoming,
        intent=incoming.intent(request_id),
        call=call,
        before=await snapshot(b.host.kit, claim),
    )


async def _actual_interpretation_finish(b, kind, monkeypatch):
    from qs_ai.application.execution.generation import DurableGeneration
    from qs_ai.infrastructure.persistence.mysql.schema import model_calls
    from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
        InterpretationFinishSender,
    )
    from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import read_finish_plan
    from tests.integration.test_interpretation_finish_root import snapshot

    p = await _seed_original_interpretation_finish(b, kind)
    finish = InterpretationFinishSender(
        b.host.kit.store,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
    )
    no_generation = AsyncMock(
        side_effect=AssertionError("Finalization must reuse durable response")
    )
    monkeypatch.setattr(DurableGeneration, "execute", no_generation)
    b.runtime.pause = True
    task = asyncio.create_task(finish(p.incoming.claim, p.incoming.result))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        facts, first, question_id = read_finish_plan(p.incoming, p.intent, owned.plan)
        message_id = first.envelope().message_id
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert message_id not in b.messages
        assert await snapshot(b.host.kit, p.incoming.claim) == p.before
        passed("interpretation_finish_half_invisible_" + kind)
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed interpretation finish final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            assert await task is None
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        passed("interpretation_finish_final_unknown_keeps_commit")
        async with asyncio.timeout(60):
            while message_id not in b.messages:
                assert not b.failures
                await asyncio.sleep(0.05)
        assert b.messages[message_id] == hashlib.sha256(first.wire).hexdigest()
        assert await b.host.archives.read_committed(message_id) == first
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.COMMIT
        after = await snapshot(b.host.kit, p.incoming.claim)
        assert after[0][0]["version"] == p.incoming.claim.session.version + 1
        assert after[2][0]["status"] == "done"
        history = next(row for row in after[5] if row["event_id"] == message_id)
        assert history["mq_owned"] and not history["delivered"] and history["attempts"] == 0
        async with b.host.tx.open() as db:
            assert (
                await db.scalar(
                    select(outbox.c.message_id).where(outbox.c.message_id == message_id)
                )
                is None
            )
            current_call = (
                (
                    await db.execute(
                        select(model_calls).where(model_calls.c.run_id == p.incoming.claim.run_id)
                    )
                )
                .mappings()
                .one_or_none()
            )
        passed("interpretation_finish_original_business_archive_witness")
        if kind == "question":
            assert after[4][0]["id"] == question_id
            assert (
                after[4][0]["question_seq"]
                == json.loads(facts.business_context)["question_sequence"]
            )
            passed("interpretation_finish_first_question_sequence")
        if kind == "artifact":
            assert p.call == current_call and current_call["status"] == "response_received"
            value = parse_body(first.envelope(), first.body).interpretation_state
            assert json.loads(value.artifact_json)["id"] == p.incoming.result.artifact.id
            passed("interpretation_finish_durable_artifact_no_new_invocation")

        async def no_plan(*args):
            raise AssertionError("Committed finish must not replan or seal")

        monkeypatch.setattr(finish._planner, "candidate", no_plan)
        assert await finish(p.incoming.claim, p.incoming.result) is None
        assert await snapshot(b.host.kit, p.incoming.claim) == after
        assert b.runtime.calls == len(b.host.executions) == 1
        assert await b.host.archives.read_committed(message_id) == first
        no_generation.assert_not_called()
        b.recorder.flush.assert_not_called()
        passed("interpretation_finish_repeat_no_replan_or_send")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_actual_interpretation_finish_question_half_gate_first_wire_and_final_unknown(
    governance_broker, monkeypatch
):
    await _actual_interpretation_finish(governance_broker, "question", monkeypatch)


async def test_actual_interpretation_finish_failure_half_gate_first_wire_and_final_unknown(
    governance_broker, monkeypatch
):
    await _actual_interpretation_finish(governance_broker, "failure", monkeypatch)


@pytest.mark.usefixtures("published_configuration")
async def test_actual_interpretation_finish_artifact_half_gate_first_wire_and_final_unknown(
    governance_broker, monkeypatch
):
    await _actual_interpretation_finish(governance_broker, "artifact", monkeypatch)


async def test_actual_interpretation_finish_source_conflict_rolls_back_without_new_send(
    governance_broker,
):
    from sqlalchemy import update

    from qs_ai.infrastructure.persistence.mysql.schema import sessions
    from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
        InterpretationFinishPending,
        InterpretationFinishSender,
    )
    from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import read_finish_plan
    from tests.integration.test_interpretation_finish_root import snapshot

    b = governance_broker
    p = await _seed_original_interpretation_finish(b, "failure")
    finish = InterpretationFinishSender(
        b.host.kit.store,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
    )
    b.runtime.pause = True
    task = asyncio.create_task(finish(p.incoming.claim, p.incoming.result))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, first, _ = read_finish_plan(p.incoming, p.intent, owned.plan)
        previous = b.host.tx.database.state_events
        b.host.tx.database.state_events = None
        try:
            async with b.host.tx.open() as db:
                await db.execute(
                    update(sessions)
                    .where(sessions.c.id == p.incoming.claim.session.id)
                    .values(goal="competing original business input")
                )
                await db.commit()
        finally:
            b.host.tx.database.state_events = previous
        competing = await snapshot(b.host.kit, p.incoming.claim)
        b.runtime.release.set()
        with pytest.raises((InterpretationFinishPending, TransactionSendError)):
            await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(branch.final == FinalOutcome.ROLLBACK_CONFIRMED for branch in result.branches)
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await snapshot(b.host.kit, p.incoming.claim) == competing
        assert await b.host.archives.read_committed(first.envelope().message_id) is None
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("interpretation_finish_source_conflict_rollback")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def _seed_original_claim(b, *, exhausted=False, quota=False):
    from tests.integration.test_transaction_interpretation_claim import candidate

    previous = b.host.tx.database.state_events
    b.host.tx.database.state_events = None
    try:
        if quota:
            first, _, _ = await candidate(b.host, snapshot=True)
            prior = await b.host.kit.store.claim(60)
            assert prior is not None and prior.session.id == first.session.id
        source, before, _ = await candidate(b.host, exhausted, snapshot=quota)
        return source, before
    finally:
        b.host.tx.database.state_events = previous


async def _actual_interpretation_claim(b, exhausted, monkeypatch):
    from qs_ai.application.execution.generation import DurableGeneration
    from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
        InterpretationClaimSender,
    )
    from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import read_claim_plan
    from tests.integration.test_interpretation_claim_root import rows

    source, before = await _seed_original_claim(b, exhausted=exhausted)
    sender = InterpretationClaimSender(
        b.host.kit.store,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
    )
    no_generation = AsyncMock(side_effect=AssertionError("Claim must not invoke provider"))
    monkeypatch.setattr(DurableGeneration, "execute", no_generation)
    b.runtime.pause = True
    task = asyncio.create_task(sender(source, 60))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        facts, first = read_claim_plan(source, 60, owned.plan)
        message_id = first.envelope().message_id
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert message_id not in b.messages
        assert await rows(b.host.kit, source.session.id) == before
        passed("claim_half_invisible_exhausted" if exhausted else "claim_half_invisible_running")
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed original claim final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            claim = await task
        assert (claim is None) is exhausted
        if claim is not None:
            assert claim.session.id == source.session.id and claim.fence == source.fence + 1
            passed("claim_first_handoff_once")
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        passed("claim_final_unknown_keeps_commit")
        async with asyncio.timeout(60):
            while message_id not in b.messages:
                assert not b.failures
                await asyncio.sleep(0.05)
        assert b.messages[message_id] == hashlib.sha256(first.wire).hexdigest()
        assert await b.host.archives.read_committed(message_id) == first
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.COMMIT
        after = await rows(b.host.kit, source.session.id)
        assert after[0][0]["version"] == facts.target_version
        assert after[0][0]["status"] == ("blocked" if exhausted else "running")
        assert after[1][0]["status"] == ("dead" if exhausted else "leased")
        assert after[1][0]["attempt"] == (3 if exhausted else 1)
        assert len(after[4]) == 1 and after[4][0]["event_id"] == message_id
        assert after[4][0]["mq_owned"] and not after[4][0]["delivered"]
        passed("claim_original_business_archive_witness")

        async def no_plan(*args):
            raise AssertionError("Committed claim must not replan or seal")

        monkeypatch.setattr(sender._planner, "candidate", no_plan)
        assert await sender(source, 60) is None
        assert await rows(b.host.kit, source.session.id) == after
        assert b.runtime.calls == len(b.host.executions) == 1
        no_generation.assert_not_called()
        b.recorder.flush.assert_not_called()
        passed("claim_duplicate_no_handoff_or_send")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_actual_interpretation_claim_running_half_gate_first_handoff_and_final_unknown(
    governance_broker,
    monkeypatch,
):
    await _actual_interpretation_claim(governance_broker, False, monkeypatch)


async def test_actual_interpretation_claim_exhausted_two_transitions_and_final_unknown(
    governance_broker,
    monkeypatch,
):
    await _actual_interpretation_claim(governance_broker, True, monkeypatch)


async def _actual_claim_rollback(b, *, quota):
    from qs_ai.infrastructure.persistence.mysql.schema import sessions
    from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
        InterpretationClaimPending,
        InterpretationClaimSender,
    )
    from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import read_claim_plan
    from tests.integration.test_interpretation_claim_root import rows

    source, before = await _seed_original_claim(b, quota=quota)
    sender = InterpretationClaimSender(
        b.host.kit.store,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
    )
    b.runtime.pause = True
    task = asyncio.create_task(sender(source, 60))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, first = read_claim_plan(source, 60, owned.plan)
        if not quota:
            previous = b.host.tx.database.state_events
            b.host.tx.database.state_events = None
            try:
                async with b.host.tx.open() as db:
                    await db.execute(
                        update(sessions)
                        .where(sessions.c.id == source.session.id)
                        .values(
                            goal="competing original claim source",
                        )
                    )
                    await db.commit()
            finally:
                b.host.tx.database.state_events = previous
            before = await rows(b.host.kit, source.session.id)
        b.runtime.release.set()
        with pytest.raises((InterpretationClaimPending, TransactionSendError)):
            await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.ABORTED
        assert all(branch.final == FinalOutcome.ROLLBACK_CONFIRMED for branch in result.branches)
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
        assert await rows(b.host.kit, source.session.id) == before
        assert await b.host.archives.read_committed(first.envelope().message_id) is None
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed(
            "claim_quota_deferral_no_phantom_event" if quota else "claim_source_conflict_rollback"
        )
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_actual_interpretation_claim_source_conflict_rolls_back_without_handoff(
    governance_broker,
):
    await _actual_claim_rollback(governance_broker, quota=False)


async def test_actual_interpretation_claim_capacity_denial_cannot_publish_running(
    governance_broker,
):
    await _actual_claim_rollback(governance_broker, quota=True)


@pytest.mark.parametrize("old_abort", [False, True])
async def test_actual_original_claim_scanner_first_plan_and_explicit_abort_retry(
    governance_broker,
    monkeypatch,
    old_abort,
):
    from qs_ai.application.execution.generation import DurableGeneration
    from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import read_claim_plan
    from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
        TransactionalInterpretationExecutionStore,
    )
    from tests.integration.test_interpretation_claim_root import rows

    b = governance_broker
    source, before = await _seed_original_claim(b)
    worker = TransactionalInterpretationExecutionStore(
        b.host.kit.store,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
    )
    previous = None
    if old_abort:
        plan = await worker._claims._planner.candidate(source, 60)
        previous = await b.runtime.reserve(source.intent(60).request, plan)
        assert await b.host.store.abort(previous.execution)
        monkeypatch.setattr(
            worker._claims._planner,
            "candidate",
            AsyncMock(
                side_effect=AssertionError("Explicit abort retry must reuse first wire"),
            ),
        )
    no_generation = AsyncMock(side_effect=AssertionError("Scanner cannot invoke provider"))
    monkeypatch.setattr(DurableGeneration, "execute", no_generation)
    b.runtime.pause = True
    task = asyncio.create_task(worker.claim(60))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        _, first = read_claim_plan(source, 60, owned.plan)
        message_id = first.envelope().message_id
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert message_id not in b.messages and await rows(b.host.kit, source.session.id) == before
        passed("scanner_half_before_original_root")
        b.runtime.release.set()
        claim = await task
        assert claim is not None and claim.session.id == source.session.id
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.COMMIT
        if previous is not None:
            assert owned.plan == previous.execution.plan
            assert owned.fence == previous.execution.fence + 1
            assert (
                await StoreChecker(b.host.store).check(previous.execution.references[0])
                == Decision.ROLLBACK
            )
            passed("scanner_explicit_abort_retry_first_wire")
        async with asyncio.timeout(60):
            while message_id not in b.messages:
                assert not b.failures
                await asyncio.sleep(0.05)
        assert b.messages[message_id] == hashlib.sha256(first.wire).hexdigest()
        assert await b.host.archives.read_committed(message_id) == first
        passed("scanner_first_claim_committed_handoff")
        assert await worker.claim(60) is None and b.runtime.calls == 1
        no_generation.assert_not_called()
        b.recorder.flush.assert_not_called()
        passed("scanner_duplicate_no_new_model_or_send")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_actual_original_claim_scanner_quota_defers_without_running_event(
    governance_broker,
):
    from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import read_claim_plan
    from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
        TransactionalInterpretationExecutionStore,
    )
    from tests.integration.test_interpretation_claim_root import rows

    b = governance_broker
    source, before = await _seed_original_claim(b, quota=True)
    worker = TransactionalInterpretationExecutionStore(
        b.host.kit.store,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
    )
    assert await worker.claim(60) is None
    owned = b.host.executions[-1]
    _, first = read_claim_plan(source, 60, owned.plan)
    assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.ROLLBACK
    after = await rows(b.host.kit, source.session.id)
    assert after[0] == before[0] and after[2:] == before[2:]
    old_job, new_job = dict(before[1][0]), dict(after[1][0])
    assert new_job.pop("available_at") > old_job.pop("available_at") and new_job == old_job
    assert not after[3] and not after[4]
    b.empty_poll.clear()
    await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
    assert first.envelope().message_id not in b.messages
    b.recorder.flush.assert_not_called()
    passed("scanner_quota_deferral_no_running_event")


@pytest.mark.usefixtures("published_configuration")
@pytest.mark.parametrize("kind", ["question", "artifact"])
async def test_actual_explicit_finalization_retry_preserves_first_wire_and_durable_response(
    governance_broker,
    monkeypatch,
    kind,
):
    from qs_ai.application.execution.generation import DurableGeneration
    from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
        InterpretationFinishSender,
    )
    from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import read_finish_plan
    from tests.integration.test_interpretation_finish_root import snapshot

    b = governance_broker
    p = await _seed_original_interpretation_finish(b, kind)
    finish = InterpretationFinishSender(
        b.host.kit.store,
        b.host.archives,
        b.runtime,
        signing_key=b.host.keys["ai.sign"],
        recipient_key=b.host.keys["qs.encrypt"],
    )
    first_plan = await finish._planner.candidate(p.incoming, p.intent)
    previous = await b.runtime.reserve(p.intent.request, first_plan)
    assert await b.host.store.abort(previous.execution)
    finish._planner.candidate = AsyncMock(side_effect=AssertionError("Retry cannot replan or Seal"))
    no_generation = AsyncMock(
        side_effect=AssertionError("Retry cannot repeat durable provider work")
    )
    monkeypatch.setattr(DurableGeneration, "execute", no_generation)
    b.runtime.pause = True
    task = asyncio.create_task(
        finish.retry(p.incoming.claim, p.incoming.result, previous.execution)
    )
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        assert owned.plan == previous.execution.plan and owned.fence == previous.execution.fence + 1
        _, first, question_id = read_finish_plan(p.incoming, p.intent, owned.plan)
        message_id = first.envelope().message_id
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert (
            message_id not in b.messages
            and await snapshot(b.host.kit, p.incoming.claim) == p.before
        )
        passed("finish_retry_half_invisible")
        b.runtime.release.set()
        assert await task is None
        assert (
            await StoreChecker(b.host.store).check(previous.execution.references[0])
            == Decision.ROLLBACK
        )
        assert await StoreChecker(b.host.store).check(owned.references[0]) == Decision.COMMIT
        assert await b.host.archives.read_committed(message_id) == first
        passed("finish_retry_first_plan_and_old_abort")
        async with asyncio.timeout(60):
            while message_id not in b.messages:
                assert not b.failures
                await asyncio.sleep(0.05)
        assert b.messages[message_id] == hashlib.sha256(first.wire).hexdigest()
        after = await snapshot(b.host.kit, p.incoming.claim)
        assert after[2][0]["status"] == "done"
        if question_id is not None:
            assert after[4][0]["id"] == question_id
        await finish(p.incoming.claim, p.incoming.result)
        assert await snapshot(b.host.kit, p.incoming.claim) == after
        assert b.runtime.calls == 1 and len(b.host.executions) == 2
        finish._planner.candidate.assert_not_called()
        no_generation.assert_not_called()
        b.recorder.flush.assert_not_called()
        passed("finish_retry_duplicate_first_result")
        passed("finish_retry_no_new_provider")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def _serial_terminal_seed(b, ready, *, unknown=False, semantic=False):
    from qs_ai.domain.evaluation.failure import ClassifiedFailure
    from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
        EvaluationResponse,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
        SerialResponseReference,
        save_serial_response,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_step import prepare_step
    from qs_ai.infrastructure.workflow_transport.serial_generation_sender import (
        SerialGenerationSender,
    )
    from qs_ai.infrastructure.workflow_transport.serial_generation_state import (
        SerialGenerationPlanner,
    )
    from tests.integration.test_evaluation_step import AT, Gateway, step

    tx, run_id, _, routes, schemas = ready
    generation_gateway = Gateway(ready) if semantic else None
    version = (await step(ready, generation_gateway)).version if semantic else 3
    started = AT + timedelta(seconds=2) if semantic else AT
    prepared = await prepare_step(
        tx, run_id, version, 1, "worker:real-terminal", routes, schemas, clock=lambda: started
    )
    gateway = Gateway(ready)
    response = await gateway.generate_messages(
        prepared.messages, prepared.route, prepared.schema, prepared.invocation_id
    )
    failure = (
        ClassifiedFailure(
            "semantic_evaluation" if semantic else "generation_execution",
            "result_unknown",
            "provider_result_unknown",
            False,
            True,
            "manual_acknowledgement",
            "Original serial provider result unknown",
            (prepared.execution_id,),
        )
        if unknown
        else None
    )
    result = EvaluationResponse(
        None if unknown else response, failure, started + timedelta(seconds=1)
    )
    reference = SerialResponseReference.from_prepared(1, prepared)
    async with tx.open() as db:
        await db.begin()
        await save_serial_response(db, reference, result)
        await db.commit()
    if semantic:
        from qs_ai.infrastructure.workflow_transport.serial_semantic_sender import (
            SerialSemanticSender,
        )
        from qs_ai.infrastructure.workflow_transport.serial_semantic_state import (
            SerialSemanticPlanner,
        )

        planner = SerialSemanticPlanner(
            b.host.tx,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
        )
        sender = SerialSemanticSender(b.host.tx, b.host.archives, b.runtime, planner)
    else:
        planner = SerialGenerationPlanner(
            b.host.tx,
            routes,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
        )
        sender = SerialGenerationSender(b.host.tx, b.host.archives, b.runtime, planner)
    intent = await planner.intent(reference)
    return SimpleNamespace(**locals())


async def _clean_serial_terminal(p):
    from qs_ai.infrastructure.persistence.mysql.schema import evaluation_response_receipts

    async with p.tx.open() as db:
        await db.execute(
            delete(evaluation_response_receipts).where(
                evaluation_response_receipts.c.run_id == str(p.run_id)
            )
        )
        await db.commit()
    await clean_governance_sequence(p.b.host, p.run_id)


@pytest.mark.parametrize("outcome", ["success", "unknown"])
async def test_actual_serial_generation_terminal_half_gate_and_first_response(
    governance_broker, resolution_ready, monkeypatch, outcome
):
    from qs_ai.application.execution.generation import DurableGeneration
    from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
        read_serial_response,
    )
    from qs_ai.infrastructure.workflow_transport.serial_generation_state import (
        read_serial_generation_plan,
    )
    from tests.integration.test_evaluation_unknowns import snapshot

    b = governance_broker
    p = await _serial_terminal_seed(b, resolution_ready, unknown=outcome == "unknown")
    no_generation = AsyncMock(side_effect=AssertionError("Terminal sender cannot invoke provider"))
    monkeypatch.setattr(DurableGeneration, "execute", no_generation)
    before = await snapshot(p.tx, p.run_id)
    b.runtime.pause = True
    task = asyncio.create_task(p.sender(p.reference))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        facts, operation = read_serial_generation_plan(p.intent, owned.plan)
        _, first = read_evaluation_state_plan(p.intent, owned.plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        assert await snapshot(p.tx, p.run_id) == before
        passed("serial_completion_half_invisible_" + outcome)
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed serial terminal final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            state = await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        assert state.version == 6 and state.checkpoint is None and facts.source_version == 5
        passed("serial_completion_final_unknown_keeps_commit")
        await b.consumed(SimpleNamespace(intent=p.intent))
        after = await snapshot(p.tx, p.run_id)
        assert after[0][2]["version"] == 6
        assert after[0][0]["progress_json"]["status"] == (
            "blocked" if outcome == "unknown" else "collecting"
        )
        async with p.tx.open() as db:
            assert await read_serial_response(db, p.reference) == p.result
        assert bool(operation.candidate_id) == (outcome == "success")
        passed("serial_completion_first_response_candidate_wire_and_witness")
        p.planner.candidate = AsyncMock(
            side_effect=AssertionError("Duplicate must read original first result")
        )
        assert await p.sender(p.reference) == state
        assert await snapshot(p.tx, p.run_id) == after
        assert p.gateway.calls == b.runtime.calls == len(b.host.executions) == 1
        p.planner.candidate.assert_not_called()
        no_generation.assert_not_called()
        b.recorder.flush.assert_not_called()
        passed("serial_completion_duplicate_first_result_no_model")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await _clean_serial_terminal(p)


async def test_actual_serial_terminal_cancel_requires_explicit_old_abort_replan(
    governance_broker, resolution_ready
):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.infrastructure.workflow_transport.serial_generation_sender import (
        SerialGenerationPending,
    )
    from qs_ai.infrastructure.workflow_transport.serial_generation_state import (
        read_serial_generation_plan,
    )
    from tests.integration.test_evaluation_step import AT

    b = governance_broker
    p = await _serial_terminal_seed(b, resolution_ready)
    b.runtime.pause = True
    task = asyncio.create_task(p.sender(p.reference))
    next_task = None
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        previous = b.host.executions[-1]
        _, original_op = read_serial_generation_plan(p.intent, previous.plan)
        _, old_wire = read_evaluation_state_plan(p.intent, previous.plan)
        await MySQLEvaluationManagement(p.tx).cancel(
            ManagementScope(p.run_id, 1, 42),
            5,
            "Drain original response after frozen source",
            AT + timedelta(seconds=2),
            discard=False,
            confirm=True,
        )
        b.runtime.release.set()
        with pytest.raises(SerialGenerationPending):
            await task
        assert await StoreChecker(b.host.store).check(previous.references[0]) == Decision.ROLLBACK
        assert all(
            v.final == FinalOutcome.ROLLBACK_CONFIRMED for v in b.runtime.results[-1].branches
        )
        b.runtime.entered.clear()
        b.runtime.release.clear()
        next_task = asyncio.create_task(p.sender.replan(p.reference, previous))
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        current = b.host.executions[-1]
        facts, operation = read_serial_generation_plan(p.intent, current.plan)
        _, first = read_evaluation_state_plan(p.intent, current.plan)
        assert current.plan.digest != previous.plan.digest and facts.source_version == 6
        assert (
            operation.reference == original_op.reference
            and operation.response_hash == original_op.response_hash
        )
        assert operation.candidate_id == original_op.candidate_id
        passed("serial_completion_cancel_same_response_candidate")
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert (
            first.envelope().message_id not in b.messages
            and old_wire.envelope().message_id not in b.messages
        )
        passed("serial_completion_cancel_new_half_invisible")
        b.runtime.release.set()
        state = await next_task
        assert state.version == 7 and state.checkpoint is None
        await b.consumed(SimpleNamespace(intent=p.intent))
        assert await StoreChecker(b.host.store).check(previous.references[0]) == Decision.ROLLBACK
        assert await StoreChecker(b.host.store).check(current.references[0]) == Decision.COMMIT
        assert await p.sender(p.reference) == state
        assert p.gateway.calls == 1 and b.runtime.calls == len(b.host.executions) == 2
        assert old_wire.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("serial_completion_cancel_old_abort_and_new_plan")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, *([next_task] if next_task else []), return_exceptions=True)
        await _clean_serial_terminal(p)


@pytest.mark.parametrize("outcome", ["success", "unknown"])
async def test_actual_serial_semantic_terminal_half_gate_and_first_response(
    governance_broker, resolution_ready, monkeypatch, outcome
):
    from qs_ai.application.execution.generation import DurableGeneration
    from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
        read_serial_response,
    )
    from qs_ai.infrastructure.workflow_transport.serial_semantic_state import (
        read_serial_semantic_plan,
    )
    from tests.integration.test_evaluation_unknowns import snapshot

    b = governance_broker
    p = await _serial_terminal_seed(
        b, resolution_ready, unknown=outcome == "unknown", semantic=True
    )
    no_generation = AsyncMock(side_effect=AssertionError("Terminal sender cannot invoke provider"))
    monkeypatch.setattr(DurableGeneration, "execute", no_generation)
    before = await snapshot(p.tx, p.run_id)
    b.runtime.pause = True
    task = asyncio.create_task(p.sender(p.reference))
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        owned = b.host.executions[-1]
        facts, operation = read_serial_semantic_plan(p.intent, owned.plan)
        _, first = read_evaluation_state_plan(p.intent, owned.plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first.envelope().message_id not in b.messages
        assert await snapshot(p.tx, p.run_id) == before
        passed("semantic_completion_half_invisible_" + outcome)
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed serial terminal final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            state = await task
        result = b.runtime.results[-1]
        assert result.local == LocalOutcome.COMMITTED
        assert all(branch.final == FinalOutcome.UNKNOWN for branch in result.branches)
        assert state.version == 9 and state.checkpoint is None and facts.source_version == 8
        passed("semantic_completion_final_unknown_keeps_commit")
        await b.consumed(SimpleNamespace(intent=p.intent))
        after = await snapshot(p.tx, p.run_id)
        assert after[0][2]["version"] == 9
        assert after[0][0]["progress_json"]["status"] == (
            "blocked" if outcome == "unknown" else "collecting"
        )
        async with p.tx.open() as db:
            assert await read_serial_response(db, p.reference) == p.result
        assert operation.candidate_id == p.reference.checkpoint.candidate_id
        passed("semantic_completion_first_response_candidate_wire_and_witness")
        p.planner.candidate = AsyncMock(
            side_effect=AssertionError("Duplicate must read original first result")
        )
        assert await p.sender(p.reference) == state
        assert await snapshot(p.tx, p.run_id) == after
        assert p.gateway.calls == b.runtime.calls == len(b.host.executions) == 1
        p.planner.candidate.assert_not_called()
        no_generation.assert_not_called()
        b.recorder.flush.assert_not_called()
        passed("semantic_completion_duplicate_first_result_no_model")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await _clean_serial_terminal(p)


async def test_actual_serial_semantic_cancel_requires_explicit_old_abort_replan(
    governance_broker, resolution_ready
):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.infrastructure.workflow_transport.serial_semantic_sender import (
        SerialSemanticPending,
    )
    from qs_ai.infrastructure.workflow_transport.serial_semantic_state import (
        read_serial_semantic_plan,
    )
    from tests.integration.test_evaluation_step import AT

    b = governance_broker
    p = await _serial_terminal_seed(b, resolution_ready, semantic=True)
    b.runtime.pause = True
    task = asyncio.create_task(p.sender(p.reference))
    next_task = None
    try:
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        previous = b.host.executions[-1]
        _, original_op = read_serial_semantic_plan(p.intent, previous.plan)
        _, old_wire = read_evaluation_state_plan(p.intent, previous.plan)
        await MySQLEvaluationManagement(p.tx).cancel(
            ManagementScope(p.run_id, 1, 42),
            8,
            "Drain original semantic response after frozen source",
            AT + timedelta(seconds=4),
            discard=False,
            confirm=True,
        )
        b.runtime.release.set()
        with pytest.raises(SerialSemanticPending):
            await task
        assert await StoreChecker(b.host.store).check(previous.references[0]) == Decision.ROLLBACK
        assert all(
            v.final == FinalOutcome.ROLLBACK_CONFIRMED for v in b.runtime.results[-1].branches
        )
        b.runtime.entered.clear()
        b.runtime.release.clear()
        next_task = asyncio.create_task(p.sender.replan(p.reference, previous))
        await asyncio.wait_for(b.runtime.entered.wait(), timeout=30)
        current = b.host.executions[-1]
        facts, operation = read_serial_semantic_plan(p.intent, current.plan)
        _, first = read_evaluation_state_plan(p.intent, current.plan)
        assert current.plan.digest != previous.plan.digest and facts.source_version == 9
        assert (
            operation.reference == original_op.reference
            and operation.response_hash == original_op.response_hash
        )
        assert operation.candidate_id == original_op.candidate_id
        passed("semantic_completion_cancel_same_response_candidate")
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert (
            first.envelope().message_id not in b.messages
            and old_wire.envelope().message_id not in b.messages
        )
        passed("semantic_completion_cancel_new_half_invisible")
        b.runtime.release.set()
        state = await next_task
        assert state.version == 10 and state.checkpoint is None
        await b.consumed(SimpleNamespace(intent=p.intent))
        assert await StoreChecker(b.host.store).check(previous.references[0]) == Decision.ROLLBACK
        assert await StoreChecker(b.host.store).check(current.references[0]) == Decision.COMMIT
        assert await p.sender(p.reference) == state
        assert p.gateway.calls == 1 and b.runtime.calls == len(b.host.executions) == 2
        assert old_wire.envelope().message_id not in b.messages
        b.recorder.flush.assert_not_called()
        passed("semantic_completion_cancel_old_abort_and_new_plan")
    finally:
        b.runtime.release.set()
        await asyncio.gather(task, *([next_task] if next_task else []), return_exceptions=True)
        await _clean_serial_terminal(p)


@pytest.mark.parametrize("outcome", ["success", "unknown"])
@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_actual_candidate_graph_halves_first_response_and_final_unknown(
    governance_broker, resolution_ready, monkeypatch, outcome, kind
):
    from qs_ai.application.interpretation.provider import ProviderFailure
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
        read_candidate_response,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import active_claims
    from qs_ai.infrastructure.persistence.mysql.schema import (
        evaluation_response_receipts,
        evaluation_slot_claims,
    )
    from qs_ai.infrastructure.workflow_transport.candidate_completion_sender import (
        CandidateCompletionSender,
    )
    from qs_ai.infrastructure.workflow_transport.candidate_completion_state import (
        CandidateCompletionPlanner,
        read_candidate_completion_plan,
    )
    from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
        CandidateDispatchPending,
        CandidateDispatchSender,
    )
    from qs_ai.infrastructure.workflow_transport.candidate_dispatch_state import (
        CandidateDispatchPlanner,
        candidate_dispatch_intent,
        read_candidate_dispatch_plan,
    )
    from qs_ai.infrastructure.workflow_transport.candidate_runtime import CandidateRuntime
    from qs_ai.infrastructure.workflows.evaluation import execute_step
    from tests.integration.test_evaluation_candidate_execution import ConcurrentGateway
    from tests.integration.test_evaluation_step import AT
    from tests.integration.test_evaluation_unknowns import snapshot

    b = governance_broker
    tx, run_id, _, routes, schemas = resolution_ready
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(execution_mode="candidate_v2")
        )
        await db.commit()
    gateway = ConcurrentGateway(resolution_ready)
    gateway.release.set()
    version = 3
    if kind == "semantic":
        generated = await execute_step(
            tx,
            run_id,
            version,
            1,
            "worker:seed",
            gateway,
            routes,
            schemas,
            clock=lambda: AT,
            candidate_limit=3,
        )
        version = generated.version
    calls = len(gateway.calls)
    if outcome == "unknown":
        actual_generate = gateway.generate_messages

        async def unknown(*args, **kwargs):
            await actual_generate(*args, **kwargs)
            raise ProviderFailure("provider_timeout", retryable=True, result_unknown=True)

        gateway.generate_messages = unknown
    at = AT + timedelta(seconds=2)
    dispatch = CandidateDispatchSender(
        b.host.tx,
        b.host.archives,
        b.runtime,
        "worker:real-candidate",
        CandidateDispatchPlanner(
            b.host.tx,
            schemas,
            signing_key=b.host.keys["ai.sign"],
            recipient_key=b.host.keys["qs.encrypt"],
            limit=3,
            clock=lambda: at,
        ),
    )
    completion = CandidateCompletionSender(
        b.host.tx,
        b.host.archives,
        b.runtime,
        CandidateCompletionPlanner(
            b.host.tx, signing_key=b.host.keys["ai.sign"], recipient_key=b.host.keys["qs.encrypt"]
        ),
    )
    runtime = CandidateRuntime(dispatch, completion)
    b.runtime.pause = True
    original_execute = b.runtime.execute
    terminal_entered = asyncio.Event()
    closing = False

    async def phased(execution, local):
        if b.runtime.results and not closing:
            b.runtime.entered.clear()
            b.runtime.release.clear()
            terminal_entered.set()
        return await original_execute(execution, local)

    monkeypatch.setattr(b.runtime, "execute", phased)
    before = await snapshot(tx, run_id)
    task = asyncio.create_task(
        execute_step(
            b.host.tx,
            run_id,
            version,
            1,
            dispatch.owner,
            gateway,
            routes,
            schemas,
            clock=lambda: at + timedelta(seconds=1),
            candidate_limit=3,
            candidate_runtime=runtime,
        )
    )

    async def barrier(event):
        async with asyncio.timeout(30):
            while not event.is_set():
                if task.done():
                    await task  # Surface the original host error instead of a secondary timeout.
                    raise AssertionError("Candidate host ended before its original root barrier")
                await asyncio.sleep(0.05)

    try:
        await barrier(b.runtime.entered)
        intent = candidate_dispatch_intent(run_id, version, 1)
        _, _, first_dispatch = read_candidate_dispatch_plan(intent, b.host.executions[-1].plan)
        b.empty_poll.clear()
        await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
        assert first_dispatch.envelope().message_id not in b.messages
        assert await snapshot(tx, run_id) == before and len(gateway.calls) == calls
        passed("candidate_dispatch_half_invisible_" + kind + "_" + outcome)
        actual_end = b.sender._client.end_transaction

        def discard(*args, **kwargs):
            actual_end(*args, **kwargs)
            raise TimeoutError("client discarded completed candidate final reply")

        with monkeypatch.context() as patch:
            patch.setattr(b.sender._client, "end_transaction", discard)
            b.runtime.release.set()
            await barrier(terminal_entered)
            await barrier(b.runtime.entered)
            owned = b.host.executions[-1]
            # Decode the first frozen intent without rebuilding a candidate or wire.
            from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
                EvaluationStateFacts,
            )
            from qs_ai.infrastructure.workflow_transport.transaction_archive import decode_branch

            facts = EvaluationStateFacts.decode(decode_branch(owned.plan.branches[0]).context)
            _, operation = read_candidate_completion_plan(facts.intent, owned.plan)
            _, first = read_evaluation_state_plan(facts.intent, owned.plan)
            b.empty_poll.clear()
            await asyncio.wait_for(b.empty_poll.wait(), timeout=20)
            assert first.envelope().message_id not in b.messages and len(gateway.calls) == calls + 1
            current = await snapshot(tx, run_id)
            assert current[0][2]["version"] == version + 1
            async with tx.open() as db:
                claims = await active_claims(db, run_id, lock=False)
                response = await read_candidate_response(db, 1, claims[0])
            assert response is not None and response.reference == operation.reference
            assert bool(operation.candidate_id) == (outcome == "success" or kind == "semantic")
            passed("candidate_completion_half_invisible_" + kind + "_" + outcome)
            b.runtime.release.set()
            state = await task
        assert state.version == version + 2 and state.checkpoint is None
        assert b.runtime.calls == len(b.host.executions) == len(b.runtime.results) == 2
        assert all(result.local == LocalOutcome.COMMITTED for result in b.runtime.results)
        assert all(
            branch.final == FinalOutcome.UNKNOWN
            for result in b.runtime.results
            for branch in result.branches
        )
        passed("candidate_final_unknown_preserves_dispatch_and_terminal_commit")
        await b.consumed(SimpleNamespace(intent=intent))
        await b.consumed(SimpleNamespace(intent=facts.intent))
        assert await completion(response.reference) == state
        with pytest.raises(CandidateDispatchPending):
            await dispatch(run_id, version, 1)
        assert len(gateway.calls) == calls + 1 and b.runtime.calls == 2
        passed("candidate_duplicate_never_reinvokes_provider_or_reseals")
        after = await snapshot(tx, run_id)
        assert after[0][0]["progress_json"]["status"] == (
            "blocked" if outcome == "unknown" else "collecting"
        )
        async with tx.open() as db:
            assert not await active_claims(db, run_id, lock=False)
        b.recorder.flush.assert_not_called()
        passed("candidate_original_roots_first_response_archive_and_witness")
    finally:
        closing = True
        b.runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)
        async with tx.open() as db:
            for table in (evaluation_response_receipts, evaluation_slot_claims):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()
        await clean_governance_sequence(b.host, run_id)
