"""Actual Create handler/primary database and original replay; no Broker claim."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import delete

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.execution_mode import ExecutionMode
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.evaluation.requests import EvaluationRequests
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_requests import MySQLEvaluationRequests
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_requests import (
    TransactionalEvaluationRequests,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_grpc_commands import Aborted, Context

pytestmark = pytest.mark.integration


@pytest.fixture
async def creation(host, setup_run, assets, complete_release):
    _, run_id, _ = setup_run
    creator = MySQLRunCreator(host.tx, *assets[0])
    scope = ManagementScope(run_id, 1, 42)

    class DatabaseOnlyRuntime:
        store = host.store
        calls = 0
        reject = False

        async def reserve(self, request, candidate):
            value = await self.store.reserve(request, candidate)
            if value.acquired:
                host.executions.append(value.execution)
            return value

        async def execute(self, execution, local):
            if self.reject:
                raise TransactionSendError(
                    "isolated_half_refused", Result(LocalOutcome.NOT_STARTED, ())
                )
            self.calls += 1
            return Result(await local(execution), ())

    runtime = DatabaseOnlyRuntime()
    sender = TransactionalEvaluationRequests(
        creator,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    try:
        yield SimpleNamespace(**locals())
    finally:
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
            )


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
@pytest.mark.parametrize("reject", [False, True])
async def test_actual_create_rpc_requires_preparation_and_replays_first_mode_after_progress(
    creation, mode, reject
):
    from qs_ai.transport.grpc.evaluation import EvaluationManagement

    p = creation
    p.creator.execution_mode = ExecutionMode(mode)
    p.runtime.reject = reject

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationRequests
                return p.sender

        yield Scope()

    service = EvaluationManagement(container)
    request = pb.EvaluationCreateCommand(
        scope=pb.EvaluationQuery(run_id=str(p.run_id), organization_id=1, operator_user_id=42),
        release=pb.EvaluationRelease(**asdict(p.complete_release)),
        reason=" 原创建意图 ",
        confirm=True,
    )
    if reject:
        with pytest.raises(Aborted):
            await service.Create(request, Context())
        assert p.runtime.calls == 0 and await rows(p.host.tx, p.run_id) == [None, None, None]
        assert (
            await StoreChecker(p.host.store).check(p.host.executions[-1].references[0])
            == Decision.UNKNOWN
        )
        return
    first = await service.Create(request, Context())
    assert (first.version, first.status, first.execution_mode) == (1, "requested", mode)
    assert p.runtime.calls == 1
    assert (
        await StoreChecker(p.host.store).check(p.host.executions[-1].references[0])
        == Decision.COMMIT
    )
    started = await MySQLEvaluationManagement(p.host.tx).start(
        p.scope,
        1,
        "explicit original start",
        datetime.now(UTC),
        confirm=True,
    )
    original = await rows(p.host.tx, p.run_id)
    p.creator.execution_mode = ExecutionMode("candidate_v2" if mode == "serial_v1" else "serial_v1")
    again = await service.Create(request, Context())
    assert (again.version, again.status, again.execution_mode) == (
        started.version,
        started.status,
        mode,
    )
    assert again.creation_json == first.creation_json == started.creation_json
    assert await rows(p.host.tx, p.run_id) == original
    assert p.runtime.calls == len(p.host.executions) == 1


async def test_legacy_creation_replay_does_not_prepare_halves_or_adopt_witness(creation):
    p = creation
    legacy = MySQLEvaluationRequests(p.host.tx, p.creator)
    at = datetime(2026, 10, 21, tzinfo=UTC)
    first = await legacy.create(p.scope, p.complete_release, "原创建意图", at, confirm=True)
    before = await rows(p.host.tx, p.run_id)
    p.creator.execution_mode = ExecutionMode("candidate_v2")
    replay = await p.sender.create(
        p.scope, p.complete_release, "原创建意图", at + timedelta(hours=1), confirm=True
    )
    assert replay == first and await rows(p.host.tx, p.run_id) == before
    assert p.runtime.calls == 0 and not p.host.executions


@pytest.mark.parametrize("conflict", ["actor", "org", "reason", "release"])
@pytest.mark.parametrize("legacy", [False, True])
async def test_creation_conflict_preserves_first_fact_and_never_executes_again(
    creation, conflict, legacy
):
    p = creation
    at = datetime(2026, 10, 21, tzinfo=UTC)
    sender = MySQLEvaluationRequests(p.host.tx, p.creator) if legacy else p.sender
    await sender.create(p.scope, p.complete_release, "原创建意图", at, confirm=True)
    before = await rows(p.host.tx, p.run_id)
    scope, release, reason = p.scope, p.complete_release, "原创建意图"
    if conflict == "actor":
        scope = replace(scope, operator_user_id=43)
    elif conflict == "org":
        scope = replace(scope, organization_id=2)
    elif conflict == "reason":
        reason = "不同创建意图"
    else:
        release = replace(release, prompt=replace(release.prompt, fingerprint="sha256:" + "f" * 64))
    with pytest.raises(CheckpointConflict):
        await p.sender.create(scope, release, reason, at + timedelta(seconds=1), confirm=True)
    assert await rows(p.host.tx, p.run_id) == before
    assert p.runtime.calls == len(p.host.executions) == int(not legacy)


async def test_concurrent_creation_has_one_executor_and_keeps_durable_first_plan(creation):
    from qs_ai.infrastructure.workflow_transport.transactional_evaluation_requests import (
        CreationTransactionPending,
    )

    p = creation
    at = datetime(2026, 10, 21, tzinfo=UTC)
    results = await asyncio.gather(
        *(
            p.sender.create(
                p.scope, p.complete_release, "原创建意图", at + timedelta(seconds=i), confirm=True
            )
            for i in range(2)
        ),
        return_exceptions=True,
    )
    assert any(not isinstance(value, Exception) for value in results)
    assert all(
        not isinstance(value, Exception) or isinstance(value, CreationTransactionPending)
        for value in results
    )
    assert p.runtime.calls == len(p.host.executions) == 1
    owned = p.host.executions[0]
    assert await StoreChecker(p.host.store).check(owned.references[0]) == Decision.COMMIT
    first = await rows(p.host.tx, p.run_id)
    replay = await p.sender.create(
        p.scope, p.complete_release, "原创建意图", at + timedelta(hours=1), confirm=True
    )
    assert replay.version == 1 and replay.status == "requested"
    assert await rows(p.host.tx, p.run_id) == first and p.runtime.calls == 1
