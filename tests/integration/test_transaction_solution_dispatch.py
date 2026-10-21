"""Actual Prepare handler and primary database; DB-only transport is not Broker evidence."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import grpc
import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import delete, text

from qs_ai.application.governance.solutions import PrepareSolution, SolutionStore
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport import solution_preparation_state
from qs_ai.infrastructure.workflow_transport.solution_preparation_dispatch import (
    PreparationTransactionPending,
    SolutionPreparationDispatcher,
)
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationInput,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.transactional_solutions import TransactionalSolutions
from qs_ai.transport.grpc.solutions import SolutionManagement
from qs_ai.transport.grpc.solutions import response as solution_response
from tests.integration import test_transaction_solution_preparation as preparation_tests
from tests.integration.test_evaluation_runs import rows
from tests.test_grpc_commands import Aborted, Context

pytestmark = pytest.mark.integration

# Keep the actual native dependency graph explicit; no fixture may silently skip.
assets = preparation_tests.assets
complete_release = preparation_tests.complete_release
evaluation_release = preparation_tests.evaluation_release
host = preparation_tests.host
keys = preparation_tests.keys
kit = preparation_tests.kit
persisted_assets = preparation_tests.persisted_assets
required_disposable_primary = preparation_tests.required_disposable_primary
setup_run = preparation_tests.setup_run
workspace = preparation_tests.workspace
trigger_admin = preparation_tests.trigger_admin


@pytest.fixture
async def sending(host, workspace):
    _, previous, scope, solution_id, create, at = workspace
    original = MySQLSolutions(host.tx, previous.settings)

    class DatabaseOnlyRuntime:
        store = host.store
        calls = 0
        reject = False
        final_unknown = False

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
            result = Result(await local(execution), ())
            if self.final_unknown:
                raise TransactionSendError("isolated_final_unknown", result)
            return result

    runtime = DatabaseOnlyRuntime()
    dispatch = SolutionPreparationDispatcher(
        original,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    sender = TransactionalSolutions(original, dispatch)
    # Create delegates the original writer and must not manufacture an event.
    initial = await sender.apply(scope, solution_id, create, at)
    assert not host.executions and runtime.calls == 0
    command = PrepareSolution(
        command_id=uuid4(), expected_revision=initial["revision"], reason="原准备意图"
    )
    incoming = SolutionPreparationInput(scope, solution_id, command)
    try:
        yield SimpleNamespace(**locals())
    finally:
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(
                    evaluation_sequences.c.run_id == str(incoming.identities.run_id)
                )
            )


def service_request(p):
    from qs_ai.contracts.workflow import workflow_pb2 as pb

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is SolutionStore
                return p.sender

        yield Scope()

    return SolutionManagement(container), pb.SolutionWrite(
        scope=pb.PublicationScope(**asdict(p.scope)),
        solution_id=str(p.solution_id),
        command_json=json.dumps(asdict(p.command), default=str),
    )


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
@pytest.mark.parametrize("reject", [False, True])
async def test_actual_prepare_handler_requires_half_preparation_before_original_business(
    sending, mode, reject
):
    p = sending
    p.original.settings = p.original.settings.model_copy(
        update={
            "evaluation": p.original.settings.evaluation.model_copy(
                update={"candidate_mode_enabled": mode == "candidate_v2"}
            )
        }
    )
    p.runtime.reject = reject
    service, request = service_request(p)
    if reject:
        with pytest.raises(Aborted) as failure:
            await service.Prepare(request, Context())
        assert failure.value.args[0] == grpc.StatusCode.UNAVAILABLE
        assert p.runtime.calls == 0
        assert await p.original.get(p.scope, p.solution_id) == p.initial
        assert await rows(p.host.tx, p.incoming.identities.run_id) == [None, None, None]
        assert await StoreChecker(p.host.store).check(p.host.executions[0].references[0]) == (
            Decision.UNKNOWN
        )
        return
    response = await service.Prepare(request, Context())
    first = json.loads(response.data_json)
    assert first["revision"] == p.initial["revision"] + 1
    run = (await rows(p.host.tx, p.incoming.identities.run_id))[0]
    assert run["execution_mode"] == mode
    assert p.runtime.calls == len(p.host.executions) == 1
    assert await StoreChecker(p.host.store).check(p.host.executions[0].references[0]) == (
        Decision.COMMIT
    )
    # Domain content retains immutable tuples; the existing RPC JSON boundary
    # deliberately encodes them as arrays. Compare the actual wire contract.
    assert solution_response(await p.original.receipt(p.scope, p.command.command_id)) == response


@pytest.mark.parametrize("legacy", [False, True])
async def test_replay_reads_first_receipt_without_new_seal_mode_or_business(
    sending, monkeypatch, legacy
):
    p = sending
    writer = p.original if legacy else p.sender
    first = await writer.apply(p.scope, p.solution_id, p.command, p.at)
    original = await rows(p.host.tx, p.incoming.identities.run_id)
    p.original.settings = p.original.settings.model_copy(
        update={
            "evaluation": p.original.settings.evaluation.model_copy(
                update={"candidate_mode_enabled": True}
            )
        }
    )
    monkeypatch.setattr(
        solution_preparation_state,
        "prepare",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("No new Seal on original command replay")
        ),
    )
    again = await p.sender.apply(p.scope, p.solution_id, p.command, p.at + timedelta(hours=1))
    assert again == first
    assert await rows(p.host.tx, p.incoming.identities.run_id) == original
    assert p.runtime.calls == len(p.host.executions) == int(not legacy)


@pytest.mark.parametrize("change", ["actor", "organization", "reason"])
@pytest.mark.parametrize("legacy", [False, True])
async def test_reused_command_preserves_original_scope_intent_and_receipt(sending, change, legacy):
    p = sending
    writer = p.original if legacy else p.sender
    first = await writer.apply(p.scope, p.solution_id, p.command, p.at)
    original = await rows(p.host.tx, p.incoming.identities.run_id)
    scope, command = p.scope, p.command
    if change == "actor":
        scope = replace(scope, operator_user_id=scope.operator_user_id + 1)
    elif change == "organization":
        scope = replace(scope, organization_id=scope.organization_id + 1)
    else:
        command = replace(command, reason="不同准备意图")
    with pytest.raises(DraftConflict if change == "reason" else NotFound):
        await p.sender.apply(scope, p.solution_id, command, p.at + timedelta(seconds=1))
    assert await p.original.receipt(p.scope, p.command.command_id) == first
    assert await rows(p.host.tx, p.incoming.identities.run_id) == original
    assert p.runtime.calls == len(p.host.executions) == int(not legacy)


async def test_final_unknown_returns_proved_local_commit_without_another_execution(sending):
    p = sending
    p.runtime.final_unknown = True
    first = await p.sender.apply(p.scope, p.solution_id, p.command, p.at)
    assert await p.sender.apply(p.scope, p.solution_id, p.command, p.at) == first
    assert p.runtime.calls == len(p.host.executions) == 1
    assert await StoreChecker(p.host.store).check(p.host.executions[0].references[0]) == (
        Decision.COMMIT
    )


async def test_prepared_protocol_does_not_adopt_a_later_legacy_receipt(sending):
    p = sending
    p.runtime.reject = True
    with pytest.raises(TransactionSendError):
        await p.sender.apply(p.scope, p.solution_id, p.command, p.at)
    legacy = await p.original.apply(p.scope, p.solution_id, p.command, p.at)
    with pytest.raises(PreparationTransactionPending):
        await p.sender.apply(p.scope, p.solution_id, p.command, p.at)
    assert await p.original.receipt(p.scope, p.command.command_id) == legacy
    assert p.runtime.calls == 0 and len(p.host.executions) == 1
    assert await StoreChecker(p.host.store).check(p.host.executions[0].references[0]) == (
        Decision.UNKNOWN
    )


async def test_concurrent_original_command_has_one_business_executor_and_one_first_plan(sending):
    p = sending
    results = await asyncio.gather(
        *(
            p.sender.apply(p.scope, p.solution_id, p.command, p.at + timedelta(seconds=i))
            for i in range(2)
        ),
        return_exceptions=True,
    )
    assert any(not isinstance(value, Exception) for value in results)
    assert all(
        not isinstance(value, Exception) or isinstance(value, PreparationTransactionPending)
        for value in results
    )
    assert p.runtime.calls == len(p.host.executions) == 1
    first = await p.original.receipt(p.scope, p.command.command_id)
    assert await p.sender.apply(p.scope, p.solution_id, p.command, p.at) == first
    assert p.runtime.calls == 1


@pytest.mark.parametrize("fault", ["archive_rejected", "witness_rejected"])
async def test_actual_handler_does_not_return_cached_response_after_original_root_rollback(
    sending, trigger_admin, fault
):
    p = sending
    trigger = "solution_sender_fault_" + uuid4().hex
    statement = (
        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
        "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated sender archive rejection'"
        if fault == "archive_rejected"
        else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
        "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT='isolated sender witness rejection'; END IF; END"
    )
    async with trigger_admin.begin() as db:
        await db.execute(text(statement))
    try:
        service, request = service_request(p)
        with pytest.raises(Aborted) as failure:
            await service.Prepare(request, Context())
        assert failure.value.args[0] == grpc.StatusCode.UNAVAILABLE
        assert p.runtime.calls == len(p.host.executions) == 1
        assert await StoreChecker(p.host.store).check(p.host.executions[0].references[0]) == (
            Decision.ROLLBACK
        )
        assert await p.original.get(p.scope, p.solution_id) == p.initial
        assert await rows(p.host.tx, p.incoming.identities.run_id) == [None, None, None]
        with pytest.raises(NotFound):
            await p.original.receipt(p.scope, p.command.command_id)
    finally:
        async with trigger_admin.begin() as db:
            await db.execute(text(f"DROP TRIGGER {trigger}"))
