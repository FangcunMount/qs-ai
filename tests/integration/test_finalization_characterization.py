"""T18-A actual worker/MySQL characterization; controlled MQ and in-process model only."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import LocalOutcome, Request, Result, State
from sqlalchemy import select

from qs_ai.application.execution.configuration import PublishedReportWorkflow
from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    MySQLExecutionConfigurations,
)
from qs_ai.infrastructure.persistence.mysql.schema import artifacts, jobs, model_calls, sessions
from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
    InterpretationFinishPending,
)
from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import (
    InterpretationFinishInput,
    finish_request_id,
)
from qs_ai.infrastructure.workflow_transport.transaction_events import TransactionOnlyEvents
from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
    TransactionalInterpretationExecutionStore,
)
from qs_ai.infrastructure.workflows.report import create_report_workflow
from tests.integration import test_interpretation
from tests.integration.test_generation import Gateway
from tests.integration.test_interpretation import expire
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys
from tests.test_input_binding import bound_case
from tests.test_output_validation import candidate

pytestmark = pytest.mark.integration


@pytest.fixture
async def kit(published_configuration):
    # Participant configurations must be removed before their publication parents.
    async for value in test_interpretation.kit.__wrapped__():
        yield value


class OriginalRootRuntime:
    """Preserve actual SQL roots, explicitly abort the first terminal without a Broker."""

    def __init__(self, host):
        self.host, self.store = host, host.store
        self.terminal_attempts = []
        self.terminal_refusals = 1
        self.abort_terminal = True

    def stats(self):
        return {"ready": True}

    async def reserve(self, request, plan):
        owned = await self.store.reserve(request, plan)
        if owned.acquired:
            self.host.executions.append(owned.execution)
        return owned

    async def retry(self, authorization):
        owned = await self.store.retry(authorization)
        if owned.acquired:
            self.host.executions.append(owned.execution)
        return owned

    async def execute(self, owned, local):
        if "/finish/" in owned.plan.idempotency_key:
            self.terminal_attempts.append(owned)
            if len(self.terminal_attempts) <= self.terminal_refusals:
                if self.abort_terminal:
                    assert await self.store.abort(owned)
                raise TransactionSendError(
                    "controlled terminal preparation refusal", Result(LocalOutcome.NOT_STARTED, ())
                )
        return Result(await local(owned), ())


@pytest.fixture
async def original_worker(host, monkeypatch):
    _, source, _ = bound_case()
    receipt = await host.kit.service.start_external(
        host.kit.actor, "7", ("42",), "goal", str(uuid4()), source.items
    )
    runtime = OriginalRootRuntime(host)
    adapter = TransactionalInterpretationExecutionStore(
        host.kit.store,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )

    class ValidGateway(Gateway):
        async def generate(self, *args):
            response = await super().generate(*args)
            return replace(
                response,
                raw_output=json.dumps(candidate()),
                validation_output=json.dumps(candidate()),
            )

    gateway = ValidGateway()
    workflow = PublishedReportWorkflow(
        MySQLExecutionConfigurations(host.tx),
        DurableGeneration(host.kit.store, gateway, JSONModelCallCodec()),
        create_report_workflow,
    )
    finish = AsyncMock(wraps=adapter.finish)
    monkeypatch.setattr(adapter, "finish", finish)
    worker = ExecuteNext(adapter, host.kit.source, workflow)
    host.tx.database.state_events = TransactionOnlyEvents()
    try:
        yield SimpleNamespace(**locals())
    finally:
        host.tx.database.state_events = None


async def terminal_snapshot(p):
    claim, _ = p.finish.await_args.args
    async with p.host.tx.open() as db:
        call = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
            .mappings()
            .one()
        )
        job = (await db.execute(select(jobs).where(jobs.c.id == claim.job_id))).mappings().one()
        session = (
            (await db.execute(select(sessions).where(sessions.c.id == claim.session.id)))
            .mappings()
            .one()
        )
        artifact = await db.scalar(select(artifacts.c.id).where(artifacts.c.run_id == claim.run_id))
    return dict(call=call, job=job, session=session, artifact=artifact)


async def first_terminal_refusal(p, *, state=State.ABORTED):
    with pytest.raises(TransactionSendError):
        await p.worker.once(60)
    before = await terminal_snapshot(p)
    assert p.gateway.calls == 1 and before["call"]["status"] == "response_received"
    assert before["artifact"] is None and before["job"]["status"] == "leased"
    assert before["session"]["status"] == "running"
    assert len(p.runtime.terminal_attempts) == 1
    claim, result = p.finish.await_args.args
    assert result.artifact is not None
    incoming = InterpretationFinishInput(claim, result)
    request = incoming.intent(await finish_request_id(p.host.kit.store, incoming)).request
    previous = await p.runtime.store.lookup(request)
    assert previous.state == state
    return before, previous


async def test_original_worker_live_claim_ordinary_finish_leaves_aborted_terminal_unresolved(
    original_worker,
):
    p = original_worker
    before, previous = await first_terminal_refusal(p)
    args = p.finish.await_args.args
    with pytest.raises(InterpretationFinishPending):
        await p.adapter._finish(*args)  # Bare sender retains explicit retry semantics.
    assert await terminal_snapshot(p) == before
    assert p.gateway.calls == 1 and len(p.runtime.terminal_attempts) == 1
    plan = previous.execution.plan
    request = Request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash)
    assert (await p.runtime.store.lookup(request)).state == State.ABORTED


async def test_original_worker_new_lease_finishes_with_original_response(original_worker):
    p = original_worker
    before, previous = await first_terminal_refusal(p)
    old_claim, old_result = p.finish.await_args.args
    await expire(p.host.kit, old_claim.session.id)
    assert await p.worker.once(60)
    after = await terminal_snapshot(p)
    claim, result = p.finish.await_args.args
    assert claim.fence > old_claim.fence and claim.session.version == old_claim.session.version
    assert after["call"] == before["call"] and result.artifact == old_result.artifact
    assert after["artifact"] == old_result.artifact.id
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert after["session"]["version"] == before["session"]["version"] + 1
    assert p.gateway.calls == 1 and len(p.runtime.terminal_attempts) == 2
    assert p.runtime.terminal_attempts[-1].plan == previous.execution.plan
