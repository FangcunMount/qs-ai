"""A graph per durable evaluation attempt; existing CAS owns recovery."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TypedDict
from uuid import UUID

from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.provider_failure import classify_provider_failure
from qs_ai.application.execution.model_capacity import CapacityToken, ModelCapacity
from qs_ai.application.interpretation.provider import (
    MessagesGateway,
    ModelResponse,
    ProviderFailure,
)
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.application.operations.diagnostics import operation
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    save_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import lock_run, renew_claim
from qs_ai.infrastructure.persistence.mysql.evaluation_step import (
    PreparedStep,
    finish_step,
    prepare_step,
)
from qs_ai.infrastructure.workflow_transport.candidate_runtime import CandidateRuntime
from qs_ai.infrastructure.workflow_transport.serial_runtime import SerialRuntime


class EvaluationState(TypedDict, total=False):
    prepared: PreparedStep
    response: ModelResponse | None
    failure: ClassifiedFailure | None
    result: CheckpointState


async def execute_step(
    transactions: Transactions,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    gateway: MessagesGateway,
    routes: RouteAssets,
    schemas: SchemaAssets,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    candidate_limit: int | None = None,
    capacity: ModelCapacity | None = None,
    prepare_dispatch: Callable[[UUID, int, int], Awaitable[PreparedStep]] | None = None,
    candidate_runtime: CandidateRuntime | None = None,
    serial_runtime: SerialRuntime | None = None,
) -> CheckpointState:
    if serial_runtime is not None and (
        serial_runtime.transactions is not transactions
        or candidate_limit is not None
        or candidate_runtime is not None
        or prepare_dispatch is not None
    ):
        raise ValueError("Serial runtime must retain its original host and complete paired graph")
    if prepare_dispatch is not None and candidate_limit is not None:
        raise ValueError("Serial transaction dispatcher cannot own candidate claims")
    if candidate_runtime is not None and (
        candidate_limit is None
        or candidate_limit != candidate_runtime.limit
        or candidate_runtime.transactions is not transactions
    ):
        raise ValueError("Candidate runtime must retain the original host and concurrency budget")
    heartbeat: asyncio.Task[None] | None = None
    token: CapacityToken | None = None

    async def renew(prepared: PreparedStep) -> None:
        assert prepared.claim is not None
        while True:
            await asyncio.sleep(30)
            at = clock()
            if candidate_runtime is not None:
                await candidate_runtime.renew(
                    organization_id, prepared.claim, at, at + timedelta(minutes=5)
                )
                continue
            async with transactions.open() as db:
                await lock_run(db, run_id, organization_id)
                await renew_claim(db, prepared.claim, at, at + timedelta(minutes=5))
                await db.commit()

    async def prepare(state: EvaluationState) -> EvaluationState:
        nonlocal heartbeat, token
        with operation("graph.evaluation.prepare", "evaluation"):
            if candidate_runtime is not None:
                value = await candidate_runtime.prepare(run_id, expected_version, organization_id)
            elif serial_runtime is not None:
                value = await serial_runtime.prepare(run_id, expected_version, organization_id)
            elif prepare_dispatch is None:
                value = await prepare_step(
                    transactions,
                    run_id,
                    expected_version,
                    organization_id,
                    owner,
                    routes,
                    schemas,
                    clock=clock,
                    candidate_limit=candidate_limit,
                    capacity=capacity,
                )
            else:
                value = await prepare_dispatch(run_id, expected_version, organization_id)
            token = value.capacity_token
            if candidate_runtime is not None and (
                value.claim is None
                or value.state.run_id != run_id
                or value.claim.run_id != run_id
                or value.state.version != expected_version + 1
                or value.claim.version != value.state.version
                or value.cp != value.state.checkpoint
                or value.cp != value.claim.checkpoint
                or value.cp.owner != owner
                or value.cp.phase != "dispatching"
                or value.cp.dispatch_started_at != value.at
                or (value.execution_id, value.invocation_id)
                != (value.cp.execution_id, value.cp.invocation_id)
            ):
                raise CheckpointConflict("Original candidate dispatch handoff differs from worker")
            if (prepare_dispatch is not None or serial_runtime is not None) and (
                value.state.run_id != run_id
                or value.state.version != expected_version + 2
                or value.claim is not None
                or value.cp.owner != owner
                or value.cp.phase != "prepared"
                or value.state.checkpoint != value.cp.mark_dispatching(owner, value.at)
                or (value.execution_id, value.invocation_id)
                != (value.cp.execution_id, value.cp.invocation_id)
            ):
                raise CheckpointConflict("Original serial dispatch handoff differs from worker")
            if value.claim is not None:
                heartbeat = asyncio.create_task(renew(value))
            return {"prepared": value}

    async def invoke(state: EvaluationState) -> EvaluationState:
        with operation("graph.evaluation.invoke", "evaluation"):
            prepared = state["prepared"]
            try:
                response = await gateway.generate_messages(
                    prepared.messages,
                    prepared.route,
                    prepared.schema,
                    prepared.invocation_id,
                )
            except ProviderFailure as error:
                return {
                    "response": None,
                    "failure": classify_provider_failure(
                        prepared.cp.kind,
                        prepared.execution_id,
                        error,
                    ),
                }
            return {"response": response, "failure": None}

    async def complete(state: EvaluationState) -> EvaluationState:
        with operation("graph.evaluation.complete", "evaluation"):
            prepared = state["prepared"]
            if heartbeat is not None and heartbeat.done():
                heartbeat.result()
            finished = max(clock(), prepared.at)
            response = EvaluationResponse(state["response"], state["failure"], finished)
            if serial_runtime is not None:
                await serial_runtime.save_response(organization_id, prepared, response)
                result = await serial_runtime.complete(organization_id, prepared, response)
                return {"result": result}
            if prepared.claim is not None:
                if candidate_runtime is not None:
                    await candidate_runtime.save_response(
                        organization_id, prepared, response, clock()
                    )
                else:
                    async with transactions.open() as db:
                        await save_response(
                            db,
                            organization_id,
                            prepared.claim,
                            response,
                            at=clock(),
                        )
                        await db.commit()
            if candidate_runtime is not None:
                result = await candidate_runtime.complete(organization_id, prepared, response)
                return {"result": result}
            result = await finish_step(
                transactions,
                run_id,
                organization_id,
                owner,
                routes,
                schemas,
                state["prepared"],
                state["response"],
                state["failure"],
                clock=lambda: finished,
            )
            return {"result": result}

    graph = StateGraph(EvaluationState)
    graph.add_node("prepare_dispatch", prepare)
    graph.add_node("invoke_model", invoke)
    graph.add_node("commit_receipt", complete)
    graph.add_edge(START, "prepare_dispatch")
    graph.add_edge("prepare_dispatch", "invoke_model")
    graph.add_edge("invoke_model", "commit_receipt")
    graph.add_edge("commit_receipt", END)
    try:
        with tracing_context(enabled=False):
            result = await graph.compile().ainvoke({})
        return result["result"]
    finally:
        try:
            if heartbeat is not None:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat
        finally:
            if token is not None:
                token.release()
