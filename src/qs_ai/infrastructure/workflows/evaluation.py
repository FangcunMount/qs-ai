"""A graph per durable evaluation attempt; existing CAS owns recovery."""

import asyncio
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TypedDict
from uuid import UUID

from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.application.evaluation.provider_failure import classify_provider_failure
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
) -> CheckpointState:
    heartbeat: asyncio.Task[None] | None = None

    async def renew(prepared: PreparedStep) -> None:
        assert prepared.claim is not None
        while True:
            await asyncio.sleep(30)
            at = clock()
            async with transactions.open() as db:
                await lock_run(db, run_id, organization_id)
                await renew_claim(db, prepared.claim, at, at + timedelta(minutes=5))
                await db.commit()

    async def prepare(state: EvaluationState) -> EvaluationState:
        nonlocal heartbeat
        with operation("graph.evaluation.prepare", "evaluation"):
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
            )
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
            if prepared.claim is not None:
                async with transactions.open() as db:
                    await save_response(
                        db,
                        organization_id,
                        prepared.claim,
                        EvaluationResponse(state["response"], state["failure"], finished),
                        at=clock(),
                    )
                    await db.commit()
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
        if heartbeat is not None:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
