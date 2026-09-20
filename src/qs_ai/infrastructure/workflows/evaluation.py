"""A graph per durable evaluation attempt; existing CAS owns recovery."""

from collections.abc import Callable
from datetime import datetime
from typing import TypedDict
from uuid import UUID

from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.application.evaluation.provider_failure import classify_provider_failure
from qs_ai.application.interpretation.provider import ModelResponse, ProviderFailure
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_step import (
    MessagesGateway,
    PreparedStep,
    finish_step,
    prepare_step,
)


class EvaluationState(TypedDict, total=False):
    prepared: PreparedStep
    response: ModelResponse | None
    failure: ClassifiedFailure | None
    result: CheckpointState


async def run_evaluation_step(
    transactions: Transactions,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    gateway: MessagesGateway,
    routes: RouteAssets,
    schemas: SchemaAssets,
    *,
    clock: Callable[[], datetime],
) -> CheckpointState:
    async def prepare(state: EvaluationState) -> EvaluationState:
        value = await prepare_step(
            transactions,
            run_id,
            expected_version,
            organization_id,
            owner,
            routes,
            schemas,
            clock=clock,
        )
        return {"prepared": value}

    async def invoke(state: EvaluationState) -> EvaluationState:
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
            clock=clock,
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
    with tracing_context(enabled=False):
        result = await graph.compile().ainvoke({})
    return result["result"]
