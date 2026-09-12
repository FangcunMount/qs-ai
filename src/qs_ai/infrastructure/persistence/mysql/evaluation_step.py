"""One durable evaluation step; no automatic replay of an active dispatch checkpoint."""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import select

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.application.evaluation.model_response import response_evidence
from qs_ai.application.evaluation.provider_failure import classify_provider_failure
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute, ProviderFailure
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import (
    prepare_run_case,
    run_model_route,
    stored_run_suite,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import (
    complete_evaluated_generation,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import reserve_dispatch
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import complete_semantic
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_runs,
)
from qs_ai.infrastructure.qs_server.evaluation_assertions import (
    assertion_inventory,
    semantic_obligations,
)
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from qs_ai.infrastructure.qs_server.semantic_input import prepare_semantic_messages
from qs_ai.infrastructure.qs_server.semantic_output import (
    SemanticDecisionInvalid,
    parse_semantic_output,
)


class MessagesGateway(Protocol):
    async def generate_messages(
        self,
        messages: PromptMessages,
        route: ModelRoute,
        schema: dict[str, Any],
        invocation_id: str,
    ) -> ModelResponse: ...


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
) -> CheckpointState:
    """Run must already be collecting with passed preflight; caller supplies authorized scope."""
    at = clock()
    invocation_id, execution_id = str(uuid4()), str(uuid4())
    candidate_fingerprint = ""
    assertions: tuple[AssertionReceipt, ...] = ()
    async with transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        state = await prepare_execution(
            db,
            run_id,
            expected_version,
            organization_id,
            owner,
            execution_id,
            invocation_id,
            at,
            at + timedelta(minutes=5),
        )
        cp = state.checkpoint
        assert cp is not None
        creation = json.loads(
            (
                await db.execute(
                    select(evaluation_runs.c.definition_json).where(
                        evaluation_runs.c.run_id == str(run_id)
                    )
                )
            ).scalar_one()
        )
        release = EvidenceReleaseIdentity(
            **{k: FrozenContractRef(**v) for k, v in creation["release"].items()}
        )
        suite = await stored_run_suite(db, creation)
        prepared = await prepare_run_case(db, creation, cp.case_id)
        route = await run_model_route(db, creation, semantic=cp.kind == "semantic")
        if cp.kind == "generation":
            messages = prepared.messages
            ref = release.output_schema
            asset = await schemas.get(ref.id, ref.version.removeprefix(ref.id + "/"))
            if asset is None or (
                asset.schema_id,
                asset.schema_id + "/" + asset.version,
                asset.fingerprint,
            ) != (ref.id, ref.version, ref.fingerprint):
                raise ValueError("Frozen output schema unavailable")
            schema = json.loads(asset.definition_json)
        else:
            row = (
                (
                    await db.execute(
                        select(evaluation_generation_completions).where(
                            evaluation_generation_completions.c.run_id == str(run_id),
                            evaluation_generation_completions.c.candidate_id == cp.candidate_id,
                        )
                    )
                )
                .mappings()
                .one()
            )
            generation = decode_completion(row)
            candidate_fingerprint = generation.normalized_fingerprint
            assertions = tuple(AssertionReceipt(**a) for a in row["candidate_json"]["assertions"])
            messages = prepare_semantic_messages(
                release, generation, assertions, prepared=prepared, frozen_suite=suite
            )
            schema = json.loads(load_semantic_assets().output_schema_json)
        state = await reserve_dispatch(db, run_id, state.version, owner, at)
        # This commit must finish before control reaches the external gateway.
        await db.commit()
    response = None
    failure = None
    try:
        response = await gateway.generate_messages(messages, route, schema, invocation_id)
    except ProviderFailure as error:
        failure = classify_provider_failure(cp.kind, execution_id, error)
    finished = max(clock(), at)
    receipt = None
    raw, normalized = b"", b""
    if response is not None:
        evidence = response_evidence(cp.kind, execution_id, invocation_id, route, schema, response)
        receipt, raw, normalized, failure = (
            evidence.receipt,
            evidence.raw,
            evidence.normalized,
            evidence.failure,
        )
    if cp.kind == "semantic" and failure is None:
        assert receipt is not None
        obligations = semantic_obligations(
            assertion_inventory(release.suite, cp.case_id, frozen_suite=suite), assertions
        )
        try:
            await parse_semantic_output(
                normalized, release, routes, receipt, invocation_id, obligations
            )
        except SemanticDecisionInvalid:
            failure = ClassifiedFailure(
                "semantic_evaluation",
                "semantic_execution",
                "semantic_decision_contract_invalid",
                True,
                False,
                "retry_semantic",
                "Semantic decision evidence invalid",
                (execution_id,),
            )
    status = (
        "succeeded" if failure is None else "result_unknown" if failure.result_unknown else "failed"
    )
    async with transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        if cp.kind == "generation":
            completed = GenerationCompletion(
                execution_id,
                cp.case_id,
                cp.slot_ordinal,
                cp.execution_ordinal,
                invocation_id,
                status,
                at,
                finished,
                1,
                receipt,
                raw,
                normalized,
                "sha256:" + hashlib.sha256(normalized).hexdigest() if normalized else "",
                failure,
            )
            state = await complete_evaluated_generation(
                db,
                run_id,
                state.version,
                organization_id,
                owner,
                completed,
                routes,
                schemas,
                candidate_id=str(uuid4()) if status == "succeeded" else "",
            )
        else:
            judged = SemanticCompletion(
                execution_id,
                cp.candidate_id,
                candidate_fingerprint,
                cp.execution_ordinal,
                invocation_id,
                status,
                at,
                finished,
                1,
                receipt,
                raw,
                normalized,
                failure,
            )
            state = await complete_semantic(
                db, run_id, state.version, organization_id, owner, judged, routes
            )
        await db.commit()
    return state
