"""One durable evaluation step; no automatic replay of an active dispatch checkpoint."""

import asyncio
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.model_response import response_evidence
from qs_ai.application.execution.model_capacity import CapacityToken, ModelCapacity
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.application.operations.diagnostics import emit
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.contract_recovery import RECOVERY_INSTRUCTION, decode_recoveries
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
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import prepare_candidate
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import (
    complete_evaluated_generation,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import semantic_contract
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import reserve_dispatch
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import complete_semantic
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim, dispatch_claim
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_runs,
)
from qs_ai.infrastructure.qs_server.evaluation_assertions import (
    assertion_inventory,
    semantic_obligations,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import FrozenSuite
from qs_ai.infrastructure.qs_server.semantic_assets import SemanticAssets
from qs_ai.infrastructure.qs_server.semantic_input import prepare_semantic_messages
from qs_ai.infrastructure.qs_server.semantic_output import (
    SemanticDecisionInvalid,
    parse_semantic_output,
)


@dataclass(frozen=True)
class PreparedStep:
    state: CheckpointState
    cp: ExecutionCheckpoint
    at: datetime
    invocation_id: str
    execution_id: str
    candidate_fingerprint: str
    assertions: tuple[AssertionReceipt, ...]
    release: EvidenceReleaseIdentity
    suite: FrozenSuite
    route: ModelRoute
    messages: PromptMessages
    schema: dict[str, Any]
    semantic: SemanticAssets | None
    claim: SlotClaim | None = None
    capacity_token: CapacityToken | None = None


async def prepare_step(
    transactions: Transactions,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    routes: RouteAssets,
    schemas: SchemaAssets,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    candidate_limit: int | None = None,
    resume_claim: SlotClaim | None = None,
    capacity: ModelCapacity | None = None,
) -> PreparedStep:
    tokens: list[CapacityToken] = []
    try:
        return await _prepare_step(
            transactions,
            run_id,
            expected_version,
            organization_id,
            owner,
            routes,
            schemas,
            clock=clock,
            candidate_limit=candidate_limit,
            resume_claim=resume_claim,
            capacity=capacity,
            tokens=tokens,
        )
    except BaseException:
        for token in tokens:
            token.release()
        raise


async def _prepare_step(
    transactions: Transactions,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    routes: RouteAssets,
    schemas: SchemaAssets,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    candidate_limit: int | None = None,
    resume_claim: SlotClaim | None = None,
    capacity: ModelCapacity | None = None,
    tokens: list[CapacityToken],
) -> PreparedStep:
    """Run must already be collecting with passed preflight; caller supplies authorized scope."""
    at = clock()
    invocation_id, execution_id = str(uuid4()), str(uuid4())
    semantic: SemanticAssets | None = None
    candidate_fingerprint = ""
    assertions: tuple[AssertionReceipt, ...] = ()
    async with transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        claim = resume_claim
        if claim is not None:
            if claim.run_id != run_id:
                raise ValueError("Resume claim Run differs")
            state = CheckpointState(run_id, claim.version, claim.checkpoint)
            invocation_id, execution_id = (
                claim.checkpoint.invocation_id,
                claim.checkpoint.execution_id,
            )
            at = claim.checkpoint.dispatch_started_at or claim.checkpoint.claimed_at
        elif candidate_limit is not None:
            claim = await prepare_candidate(
                db,
                run_id,
                organization_id,
                owner,
                execution_id,
                invocation_id,
                at,
                at + timedelta(minutes=5),
                limit=candidate_limit,
            )
            state = CheckpointState(run_id, claim.version, claim.checkpoint)
        else:
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
        if capacity is not None and resume_claim is None:
            token = capacity.try_acquire(route.provider, evaluation=True)
            if token is None:
                raise CheckpointConflict("Model capacity unavailable; defer without dispatch")
            tokens.append(token)
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
            semantic = await semantic_contract(db, release, organization_id, frozen=creation)
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
            messages = await asyncio.to_thread(
                prepare_semantic_messages,
                release,
                generation,
                assertions,
                prepared=prepared,
                frozen_suite=suite,
                assets=semantic,
            )
            progress = (
                await db.execute(
                    select(evaluation_runs.c.progress_json).where(
                        evaluation_runs.c.run_id == str(run_id)
                    )
                )
            ).scalar_one()
            recoveries = decode_recoveries(progress.get("semantic_contract_recoveries", []))
            # Preparation validated the ledger and exact next attempt under CAS.
            # Keep frozen prompt bytes intact; the separate v1 authorization owns this supplement.
            if cp.execution_ordinal == 2 and any(
                r.candidate_id == cp.candidate_id for r in recoveries
            ):
                messages = replace(
                    messages, task_message=messages.task_message + "\n" + RECOVERY_INSTRUCTION
                )
            schema = json.loads(semantic.output_schema_json)
        if claim is not None:
            if resume_claim is None:
                claim = await dispatch_claim(db, organization_id, claim, clock())
                cp = claim.checkpoint
                assert cp.dispatch_started_at is not None
                at = cp.dispatch_started_at
            state = CheckpointState(run_id, claim.version, claim.checkpoint)
        else:
            state = await reserve_dispatch(db, run_id, state.version, owner, at)
        # This commit must finish before control reaches the external gateway.
        await db.commit()
    emit(
        "evaluation.model_dispatched",
        "evaluation",
        run_id=str(run_id),
        invocation_id=invocation_id,
        stage=cp.kind,
    )
    return PreparedStep(
        state,
        cp,
        at,
        invocation_id,
        execution_id,
        candidate_fingerprint,
        assertions,
        release,
        suite,
        route,
        messages,
        schema,
        semantic,
        claim,
        tokens[0] if tokens else None,
    )


async def finish_step(
    transactions: Transactions,
    run_id: UUID,
    organization_id: int,
    owner: str,
    routes: RouteAssets,
    schemas: SchemaAssets,
    prepared: PreparedStep,
    response: ModelResponse | None,
    failure: ClassifiedFailure | None,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    recovery_at: datetime | None = None,
) -> CheckpointState:
    state = prepared.state
    cp = prepared.cp
    at = prepared.at
    invocation_id = prepared.invocation_id
    execution_id = prepared.execution_id
    candidate_fingerprint = prepared.candidate_fingerprint
    assertions = prepared.assertions
    release = prepared.release
    suite = prepared.suite
    route = prepared.route
    schema = prepared.schema
    semantic = prepared.semantic
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
        assert semantic is not None
        assert receipt is not None
        obligations = semantic_obligations(
            (
                await asyncio.to_thread(
                    assertion_inventory, release.suite, cp.case_id, frozen_suite=suite
                )
            ),
            assertions,
        )
        try:
            await parse_semantic_output(
                normalized, release, routes, receipt, invocation_id, obligations, assets=semantic
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
        if prepared.claim is not None:
            from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import (
                completed_claim_state,
            )
            from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
                lock_run,
                require_claim,
            )

            completed_state = await completed_claim_state(db, organization_id, prepared.claim)
            if completed_state is not None:
                return completed_state
            await lock_run(db, run_id, organization_id)
            current = await require_claim(db, prepared.claim)
            if recovery_at is not None and (
                current.checkpoint.lease_expires_at != prepared.claim.checkpoint.lease_expires_at
                or recovery_at < current.checkpoint.lease_expires_at
            ):
                from qs_ai.application.evaluation.checkpoints import CheckpointConflict

                raise CheckpointConflict("Recovery ownership was renewed")
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
                claim=prepared.claim,
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
                db,
                run_id,
                state.version,
                organization_id,
                owner,
                judged,
                routes,
                claim=prepared.claim,
            )
        await db.commit()
    emit(
        "evaluation.receipt_committed",
        "evaluation",
        run_id=str(run_id),
        invocation_id=invocation_id,
        status=status,
        stage=cp.kind,
    )
    return state
