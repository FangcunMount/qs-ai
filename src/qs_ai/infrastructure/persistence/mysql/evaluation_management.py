import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.candidates import (
    CandidateEvidence,
    CandidateIndex,
    validate_candidate_query,
)
from qs_ai.application.evaluation.gates import GatePreview
from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.application.evaluation.unknowns import UnknownExecutionIndex, validate_unknown_query
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
from qs_ai.domain.evaluation.review import CandidateHumanReview
from qs_ai.infrastructure.persistence.mysql import evaluation_candidates
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import cancel, read_cancellation
from qs_ai.infrastructure.persistence.mysql.evaluation_creation_receipt import creation_receipt
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import (
    finalize,
    read_finalization,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import preview_gates
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import transition_requested
from qs_ai.infrastructure.persistence.mysql.evaluation_reopening import reopen
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import accept_resolution
from qs_ai.infrastructure.persistence.mysql.evaluation_review_history import canonical
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews
from qs_ai.infrastructure.persistence.mysql.evaluation_unknowns import list_unknowns
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


async def read_view(db: AsyncSession, scope: ManagementScope) -> EvaluationView:
    row = (
        (
            await db.execute(
                select(
                    evaluation_runs,
                    evaluation_checkpoints.c.version,
                    evaluation_checkpoints.c.checkpoint_json,
                )
                .join(
                    evaluation_checkpoints,
                    evaluation_runs.c.run_id == evaluation_checkpoints.c.run_id,
                )
                .where(
                    evaluation_runs.c.run_id == str(scope.run_id),
                    evaluation_runs.c.organization_id == scope.organization_id,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise NotFound("Evaluation unavailable in organization")
    progress = row["progress_json"]
    if progress is None:
        raise ValueError("Legacy progress needs reconciliation")
    cancellation, source = await read_cancellation(db, scope, dict(row))
    finalization, can_reopen = await read_finalization(db, scope, source)
    return EvaluationView(
        str(scope.run_id),
        row["version"],
        progress["status"],
        progress.get("unresolved_result_unknown_count", 0),
        json.dumps(progress.get("result_unknown_resolutions", []), ensure_ascii=False),
        json.dumps(progress.get("human_reviews", []), ensure_ascii=False),
        finalization,
        canonical(progress.get("review_reopenings", [])),
        creation_receipt(dict(row)),
        cancellation,
        can_reopen and not bool(cancellation),
    )


class MySQLEvaluationManagement:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def cancel(
        self,
        scope: ManagementScope,
        expected_version: int,
        reason: str,
        at: datetime,
        *,
        discard: bool,
        confirm: bool,
    ) -> EvaluationView:
        async with self.transactions.open() as db:
            await cancel(db, scope, expected_version, reason, at, discard=discard, confirm=confirm)
            view = await read_view(db, scope)
            await db.commit()
            return view

    async def list_unknowns(
        self, scope: ManagementScope, expected_version: int
    ) -> UnknownExecutionIndex:
        validate_unknown_query(expected_version)
        async with self.transactions.open() as db:
            return await list_unknowns(db, scope, expected_version)

    async def reopen(
        self,
        scope: ManagementScope,
        expected_version: int,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        async with self.transactions.open() as db:
            await reopen(db, scope, expected_version, reason, at, confirm=confirm)
            view = await read_view(db, scope)
            await db.commit()
            return view

    async def finalize(
        self,
        scope: ManagementScope,
        expected_version: int,
        expected_passed: bool,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        async with self.transactions.open() as db:
            await finalize(
                db, scope, expected_version, expected_passed, reason, at, confirm=confirm
            )
            view = await read_view(db, scope)
            await db.commit()
            return view

    async def preview_gates(
        self, scope: ManagementScope, expected_version: int, at: datetime
    ) -> GatePreview:
        async with self.transactions.open() as db:
            return await preview_gates(db, scope, expected_version, at)

    async def list_candidates(self, scope: ManagementScope) -> CandidateIndex:
        async with self.transactions.open() as db:
            return await evaluation_candidates.list_candidates(db, scope)

    async def get_candidate(
        self, scope: ManagementScope, candidate_id: str, expected_version: int
    ) -> CandidateEvidence:
        validate_candidate_query(candidate_id, expected_version)
        async with self.transactions.open() as db:
            return await evaluation_candidates.get_candidate(
                db, scope, candidate_id, expected_version
            )

    async def get(self, scope: ManagementScope) -> EvaluationView:
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            return await read_view(db, scope)

    async def review(
        self,
        scope: ManagementScope,
        expected_version: int,
        values: tuple[CandidateHumanReview, ...],
    ) -> EvaluationView:
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            await read_view(db, scope)
            await accept_reviews(db, scope, expected_version, values)
            view = await read_view(db, scope)
            await db.commit()
            return view

    async def start(
        self,
        scope: ManagementScope,
        expected_version: int,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        if confirm is not True or type(expected_version) is not int or expected_version < 1:
            raise ValueError("Explicit version and confirmation required")
        async with self.transactions.open() as db:
            # Scope lookup precedes state changes; absent and foreign Runs look identical.
            await read_view(db, scope)
            await transition_requested(
                db,
                scope.run_id,
                expected_version,
                scope.organization_id,
                "collecting",
                scope.actor,
                reason,
                at,
            )
            view = await read_view(db, scope)
            await db.commit()
            return view

    async def resolve(
        self,
        scope: ManagementScope,
        expected_version: int,
        value: ResultUnknownResolution,
        *,
        confirm: bool,
    ) -> EvaluationView:
        if value.actor != scope.actor:
            raise ValueError("Decision actor differs from trusted scope")
        async with self.transactions.open() as db:
            await accept_resolution(
                db, scope.run_id, expected_version, scope.organization_id, value, confirm=confirm
            )
            view = await read_view(db, scope)
            await db.commit()
            return view
