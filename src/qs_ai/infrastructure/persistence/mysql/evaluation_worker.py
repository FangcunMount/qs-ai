"""Poll started Runs; active checkpoints belong to recovery, never ordinary polling."""

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, or_, select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.execution.model_capacity import ModelCapacity
from qs_ai.application.interpretation.provider import MessagesGateway
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.application.operations.diagnostics import attempt_context, operation
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import finish_cancellation
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
    recover_next_candidate,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import execute_preflight
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor, recover_next
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_runs,
    evaluation_slot_claims,
)
from qs_ai.infrastructure.workflows.evaluation import execute_step


class EvaluationWorker:
    def __init__(
        self,
        transactions: Transactions,
        gateway: MessagesGateway,
        routes: RouteAssets,
        schemas: SchemaAssets,
        owner: str,
        *,
        enabled: bool = False,
        recovery_cursor: RecoveryCursor | None = None,
        candidate_limit: int = 1,
        capacity: ModelCapacity | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if type(candidate_limit) is not int or not 1 <= candidate_limit <= 32:
            raise ValueError("Invalid candidate concurrency")
        self.candidate_limit = candidate_limit
        self.capacity = capacity
        self.transactions = transactions
        self.gateway, self.routes, self.schemas = gateway, routes, schemas
        self.owner, self.enabled, self.clock = owner, enabled, clock
        self.recovery_cursor = recovery_cursor if recovery_cursor is not None else RecoveryCursor()

    async def once(self) -> bool:
        if not self.enabled:
            return False
        if await recover_next(
            self.transactions,
            self.routes,
            self.schemas,
            self.owner,
            self.clock(),
            self.recovery_cursor,
        ):
            return True
        if await recover_next_candidate(
            self.transactions, self.routes, self.schemas, self.clock(), self.recovery_cursor
        ):
            return True
        if await self._drain_cancellations():
            return True
        # Selection is only a hint. The shared checkpoint CAS owns the claim, and
        # the session closes before model I/O. Organization comes from the Run.
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(
                            evaluation_runs.c.run_id,
                            evaluation_runs.c.organization_id,
                            evaluation_runs.c.progress_json,
                            evaluation_runs.c.execution_mode,
                            evaluation_checkpoints.c.version,
                        )
                        .join(
                            evaluation_checkpoints,
                            evaluation_runs.c.run_id == evaluation_checkpoints.c.run_id,
                        )
                        .where(
                            evaluation_runs.c.progress_json["status"].as_string() == "collecting",
                            func.json_extract(
                                evaluation_runs.c.progress_json, "$.cancel_requested"
                            ).is_(None),
                            or_(
                                evaluation_runs.c.execution_mode == "serial_v1",
                                select(func.count())
                                .select_from(evaluation_slot_claims)
                                .where(evaluation_slot_claims.c.run_id == evaluation_runs.c.run_id)
                                .scalar_subquery()
                                < self.candidate_limit,
                            ),
                            or_(
                                evaluation_checkpoints.c.checkpoint_json.is_(None),
                                func.json_type(evaluation_checkpoints.c.checkpoint_json) == "NULL",
                            ),
                        )
                        .order_by(evaluation_checkpoints.c.version, evaluation_runs.c.run_id)
                        .limit(1)
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return False
        run_id = UUID(row["run_id"])
        try:
            with attempt_context(run_id=str(run_id)), operation("evaluation.step", "evaluation"):
                if "preflight" not in row["progress_json"]:
                    async with self.transactions.open() as db:
                        await execute_preflight(
                            db, run_id, row["version"], row["organization_id"], self.clock()
                        )
                        await db.commit()
                else:
                    await execute_step(
                        self.transactions,
                        run_id,
                        row["version"],
                        row["organization_id"],
                        self.owner,
                        self.gateway,
                        self.routes,
                        self.schemas,
                        clock=self.clock,
                        capacity=self.capacity,
                        candidate_limit=self.candidate_limit
                        if row["execution_mode"] == "candidate_v2"
                        else None,
                    )
        except CheckpointConflict:
            # Another consumer or lifecycle transition won. Never retry this
            # invocation inside the same attempt; next poll reads current state.
            return False
        return True

    async def _drain_cancellations(self) -> bool:
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(
                            evaluation_runs.c.run_id,
                            evaluation_runs.c.organization_id,
                            evaluation_runs.c.progress_json,
                        )
                        .join(
                            evaluation_checkpoints,
                            evaluation_checkpoints.c.run_id == evaluation_runs.c.run_id,
                        )
                        .where(
                            evaluation_runs.c.progress_json["status"].as_string() != "canceled",
                            func.json_extract(
                                evaluation_runs.c.progress_json, "$.cancel_requested"
                            ).is_not(None),
                            func.coalesce(
                                evaluation_runs.c.progress_json[
                                    "unresolved_result_unknown_count"
                                ].as_integer(),
                                0,
                            )
                            == 0,
                            or_(
                                evaluation_checkpoints.c.checkpoint_json.is_(None),
                                func.json_type(evaluation_checkpoints.c.checkpoint_json) == "NULL",
                            ),
                            ~select(evaluation_slot_claims.c.run_id)
                            .where(evaluation_slot_claims.c.run_id == evaluation_runs.c.run_id)
                            .exists(),
                        )
                        .order_by(evaluation_runs.c.run_id)
                        .limit(1)
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return False
        intent = row["progress_json"]["cancel_requested"]
        scope = ManagementScope(
            UUID(row["run_id"]), row["organization_id"], int(intent["actor"].removeprefix("user:"))
        )
        async with self.transactions.open() as db:
            changed = await finish_cancellation(db, scope, self.clock())
            await db.commit()
            return changed
