"""Poll started Runs; active checkpoints belong to recovery, never ordinary polling."""

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, or_, select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.application.operations.diagnostics import attempt_context, operation
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import execute_preflight
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor, recover_next
from qs_ai.infrastructure.persistence.mysql.evaluation_step import MessagesGateway, execute_step
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


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
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
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
                            evaluation_checkpoints.c.version,
                        )
                        .join(
                            evaluation_checkpoints,
                            evaluation_runs.c.run_id == evaluation_checkpoints.c.run_id,
                        )
                        .where(
                            evaluation_runs.c.progress_json["status"].as_string() == "collecting",
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
                    )
        except CheckpointConflict:
            # Another consumer or lifecycle transition won. Never retry this
            # invocation inside the same attempt; next poll reads current state.
            return False
        return True
