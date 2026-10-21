"""Read-only composition of original evaluation admission rules before half messages."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.application.evaluation.execution_mode import (
    SERIAL_RUNTIME_LIMITS,
    EvaluationRuntimeLimits,
)
from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.governance.solution_models import (
    DEFAULT_EDITABLE_MODELS,
    EditableModelPolicy,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.editable_model_policy import check_editable_models
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import run_release
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import plan_cancellation
from qs_ai.infrastructure.persistence.mysql.evaluation_capacity import plan_admission
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_management import view_from_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import plan_requested_transition
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs

_DEFAULT_CAPACITY = EvaluationCapacityPolicy()
_FIELDS = (
    "run_id",
    "organization_id",
    "version",
    "checkpoint_json",
    "definition_json",
    "progress_json",
    "execution_mode",
)


def evaluation_source_digest(source: dict[str, Any]) -> bytes:
    value = {name: source[name] for name in _FIELDS}
    raw = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()
    return hashlib.sha256(b"qs-ai-evaluation-source/v1\0" + raw).digest()


@dataclass(frozen=True)
class EvaluationStateProjection:
    run_id: str
    organization_id: str
    version: int
    status: str
    release_fingerprint: str


class EvaluationAdmissionProjection:
    def __init__(
        self,
        transactions: Transactions,
        capacity: EvaluationCapacityPolicy = _DEFAULT_CAPACITY,
        models: EditableModelPolicy = DEFAULT_EDITABLE_MODELS,
        quota_baseline: QuotaBaseline | None = None,
        runtime_limits: EvaluationRuntimeLimits = SERIAL_RUNTIME_LIMITS,
    ) -> None:
        self.transactions, self.capacity, self.models = transactions, capacity, models
        self.quota_baseline, self.runtime_limits = quota_baseline, runtime_limits
        self._source_digest: bytes | None = None
        self._state: EvaluationStateProjection | None = None
        self._used = False

    @property
    def source_digest(self) -> bytes | None:
        return self._source_digest

    @property
    def state(self) -> EvaluationStateProjection | None:
        return self._state

    def _once(self) -> None:
        if self._used:
            raise RuntimeError("One original evaluation projection per plan required")
        self._used = True

    async def _source(self, db: AsyncSession, scope: ManagementScope) -> dict[str, Any]:
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
        if (
            row is None
            or row["run_id"] != str(scope.run_id)
            or row["organization_id"] != scope.organization_id
        ):
            raise NotFound("Evaluation unavailable in organization")
        source = dict(row)
        self._source_digest = evaluation_source_digest(source)
        return source

    async def _view(
        self,
        db: AsyncSession,
        scope: ManagementScope,
        source: dict[str, Any],
        state: CheckpointState,
        updated: dict[str, Any],
    ) -> EvaluationView:
        projected = {
            **source,
            "version": state.version,
            "checkpoint_json": encode(state.checkpoint),
            "progress_json": updated,
        }
        result = await view_from_snapshot(db, scope, projected, self.runtime_limits)
        creation = json.loads(source["definition_json"])
        self._state = EvaluationStateProjection(
            str(scope.run_id),
            str(scope.organization_id),
            state.version,
            updated["status"],
            creation["release_fingerprint"],
        )
        return result

    async def start(
        self,
        scope: ManagementScope,
        expected_version: int,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        self._once()
        if confirm is not True or type(expected_version) is not int or expected_version < 1:
            raise ValueError("Explicit version and confirmation required")
        async with self.transactions.open() as db:
            source = await self._source(db, scope)
            before = await view_from_snapshot(db, scope, source, self.runtime_limits)
            if before.status == "requested":
                await check_editable_models(
                    db, run_release(json.loads(source["definition_json"])), self.models
                )
            state, updated = plan_requested_transition(
                scope.run_id,
                expected_version,
                scope.organization_id,
                "collecting",
                scope.actor,
                reason,
                at,
                {"version": source["version"], "checkpoint_json": source["checkpoint_json"]},
                source,
            )
            await plan_admission(db, scope, self.capacity, at, self.quota_baseline)
            return await self._view(db, scope, source, state, updated)

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
        self._once()
        async with self.transactions.open() as db:
            state, updated = await plan_cancellation(
                db, scope, expected_version, reason, at, discard=discard, confirm=confirm
            )
            source = await self._source(db, scope)
            return await self._view(db, scope, source, state, updated)
