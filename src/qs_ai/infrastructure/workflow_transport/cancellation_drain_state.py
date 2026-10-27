"""Internal drain of an existing cancel request; never a new cancellation authority."""

import asyncio
import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transport import RouteResolver
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.cancellation import CancellationDecision
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import (
    finish_cancellation,
    plan_cancellation,
    read_cancel_drain_source,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
    source_snapshot_digest,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive

_MAGIC = b"qs-ai-cancellation-drain/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate drain fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class CancellationDrainOperation:
    scope: ManagementScope
    request_version: int
    reason: str = field(repr=False)
    requested_at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, ManagementScope)
            or type(self.request_version) is not int
            or not 1 < self.request_version < 2**63 - 1
        ):
            raise ValueError("Original cancellation request identity required")
        CancellationDecision(self.scope.actor, self.reason, False, self.requested_at)

    def _value(self) -> dict[str, Any]:
        return dict(
            run_id=str(self.scope.run_id),
            organization_id=self.scope.organization_id,
            operator_user_id=self.scope.operator_user_id,
            request_version=self.request_version,
            reason=self.reason,
            requested_at=self.requested_at.isoformat(timespec="microseconds"),
        )

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.scope.organization_id),
            str(self.scope.run_id),
            f"cancel-drain/{self.request_version}",
            self.request_version,
            hashlib.sha256(b"qs-ai-cancellation-drain-input/v1\0" + _json(self._value())).digest(),
        )

    @classmethod
    def from_run(cls, scope: ManagementScope, run: Any) -> "CancellationDrainOperation":
        value = run["progress_json"]["cancel_requested"]
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "schema_version",
                "run_id",
                "source_version",
                "version",
                "status",
                "actor",
                "reason",
                "requested_at",
            }
            or value["schema_version"] != "qs-ai-evaluation-cancel-request/v1"
            or value["run_id"] != str(scope.run_id)
            or value["actor"] != scope.actor
            or value["status"] != "cancel_requested"
            or type(value["source_version"]) is not int
            or value["source_version"] < 1
            or type(value["version"]) is not int
            or value["version"] != value["source_version"] + 1
            or value["version"] > run["version"]
        ):
            raise ValueError("Exact original cancellation request audit required")
        return cls(
            scope, value["version"], value["reason"], datetime.fromisoformat(value["requested_at"])
        )

    def encode(self, source: bytes) -> bytes:
        if type(source) is not bytes or len(source) != 32:
            raise ValueError("Exact original drain source required")
        raw = _MAGIC + _json(dict(operation=self._value(), source=source.hex()))
        if len(raw) > 8192:
            raise ValueError("Frozen drain facts exceed bounds")
        return raw

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> tuple["CancellationDrainOperation", bytes]:
        raw = facts.business_context
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Bounded original drain facts required")
        try:
            obj = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(obj, dict) or set(obj) != {"operation", "source"}:
                raise ValueError
            v = obj["operation"]
            if not isinstance(v, dict) or set(v) != {
                "run_id",
                "organization_id",
                "operator_user_id",
                "request_version",
                "reason",
                "requested_at",
            }:
                raise ValueError
            first = cls(
                ManagementScope(UUID(v["run_id"]), v["organization_id"], v["operator_user_id"]),
                v["request_version"],
                v["reason"],
                datetime.fromisoformat(v["requested_at"]),
            )
            source = bytes.fromhex(obj["source"])
            if (
                first.intent != facts.intent
                or first.encode(source) != raw
                or facts.source_version is None
                or facts.at < first.requested_at
            ):
                raise ValueError
            return first, source
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen original drain facts") from None


def _source(run: Any, progress: dict) -> bytes:
    checkpoint = {
        "run_id": run["run_id"],
        "version": run["version"],
        "checkpoint_json": run["checkpoint_json"],
    }
    return hashlib.sha256(
        b"qs-ai-cancellation-drain-source/v1\0"
        + source_snapshot_digest(checkpoint, run)
        + _json(progress)
    ).digest()


def read_cancellation_drain_plan(
    operation: CancellationDrainOperation,
    winner: FrozenPlan,
    *,
    route_resolver: RouteResolver,
) -> tuple[EvaluationStateFacts, bytes, FrozenArchive]:
    facts, archive = read_evaluation_state_plan(
        operation.intent, winner, route_resolver=route_resolver
    )
    first, source = CancellationDrainOperation.from_facts(facts)
    visible = pb.MessagingBody.FromString(archive.body).evaluation_state
    if (
        first != operation
        or visible.status != "canceled"
        or facts.source_version is None
        or visible.version != facts.source_version + 1
    ):
        raise MessageConflict("Exact first cancellation drain projection required")
    return facts, source, archive


class CancellationDrainPlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive drain database budget required")
        self.transactions, self.database_seconds = transactions, database_seconds
        self.signing_key, self.recipient_key = signing_key, recipient_key

    async def observe(
        self, scope: ManagementScope, at: datetime
    ) -> CancellationDrainOperation | None:
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            run = await header(db, scope)
            source = await read_cancel_drain_source(db, scope, at, lock=False)
            return None if source is None else CancellationDrainOperation.from_run(scope, run)

    async def candidate(self, operation: CancellationDrainOperation, at: datetime) -> FrozenPlan:
        at = max(at, operation.requested_at)
        EvaluationStateFacts(operation.intent, at, 0, 0)
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            run = await header(db, operation.scope)
            source = await read_cancel_drain_source(db, operation.scope, at, lock=False)
            if (
                source is None
                or CancellationDrainOperation.from_run(operation.scope, run) != operation
            ):
                raise CheckpointConflict("Original cancellation is no longer ready to drain")
            state, progress = await plan_cancellation(
                db,
                source.scope,
                source.version,
                source.reason,
                source.at,
                discard=False,
                confirm=True,
                lock=False,
            )
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(operation.scope.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            facts = EvaluationStateFacts(
                operation.intent,
                source.at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                operation.encode(_source(run, progress)),
                source_version=source.version,
            )
            visible = evaluation_state(
                str(operation.scope.run_id),
                {
                    **run,
                    "version": state.version,
                    "progress_json": progress,
                },
                facts.sequence,
            )
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(operation.scope.run_id, f"state:{visible.version}")),
            str(operation.scope.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(operation.scope.organization_id),
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(
            facts, str(uuid4()), first, route_resolver=self._route_resolver
        )
        read_cancellation_drain_plan(operation, winner, route_resolver=self._route_resolver)
        return winner


async def apply_cancellation_drain(db: AsyncSession, facts: EvaluationStateFacts) -> None:
    """Caller holds the guarded root; original writer retains terminal admission."""
    operation, digest = CancellationDrainOperation.from_facts(facts)
    checkpoint = (
        (
            await db.execute(
                select(evaluation_checkpoints)
                .where(evaluation_checkpoints.c.run_id == str(operation.scope.run_id))
                .with_for_update()
            )
        )
        .mappings()
        .one()
    )
    run = (
        (
            await db.execute(
                select(evaluation_runs)
                .where(
                    evaluation_runs.c.run_id == str(operation.scope.run_id),
                    evaluation_runs.c.organization_id == operation.scope.organization_id,
                )
                .with_for_update()
            )
        )
        .mappings()
        .one()
    )
    current = {
        **run,
        "version": checkpoint["version"],
        "checkpoint_json": checkpoint["checkpoint_json"],
    }
    source = await read_cancel_drain_source(db, operation.scope, facts.at, lock=True)
    if (
        source is None
        or source.version != facts.source_version
        or CancellationDrainOperation.from_run(operation.scope, current) != operation
    ):
        raise CheckpointConflict("Original cancellation drain source changed")
    _, progress = await plan_cancellation(
        db,
        source.scope,
        source.version,
        source.reason,
        source.at,
        discard=False,
        confirm=True,
        lock=True,
    )
    if _source(current, progress) != digest:
        raise MessageConflict("Original drain evidence differs from frozen plan")
    if not await finish_cancellation(db, operation.scope, facts.at):
        raise CheckpointConflict("Original cancellation did not finish")
