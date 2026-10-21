"""Frozen, explicitly authorized finalization/reopening on the original root.

The caller owns permission checks and the operation identity for this invocation.
This module does not introduce a public cross-request idempotency contract.
"""

import asyncio
import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import (
    finalized_progress,
    lock_run_in_transaction,
    save_progress,
    verified_final_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import evaluate_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_reopening import reopened_progress
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state

_MAGIC = b"qs-ai-governance-state/v1\0"
_SOURCE = (
    "run_id",
    "organization_id",
    "requested_by",
    "execution_mode",
    "definition_json",
    "progress_json",
    "version",
    "frozen_execution_policy_fingerprint",
    "frozen_execution_policy_json",
)


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate governance fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class GovernanceStateOperation:
    kind: str
    scope: ManagementScope
    operation_id: UUID
    expected_version: int
    reason: str = field(repr=False)
    confirm: bool
    expected_passed: bool | None = None

    def __post_init__(self) -> None:
        if (
            self.kind not in ("finalize", "reopen")
            or not isinstance(self.scope, ManagementScope)
            or not isinstance(self.operation_id, UUID)
            or not self.operation_id.int
            or type(self.expected_version) is not int
            or not 0 < self.expected_version < 2**63 - 1
            or type(self.reason) is not str
            or not self.reason.strip()
            or len(self.reason.encode()) > 1000
            or any(c in self.reason for c in "<>")
            or self.confirm is not True
            or (self.kind == "finalize" and type(self.expected_passed) is not bool)
            or (self.kind == "reopen" and self.expected_passed is not None)
        ):
            raise ValueError("Explicit authorized governance operation required")

    def _value(self) -> dict[str, Any]:
        return dict(
            kind=self.kind,
            run_id=str(self.scope.run_id),
            organization_id=self.scope.organization_id,
            operator_user_id=self.scope.operator_user_id,
            operation_id=str(self.operation_id),
            expected_version=self.expected_version,
            reason=self.reason,
            confirm=self.confirm,
            expected_passed=self.expected_passed,
        )

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.scope.organization_id),
            str(self.scope.run_id),
            f"governance/{self.kind}/{self.operation_id}",
            self.expected_version,
            hashlib.sha256(b"qs-ai-governance-input/v1\0" + _json(self._value())).digest(),
        )

    def encode(self, snapshot_digest: bytes) -> bytes:
        if type(snapshot_digest) is not bytes or len(snapshot_digest) != 32:
            raise ValueError("Original governance snapshot required")
        raw = _MAGIC + _json(dict(operation=self._value(), snapshot_digest=snapshot_digest.hex()))
        if len(raw) > 8192:
            raise ValueError("Frozen governance operation exceeds bounds")
        return raw

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> tuple["GovernanceStateOperation", bytes]:
        raw = facts.business_context
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded governance facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {"operation", "snapshot_digest"}:
                raise ValueError
            v = value["operation"]
            if not isinstance(v, dict) or set(v) != {
                "kind",
                "run_id",
                "organization_id",
                "operator_user_id",
                "operation_id",
                "expected_version",
                "reason",
                "confirm",
                "expected_passed",
            }:
                raise ValueError
            operation = cls(
                v["kind"],
                ManagementScope(UUID(v["run_id"]), v["organization_id"], v["operator_user_id"]),
                UUID(v["operation_id"]),
                v["expected_version"],
                v["reason"],
                v["confirm"],
                v["expected_passed"],
            )
            digest = bytes.fromhex(value["snapshot_digest"])
            if operation.intent != facts.intent or operation.encode(digest) != raw:
                raise ValueError
            return operation, digest
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen governance facts") from None


def source_snapshot_digest(run: Mapping[Any, Any]) -> bytes:
    return hashlib.sha256(
        b"qs-ai-governance-source/v1\0"
        + _json(
            {**{key: run[key] for key in _SOURCE}, "checkpoint_json": run.get("checkpoint_json")}
        )
    ).digest()


async def _progress(
    db: AsyncSession, operation: GovernanceStateOperation, run: Mapping[Any, Any], at: datetime
) -> dict:
    if operation.kind == "finalize":
        preview = await evaluate_snapshot(db, operation.scope, run, operation.expected_version, at)
        assert operation.expected_passed is not None
        return finalized_progress(
            run["progress_json"],
            preview,
            operation.scope.actor,
            operation.reason,
            operation.expected_passed,
        )
    if not run["progress_json"] or run["progress_json"]["status"] != "rejected":
        raise CheckpointConflict("Only a finalized rejection can be reopened")
    snapshot = await verified_final_snapshot(db, operation.scope, run)
    return reopened_progress(operation.scope, operation.reason, at, run["progress_json"], snapshot)


class GovernanceStatePlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive governance database budget required")
        self._transactions, self._database_seconds = transactions, database_seconds
        self._signing_key, self._recipient_key = signing_key, recipient_key

    async def candidate(self, operation: GovernanceStateOperation, at: datetime) -> FrozenPlan:
        # Validate the first host time before even opening the read boundary.
        EvaluationStateFacts(operation.intent, at, 0, 0)
        async with asyncio.timeout(self._database_seconds), self._transactions.open() as db:
            run = await header(db, operation.scope)
            if run["version"] != operation.expected_version or run["checkpoint_json"] is not None:
                raise CheckpointConflict("Governance version changed or execution is active")
            progress = await _progress(db, operation, run, at)
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
                at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                operation.encode(source_snapshot_digest(run)),
            )
            visible = evaluation_state(
                str(operation.scope.run_id),
                {
                    **run,
                    "version": operation.expected_version + 1,
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
            signing_key=self._signing_key,
            recipient_key=self._recipient_key,
            original_occurred_at=at.isoformat(timespec="microseconds"),
        )
        return freeze_evaluation_state_plan(facts, str(operation.operation_id), first)


async def apply_governance_state(db: AsyncSession, facts: EvaluationStateFacts) -> None:
    """Caller holds the guarded original REPEATABLE READ root; never commit or Seal here."""
    operation, digest = GovernanceStateOperation.from_facts(facts)
    run = await lock_run_in_transaction(db, operation.scope, operation.expected_version)
    if source_snapshot_digest(run) != digest:
        raise MessageConflict("Original governance source snapshot changed")
    progress = await _progress(db, operation, run, facts.at)
    await save_progress(db, operation.scope, operation.expected_version, progress)


class GovernanceStateRoot(EvaluationStateRoot):
    """Bind this exact authorized operation and snapshot isolation before the guard."""

    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        operation: GovernanceStateOperation,
        winner: FrozenPlan,
        *,
        database_seconds: float = 2,
    ) -> None:
        facts, archived = read_evaluation_state_plan(operation.intent, winner)
        actual, _ = GovernanceStateOperation.from_facts(facts)
        state = pb.MessagingBody.FromString(archived.body).evaluation_state
        if actual != operation or state.version != operation.expected_version + 1:
            raise MessageConflict("Exact original governance operation required")
        super().__init__(
            transactions,
            archives,
            store,
            operation.intent,
            winner,
            apply_governance_state,
            database_seconds=database_seconds,
            isolation_level="REPEATABLE READ",
        )
