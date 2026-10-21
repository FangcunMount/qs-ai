"""Explicit trusted-host policy exception; never a Checker or model retry authority."""

import asyncio
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.contract_recovery import ContractRecovery
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_contract_recovery import (
    ContractRecoveryPlan,
    authorize_contract_recovery,
    plan_contract_recovery,
)
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
    load_preflight_source,
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

_MAGIC = b"qs-ai-contract-recovery-state/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate original policy exception fact")
        value[key] = item
    return value


@dataclass(frozen=True)
class ContractRecoveryInput:
    run_id: UUID
    organization_id: int
    expected_version: int
    expected_release_fingerprint: str
    value: ContractRecovery = field(repr=False)
    confirm: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.run_id, UUID)
            or not self.run_id.int
            or type(self.organization_id) is not int
            or not 0 < self.organization_id < 2**64
            or type(self.expected_version) is not int
            or not 0 < self.expected_version < 2**63 - 1
            or not isinstance(self.expected_release_fingerprint, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", self.expected_release_fingerprint) is None
            or not isinstance(self.value, ContractRecovery)
            or not self.value.actor.startswith("operator:")
            or self.confirm is not True
        ):
            raise ValueError("Explicit original trusted-host policy exception required")

    def _fields(self, *, clock: bool) -> dict[str, Any]:
        value = asdict(self.value)
        value.pop("resolved_at")
        if clock:
            value["resolved_at"] = self.value.resolved_at.isoformat(timespec="microseconds")
        return dict(
            run_id=str(self.run_id),
            organization_id=self.organization_id,
            expected_version=self.expected_version,
            expected_release_fingerprint=self.expected_release_fingerprint,
            value=value,
            confirm=self.confirm,
        )

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.organization_id),
            str(self.run_id),
            f"contract-recovery/{self.expected_version}",
            self.expected_version,
            hashlib.sha256(
                b"qs-ai-contract-recovery-input/v1\0" + _json(self._fields(clock=False))
            ).digest(),
        )

    def encode(self, source_digest: bytes) -> bytes:
        if type(source_digest) is not bytes or len(source_digest) != 32:
            raise ValueError("Exact original policy exception source required")
        raw = _MAGIC + _json(
            dict(operation=self._fields(clock=True), source_digest=source_digest.hex())
        )
        if len(raw) > 8192:
            raise ValueError("Original policy exception facts exceed bounds")
        return raw

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> tuple["ContractRecoveryInput", bytes]:
        raw = facts.business_context
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded original policy exception facts required")
        try:
            obj = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(obj, dict) or set(obj) != {"operation", "source_digest"}:
                raise ValueError
            value = obj["operation"]
            if not isinstance(value, dict) or set(value) != {
                "run_id",
                "organization_id",
                "expected_version",
                "expected_release_fingerprint",
                "value",
                "confirm",
            }:
                raise ValueError
            audit = value["value"]
            if not isinstance(audit, dict) or set(audit) != {
                "execution_id",
                "candidate_id",
                "candidate_fingerprint",
                "output_fingerprint",
                "actor",
                "reason",
                "resolved_at",
                "acknowledged_policy_exception_and_cost",
                "recovery_version",
                "instruction_fingerprint",
            }:
                raise ValueError
            operation = cls(
                UUID(value["run_id"]),
                value["organization_id"],
                value["expected_version"],
                value["expected_release_fingerprint"],
                ContractRecovery(
                    **{**audit, "resolved_at": datetime.fromisoformat(audit["resolved_at"])}
                ),
                value["confirm"],
            )
            source = bytes.fromhex(obj["source_digest"])
            if (
                operation.intent != facts.intent
                or operation.value.resolved_at != facts.at
                or operation.encode(source) != raw
            ):
                raise ValueError
            return operation, source
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen original policy exception facts") from None

    def response(self) -> dict[str, Any]:
        value = self.value
        return dict(
            mode="applied",
            run_id=str(self.run_id),
            expected_version=self.expected_version,
            result_version=self.expected_version + 1,
            execution_id=value.execution_id,
            candidate_id=value.candidate_id,
            output_fingerprint=value.output_fingerprint,
            release_fingerprint=self.expected_release_fingerprint,
            actor=value.actor,
            recovery_version=value.recovery_version,
            instruction_fingerprint=value.instruction_fingerprint,
            reason=value.reason,
            next_action="retry_same_candidate_semantic_once",
            existing_generation_outputs="preserved",
        )


def _source(checkpoint: Any, run: Any, plan: ContractRecoveryPlan) -> bytes:
    return hashlib.sha256(
        b"qs-ai-contract-recovery-source/v1\0"
        + source_snapshot_digest(checkpoint, run)
        + plan.progress_json.encode()
    ).digest()


def read_contract_recovery_plan(
    incoming: ContractRecoveryInput, winner: FrozenPlan
) -> tuple[EvaluationStateFacts, ContractRecoveryInput, bytes, FrozenArchive]:
    facts, archive = read_evaluation_state_plan(incoming.intent, winner)
    first, source = ContractRecoveryInput.from_facts(facts)
    state = pb.MessagingBody.FromString(archive.body).evaluation_state
    if (
        state.version != first.expected_version + 1
        or state.status != "collecting"
        or state.release_fingerprint != first.expected_release_fingerprint
    ):
        raise MessageConflict("Original policy exception projection differs from first plan")
    return facts, first, source, archive


class ContractRecoveryPlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive policy exception database budget required")
        self.transactions, self.database_seconds = transactions, database_seconds
        self.signing_key, self.recipient_key = signing_key, recipient_key

    async def candidate(self, incoming: ContractRecoveryInput) -> FrozenPlan:
        intent = incoming.intent
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            checkpoint, run = await load_preflight_source(db, intent, lock=False)
            planned = await plan_contract_recovery(
                db,
                incoming.run_id,
                incoming.expected_version,
                incoming.organization_id,
                incoming.expected_release_fingerprint,
                incoming.value,
                confirm=incoming.confirm,
            )
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == intent.run_id
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            facts = EvaluationStateFacts(
                intent,
                incoming.value.resolved_at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                incoming.encode(_source(checkpoint, run, planned)),
            )
            visible = evaluation_state(
                intent.run_id,
                {**run, "progress_json": planned.progress(), "version": planned.state.version},
                facts.sequence,
            )
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(incoming.run_id, f"state:{planned.state.version}")),
            intent.run_id,
            pb.MessagingBody(evaluation_state=visible),
            organization_id=intent.organization_id,
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(facts, str(uuid4()), first)
        read_contract_recovery_plan(incoming, winner)
        return winner


async def apply_contract_recovery_state(db: AsyncSession, facts: EvaluationStateFacts) -> None:
    first, source = ContractRecoveryInput.from_facts(facts)
    checkpoint, run = await load_preflight_source(db, facts.intent, lock=True)
    planned = await plan_contract_recovery(
        db,
        first.run_id,
        first.expected_version,
        first.organization_id,
        first.expected_release_fingerprint,
        first.value,
        confirm=first.confirm,
        lock=True,
    )
    if _source(checkpoint, run, planned) != source:
        raise MessageConflict("Original policy exception source differs from first plan")
    state = await authorize_contract_recovery(
        db,
        first.run_id,
        first.expected_version,
        first.organization_id,
        first.expected_release_fingerprint,
        first.value,
        confirm=first.confirm,
    )
    if state != planned.state:
        raise MessageConflict("Original policy exception CAS differs from frozen result")
