"""Host-owned state snapshots staged on their original business transaction."""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid5

import sqlalchemy as sa
from reliable_messaging.durable import HELD, MessageConflict
from reliable_messaging.sqlalchemy import bind
from reliable_messaging.wire import decode
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore, metadata
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_runs,
    result_outbox,
)
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    METADATA,
    prepare,
    valid_id,
    valid_number,
)

# Independent from Run/checkpoint schema: no historical state is synthesized on upgrade.
evaluation_sequences = sa.Table(
    "messaging_evaluation_sequences",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("sequence", sa.BigInteger, nullable=False),
    sa.Column("version", sa.BigInteger, nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


class StateEventRecorder:
    def __init__(
        self,
        store: MessagingStore,
        signing_key: Any,
        recipient_key: Any,
        *,
        result_table: sa.Table = result_outbox,
    ) -> None:
        self.store, self.signing_key, self.recipient_key = store, signing_key, recipient_key
        self.result_table = result_table

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        # Repeated stage_state saves also reuse the first wire without resealing it.
        await self.record_legacy_interpretation(db, row)

    async def record_legacy_interpretation(
        self, db: AsyncSession, locked_row: Mapping[Any, Any]
    ) -> bool:
        """Transfer one explicit original result; caller owns the root commit.

        The caller locks and validates its reviewed source before calling. Relock
        that same row and compare the snapshot, preserving source -> MQ lock order.
        No scans, connections, PUBs, or task creation belong to this entry point.
        True means ownership changed in this transaction, not consumer delivery.
        """
        result_outbox, outbox = self.result_table, self.store.outbox_table
        original = bind(db)
        await original.validate()
        if any(c.name not in locked_row for c in result_outbox.columns):
            raise MessageConflict("Complete original interpretation source required")
        row = (
            (
                await db.execute(
                    sa.select(result_outbox)
                    .where(result_outbox.c.event_id == locked_row["event_id"])
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None or any(row[c.name] != locked_row[c.name] for c in result_outbox.columns):
            raise MessageConflict("Original interpretation source changed")
        try:
            event = workflow.StateEvent(**row["payload"])
        except (TypeError, ValueError):
            raise MessageConflict("Original interpretation identity invalid") from None
        if (
            (event.event_id, event.session_id, event.version)
            != (row["event_id"], row["session_id"], row["version"])
            or not all(valid_id(v) for v in (event.event_id, event.session_id, event.request_id))
            or not valid_number(event.actor.org_id)
            or event.version < 1
        ):
            raise MessageConflict("Original interpretation identity invalid")
        attempts, available_at, created_at = row["attempts"], row["available_at"], row["created_at"]
        if (
            type(attempts) is not int
            or not 0 <= attempts < 2**64
            or not isinstance(available_at, datetime)
            or (created_at is not None and not isinstance(created_at, datetime))
            or any(
                at is not None and at.tzinfo is not None and at.utcoffset() != UTC.utcoffset(at)
                for at in (available_at, created_at)
            )
        ):
            raise MessageConflict("Original interpretation delivery metadata invalid")
        body = pb.MessagingBody(interpretation_state=event)
        raw = body.SerializeToString(deterministic=True)
        first = (
            (
                await db.execute(
                    sa.select(outbox)
                    .where(
                        outbox.c.producer == "qs-ai",
                        outbox.c.destination == "qs-server",
                        outbox.c.message_id == event.event_id,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if first is not None:
            _validate_first_interpretation(first, event, raw)
        elif row["mq_owned"]:
            raise MessageConflict("Owned interpretation requires first durable wire")
        if row["delivered"] or row["mq_owned"]:
            return False  # Never revive delivered rows or replenish an owned MQ budget.
        if first is None:
            original_time = created_at.replace(tzinfo=UTC).isoformat() if created_at else ""
            prepared = prepare(
                pb.INTERPRETATION_STATE,
                event.event_id,
                event.request_id,
                body,
                organization_id=event.actor.org_id,
                signing_key=self.signing_key,
                recipient_key=self.recipient_key,
                original_occurred_at=original_time,
            )
            await self.store.stage(
                db,
                prepared,
                organization_id=event.actor.org_id,
                sequence=event.version,
                ordered=False,
            )
            metadata: dict[str, Any] = {"attempts": attempts, "available_at": available_at}
            if attempts >= 8:
                metadata.update(stage=HELD, error_code="delivery_budget_exhausted")
            await db.execute(
                sa.update(outbox)
                .where(
                    outbox.c.producer == "qs-ai",
                    outbox.c.destination == "qs-server",
                    outbox.c.message_id == event.event_id,
                )
                .values(**metadata)
            )
        elif (first["attempts"], first["available_at"]) != (attempts, available_at) or (
            attempts >= 8
            and (first["stage"], first["error_code"]) != (HELD, "delivery_budget_exhausted")
        ):
            # An unexplained unowned/staged mismatch needs an explicit disposition,
            # not a migration that guesses which side's delivery budget is current.
            raise MessageConflict("Unowned interpretation delivery metadata differs")
        await db.execute(
            sa.update(result_outbox)
            .where(result_outbox.c.event_id == event.event_id)
            .values(mq_owned=True)
        )
        await original.validate()
        return True

    async def flush(self, db: AsyncSession) -> None:
        # Checkpoint is already locked by each mutation; projection writes are now complete.
        for run_id in sorted(db.info.get("evaluation_events", set())):
            await self.record_evaluation(db, run_id)

    async def record_evaluation(self, db: AsyncSession, run_id: str) -> None:
        row = (
            (
                await db.execute(
                    sa.select(
                        evaluation_runs.c.organization_id,
                        evaluation_runs.c.definition_json,
                        evaluation_runs.c.progress_json,
                        evaluation_checkpoints.c.version,
                    )
                    .join(
                        evaluation_checkpoints,
                        evaluation_checkpoints.c.run_id == evaluation_runs.c.run_id,
                    )
                    .where(evaluation_runs.c.run_id == run_id)
                )
            )
            .mappings()
            .one()
        )
        creation = json.loads(row["definition_json"])
        progress = row["progress_json"] or creation
        version = row["version"]
        await db.execute(
            insert(evaluation_sequences)
            .values(run_id=run_id, sequence=0, version=0)
            .on_duplicate_key_update(run_id=evaluation_sequences.c.run_id)
        )
        previous = (
            (
                await db.execute(
                    sa.select(evaluation_sequences)
                    .where(evaluation_sequences.c.run_id == run_id)
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if previous["version"] > version:
            raise RuntimeError("Evaluation event version regressed")
        sequence = previous["sequence"] + (previous["version"] != version)
        state = pb.EvaluationRuntimeState(
            run_id=run_id,
            organization_id=str(row["organization_id"]),
            version=version,
            event_sequence=sequence,
            status=progress["status"],
            release_fingerprint=creation["release_fingerprint"],
        )
        transitions = progress.get("transitions", [])
        # Historical records with no original timestamp retain an explicit empty value.
        at = transitions[-1].get("at", "") if transitions else ""
        prepared = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(UUID(run_id), f"state:{version}")),
            run_id,
            pb.MessagingBody(evaluation_state=state),
            organization_id=state.organization_id,
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=at,
        )
        await self.store.stage(
            db, prepared, organization_id=state.organization_id, sequence=sequence, ordered=False
        )
        await db.execute(
            sa.update(evaluation_sequences)
            .where(evaluation_sequences.c.run_id == run_id)
            .values(sequence=sequence, version=version)
        )


def _validate_first_interpretation(
    row: Mapping[Any, Any], event: workflow.StateEvent, raw: bytes
) -> None:
    if (
        row["body"],
        row["body_sha256"],
        row["kind"],
        row["topic"],
        row["organization_id"],
        row["aggregate_key"],
        row["aggregate_sequence"],
        row["ordered"],
        row["requires_receipt"],
    ) != (
        raw,
        hashlib.sha256(raw).hexdigest(),
        pb.INTERPRETATION_STATE,
        EVENTS,
        int(event.actor.org_id),
        event.request_id,
        event.version,
        False,
        True,
    ):
        raise MessageConflict("First interpretation identity or body differs")
    try:
        wire = row["wire"]
        if not isinstance(wire, bytes) or len(wire) > 262_144:
            raise ValueError
        framed = decode(wire)
        if (
            hashlib.sha256(wire).hexdigest() != row["wire_sha256"]
            or framed.message_id != event.event_id
            or framed.metadata != METADATA
            or not framed.payload
        ):
            raise ValueError
    except ValueError:
        # Storage integrity only: a public recipient key cannot authenticate/decrypt
        # old JWE. The original QS receiver still owns full JOSE authentication.
        raise MessageConflict("First interpretation wire damaged") from None
