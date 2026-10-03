"""Host-owned state snapshots staged on their original business transaction."""

import json
from collections.abc import Mapping
from datetime import UTC
from typing import Any
from uuid import UUID, uuid5

import sqlalchemy as sa
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
from qs_ai.infrastructure.workflow_transport.messaging import prepare

# Independent from Run/checkpoint schema: no historical state is synthesized on upgrade.
evaluation_sequences = sa.Table(
    "ai_messaging_evaluation_sequences",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("sequence", sa.BigInteger, nullable=False),
    sa.Column("version", sa.BigInteger, nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


class StateEventRecorder:
    def __init__(self, store: MessagingStore, signing_key: Any, recipient_key: Any) -> None:
        self.store, self.signing_key, self.recipient_key = store, signing_key, recipient_key

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        payload = row["payload"]
        event = workflow.StateEvent(**payload)
        at = row["created_at"]
        original_time = at.replace(tzinfo=UTC).isoformat() if at is not None else ""
        prepared = prepare(
            pb.INTERPRETATION_STATE,
            event.event_id,
            event.request_id,
            pb.MessagingBody(interpretation_state=event),
            organization_id=event.actor.org_id,
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=original_time,
        )
        await self.store.stage(
            db, prepared, organization_id=event.actor.org_id, sequence=event.version, ordered=False
        )
        # Ownership is transferred, not consumer-confirmed. Only the MQ ACK confirms delivery.
        await db.execute(
            sa.update(result_outbox)
            .where(result_outbox.c.event_id == event.event_id)
            .values(mq_owned=True)
        )

    async def handoff(self, db: AsyncSession, limit: int = 20) -> int:
        if not 1 <= limit <= 100:
            raise ValueError("Handoff batch limit must be 1..100")
        rows = (
            (
                await db.execute(
                    sa.select(result_outbox)
                    .where(
                        result_outbox.c.delivered.is_(False), result_outbox.c.mq_owned.is_(False)
                    )
                    .order_by(result_outbox.c.created_at, result_outbox.c.event_id)
                    .limit(limit)
                    .with_for_update()
                )
            )
            .mappings()
            .all()
        )
        for row in rows:
            await self.record_interpretation(db, row)
        return len(rows)

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
