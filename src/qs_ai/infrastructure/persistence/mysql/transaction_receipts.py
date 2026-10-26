"""Business ACK history for committed transaction archives, never a publish queue."""

import asyncio
import math
from typing import Any

import sqlalchemy as sa
from reliable_messaging.durable import MessageConflict
from reliable_messaging.sqlalchemy import bind
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.persistence.mysql.transaction_schema import event_receipts
from qs_ai.infrastructure.workflow_transport.frozen_start_events import same_state_event
from qs_ai.infrastructure.workflow_transport.messaging import parse_body


class TransactionMessagingStore(MessagingStore):
    """Only an absent archive permits the original legacy confirmation path.

    The caller authenticates the QS envelope, owns the original root transaction
    and commits receipt/history changes together. Broker confirmation alone
    never marks an interpretation as delivered.
    """

    def __init__(self, archives: TransactionArchive, *, database_seconds: float = 2) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive business receipt database budget required")
        super().__init__()
        self.archives = archives
        self.database_seconds = database_seconds

    async def confirm_event(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, raw: bytes
    ) -> None:
        async with asyncio.timeout(self.database_seconds):
            await self._confirm_event(db, envelope, raw)

    async def _confirm_event(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, raw: bytes
    ) -> None:
        value = parse_body(envelope, raw).event_acknowledgement
        if envelope.kind != pb.EVENT_ACKNOWLEDGEMENT or envelope.producer != "qs-server":
            raise MessageConflict("Authenticated QS acknowledgement required")
        root = db.get_transaction()
        if root is None or not root.is_active or db.get_nested_transaction() is not None:
            raise MessageConflict("Business ACK requires an active original root")
        original = bind(db)
        await original.validate()
        archived = await self.archives.read_committed(value.event_id)
        await original.validate()
        if archived is None:
            raise MessageConflict("Original transaction event archive not found")
        event = archived.envelope()
        if (
            not archived.requires_receipt
            or event.kind != value.event_kind
            or event.aggregate_key != envelope.aggregate_key
            or event.body_sha256 != value.event_body_sha256
            or event.producer != "qs-ai"
            or event.destination != "qs-server"
            or (
                envelope.HasField("payload_reference")
                and envelope.payload_reference.organization_id != str(archived.organization_id)
            )
        ):
            raise MessageConflict("Original transaction event confirmation identity mismatch")
        # Keep the original history-first lock order. This row remains business
        # history; it is not used as a substitute for committed SDK evidence.
        legacy = None
        if event.kind == pb.INTERPRETATION_STATE:
            legacy = (
                (
                    await db.execute(
                        sa.select(result_outbox)
                        .where(result_outbox.c.event_id == value.event_id)
                        .with_for_update()
                    )
                )
                .mappings()
                .one_or_none()
            )
            state = pb.MessagingBody.FromString(archived.body).interpretation_state
            try:
                valid = (
                    legacy is not None
                    and legacy["mq_owned"] is True
                    and legacy["event_id"] == state.event_id
                    and legacy["session_id"] == state.session_id
                    and legacy["version"] == state.version
                    and event.aggregate_key == state.request_id
                    and same_state_event(workflow.StateEvent(**legacy["payload"]), state)
                )
            except (KeyError, TypeError, ValueError, RecursionError, UnicodeError):
                valid = False
            if not valid:
                raise MessageConflict("Original interpretation confirmation history differs")
        values: dict[str, Any] = {
            "producer": envelope.producer.encode(),
            "ack_id": envelope.message_id.encode(),
            "event_id": event.message_id.encode(),
            "organization_id": archived.organization_id,
            "header": envelope.SerializeToString(deterministic=True),
            "body": raw,
            "outcome": value.outcome,
        }
        statement = insert(event_receipts).values(**values, recorded_at=sa.func.utc_timestamp(6))
        await db.execute(statement.on_duplicate_key_update(ack_id=event_receipts.c.ack_id))
        stored = (
            (
                await db.execute(
                    sa.select(event_receipts)
                    .where(
                        event_receipts.c.producer == values["producer"],
                        event_receipts.c.ack_id == values["ack_id"],
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if any(stored[key] != expected for key, expected in values.items()):
            raise MessageConflict("First business ACK identity cannot be overwritten")
        if (
            value.outcome == pb.MessagingEventAcknowledgement.STORED
            and legacy is not None
            and not legacy["delivered"]
        ):
            await db.execute(
                sa.update(result_outbox)
                .where(result_outbox.c.event_id == value.event_id)
                .values(delivered=True, delivered_at=sa.func.utc_timestamp(6))
            )
        await original.validate()
