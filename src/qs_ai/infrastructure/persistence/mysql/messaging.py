"""MQ host storage. Every method borrows the caller's original active transaction.

Tables are declared separately so importing an adapter cannot install schema or
alter execution metadata. The host migration/lifecycle chooses when to attach them.
"""

import hashlib
from typing import Any, cast
from uuid import uuid4

import sqlalchemy as sa
from reliable_messaging.durable import (
    CONFIRMED,
    STAGED,
    Identity,
    MessageConflict,
    MySQLDurableOutbox,
)
from reliable_messaging.sqlalchemy import bind
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.messaging_observations import (
    observations,
    record_observation,
)
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    PreparedMessage,
    parse_body,
    valid_number,
)

metadata = sa.MetaData()
# Isolated storage fixtures may install the separate host-owned technical table.
# Importing this declaration never installs schema or opens a connection.
observations.to_metadata(metadata)
TEXT_ID = sa.String(128, collation="utf8mb4_bin")
HASH = mysql.CHAR(64, charset="ascii", collation="ascii_bin")
UTC = mysql.DATETIME(fsp=6)
outbox = sa.Table(
    "ai_messaging_outbox",
    metadata,
    sa.Column("producer", sa.String(64, collation="ascii_bin"), primary_key=True),
    sa.Column("destination", sa.String(64, collation="ascii_bin"), primary_key=True),
    sa.Column("message_id", TEXT_ID, primary_key=True),
    sa.Column("body_sha256", HASH, nullable=False),
    sa.Column("body", mysql.MEDIUMBLOB, nullable=False),
    sa.Column("wire", mysql.MEDIUMBLOB, nullable=False),
    sa.Column("wire_sha256", HASH, nullable=False),
    sa.Column("topic", sa.String(64, collation="ascii_bin"), nullable=False),
    sa.Column("kind", sa.Integer, nullable=False),
    sa.Column("organization_id", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("aggregate_key", sa.String(192, collation="utf8mb4_bin"), nullable=False),
    sa.Column("aggregate_sequence", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("ordered", sa.Boolean, nullable=False),
    sa.Column("requires_receipt", sa.Boolean, nullable=False),
    sa.Column("stage", sa.String(32, collation="ascii_bin"), nullable=False),
    sa.Column("attempts", mysql.BIGINT(unsigned=True), nullable=False, server_default="0"),
    sa.Column("available_at", UTC, nullable=False),
    sa.Column("created_at", UTC, nullable=False),
    sa.Column("published_at", UTC),
    sa.Column("confirmed_at", UTC),
    sa.Column(
        "error_code", sa.String(128, collation="ascii_bin"), nullable=False, server_default=""
    ),
    sa.Index("ix_ai_messaging_pending", "stage", "available_at", "message_id"),
    sa.Index(
        "ix_ai_messaging_order",
        "producer",
        "destination",
        "aggregate_key",
        "ordered",
        "aggregate_sequence",
        "stage",
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
inbox = sa.Table(
    "ai_messaging_inbox",
    metadata,
    sa.Column("producer", sa.String(64, collation="ascii_bin"), primary_key=True),
    sa.Column("message_id", TEXT_ID, primary_key=True),
    sa.Column("destination", sa.String(64, collation="ascii_bin"), nullable=False),
    sa.Column("body_sha256", HASH, nullable=False),
    sa.Column("body", mysql.MEDIUMBLOB, nullable=False),
    sa.Column("wire_sha256", HASH, nullable=False),
    sa.Column("kind", sa.Integer, nullable=False),
    sa.Column("aggregate_key", sa.String(192, collation="utf8mb4_bin"), nullable=False),
    sa.Column("reservation_token", sa.String(36), nullable=False),
    sa.Column("decision", sa.String(32), nullable=False),
    sa.Column("receipt_id", TEXT_ID),
    sa.Column("received_at", UTC, nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
quarantine = sa.Table(
    "ai_messaging_quarantine",
    metadata,
    sa.Column("wire_sha256", HASH, primary_key=True),
    sa.Column("wire", mysql.MEDIUMBLOB, nullable=False),
    sa.Column("code", sa.String(128, collation="ascii_bin"), nullable=False),
    # Null for untrusted input. Populate only after JOSE and original body validation.
    sa.Column("logical_producer", sa.String(64, collation="ascii_bin")),
    sa.Column("logical_message_id", TEXT_ID),
    sa.Column("logical_body_sha256", HASH),
    sa.Column("attempts", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("first_seen_at", UTC, nullable=False),
    sa.Column("last_seen_at", UTC, nullable=False),
    sa.UniqueConstraint(
        "logical_producer", "logical_message_id", name="uq_ai_messaging_failure_identity"
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


class MessagingStore:
    def __init__(self) -> None:
        self.outbox = MySQLDurableOutbox(outbox)

    async def stage(
        self,
        db: AsyncSession,
        message: PreparedMessage,
        *,
        organization_id: str,
        sequence: int,
        ordered: bool = False,
    ) -> Any:
        envelope = message.envelope
        parse_body(envelope, message.body)
        if not valid_number(organization_id) or not 0 < sequence < 2**64:
            raise MessageConflict("organization and original sequence required")
        if envelope.producer != "qs-ai" or message.topic != EVENTS or ordered:
            raise MessageConflict("AI publishes only durable events")
        if (
            envelope.HasField("payload_reference")
            and envelope.payload_reference.organization_id != organization_id
        ):
            raise MessageConflict("original reference organization mismatch")
        statement = mysql.insert(outbox).values(
            producer=envelope.producer,
            destination=envelope.destination,
            message_id=envelope.message_id,
            body_sha256=envelope.body_sha256,
            body=message.body,
            wire=message.wire,
            wire_sha256=hashlib.sha256(message.wire).hexdigest(),
            topic=message.topic,
            kind=envelope.kind,
            organization_id=int(organization_id),
            aggregate_key=envelope.aggregate_key,
            aggregate_sequence=sequence,
            ordered=False,
            requires_receipt=True,
            stage=STAGED,
            attempts=0,
            available_at=sa.func.utc_timestamp(6),
            created_at=sa.func.utc_timestamp(6),
            error_code="",
        )
        await bind(db).append(statement.on_duplicate_key_update(message_id=outbox.c.message_id))
        row = (
            (
                await db.execute(
                    sa.select(outbox)
                    .where(
                        outbox.c.producer == envelope.producer,
                        outbox.c.destination == envelope.destination,
                        outbox.c.message_id == envelope.message_id,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if (
            row["body_sha256"],
            row["body"],
            row["kind"],
            row["organization_id"],
            row["aggregate_key"],
            row["aggregate_sequence"],
        ) != (
            envelope.body_sha256,
            message.body,
            envelope.kind,
            int(organization_id),
            envelope.aggregate_key,
            sequence,
        ):
            raise MessageConflict("first persisted event identity conflict")
        return row  # only first wire is retained; repeated identity never overwrites it

    async def reserve_command(
        self,
        db: AsyncSession,
        envelope: pb.MessagingEnvelope,
        raw: bytes,
        wire: bytes,
    ) -> Any | None:
        parse_body(envelope, raw)
        if (
            envelope.producer != "qs-server"
            or envelope.destination != "qs-ai"
            or envelope.kind
            not in (
                pb.START,
                pb.CHANGE,
                pb.PARTICIPANT_RETRY,
                pb.EVALUATION_START,
                pb.EVALUATION_CANCEL,
            )
        ):
            raise MessageConflict("authenticated original command required")
        token = str(uuid4())
        statement = mysql.insert(inbox).values(
            producer=envelope.producer,
            destination=envelope.destination,
            message_id=envelope.message_id,
            body_sha256=envelope.body_sha256,
            body=raw,
            wire_sha256=hashlib.sha256(wire).hexdigest(),
            kind=envelope.kind,
            aggregate_key=envelope.aggregate_key,
            reservation_token=token,
            decision="processing",
            received_at=sa.func.utc_timestamp(6),
        )
        await bind(db).append(statement.on_duplicate_key_update(message_id=inbox.c.message_id))
        row = (
            (
                await db.execute(
                    sa.select(inbox)
                    .where(
                        inbox.c.producer == envelope.producer,
                        inbox.c.message_id == envelope.message_id,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if (row["body_sha256"], row["body"], row["kind"], row["aggregate_key"]) != (
            envelope.body_sha256,
            raw,
            envelope.kind,
            envelope.aggregate_key,
        ):
            raise MessageConflict("original command identity conflict")
        if row["reservation_token"] == token:
            return None  # original owner executes once, on this same uncommitted transaction
        if row["decision"] not in ("accepted", "rejected", "held") or not row["receipt_id"]:
            raise MessageConflict("original command has no durable decision")
        await record_observation(db, "duplicate_command")
        return row  # duplicate BEFORE business CAS; never execute or refreeze again

    async def decide(
        self,
        db: AsyncSession,
        command: pb.MessagingEnvelope,
        receipt: PreparedMessage,
        *,
        organization_id: str,
        sequence: int,
    ) -> None:
        value = parse_body(receipt.envelope, receipt.body).command_receipt
        decisions = {pb.ACCEPTED: "accepted", pb.REJECTED: "rejected", pb.HELD: "held"}
        if (
            receipt.envelope.kind != pb.COMMAND_RECEIPT
            or value.command_id != command.message_id
            or value.command_body_sha256 != command.body_sha256
        ):
            raise MessageConflict("durable receipt must bind the original command")
        await self.stage(db, receipt, organization_id=organization_id, sequence=sequence)
        await bind(db).validate()
        result = await db.execute(
            sa.update(inbox)
            .where(
                inbox.c.producer == command.producer,
                inbox.c.message_id == command.message_id,
                inbox.c.body_sha256 == command.body_sha256,
                inbox.c.decision == "processing",
            )
            .values(decision=decisions[value.decision], receipt_id=receipt.envelope.message_id)
        )
        if cast(CursorResult[Any], result).rowcount != 1:
            raise MessageConflict("decision requires the original reserved command")

    async def confirm_event(
        self,
        db: AsyncSession,
        envelope: pb.MessagingEnvelope,
        raw: bytes,
    ) -> None:
        value = parse_body(envelope, raw).event_acknowledgement
        if envelope.kind != pb.EVENT_ACKNOWLEDGEMENT or envelope.producer != "qs-server":
            raise MessageConflict("authenticated QS acknowledgement required")
        identity = Identity("qs-ai", "qs-server", value.event_id)
        await bind(db).validate()
        # Handoff locks the legacy row before staging the MQ row. Keep that
        # order here too, including duplicate ACKs, to avoid an ABBA deadlock.
        legacy = None
        if value.event_kind == pb.INTERPRETATION_STATE:
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
        row = (
            (
                await db.execute(
                    sa.select(outbox)
                    .where(
                        outbox.c.producer == identity.producer,
                        outbox.c.destination == identity.destination,
                        outbox.c.message_id == identity.message_id,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["kind"] != value.event_kind
            or row["aggregate_key"] != envelope.aggregate_key
            or row["body_sha256"] != value.event_body_sha256
            or not row["requires_receipt"]
        ):
            raise MessageConflict("original event confirmation identity mismatch")
        if value.outcome == pb.MessagingEventAcknowledgement.STORED:
            if legacy is not None:
                original = pb.MessagingBody.FromString(row["body"]).interpretation_state
                try:
                    same_body = workflow.StateEvent(**legacy["payload"]) == original
                except (TypeError, ValueError):
                    same_body = False
                if (
                    not legacy["mq_owned"]
                    or legacy["event_id"] != original.event_id
                    or legacy["session_id"] != original.session_id
                    or legacy["version"] != original.version
                    or row["aggregate_key"] != original.request_id
                    or not same_body
                ):
                    raise MessageConflict("original result confirmation identity mismatch")
            if row["stage"] == CONFIRMED:
                # Preserve the first business-confirmation time; the exact hash
                # and identity were validated on this same locked original row.
                await record_observation(db, "duplicate_ack")
            else:
                await self.outbox.confirm(db, identity, value.event_body_sha256)
            if legacy is not None and not legacy["delivered"]:
                await db.execute(
                    sa.update(result_outbox)
                    .where(result_outbox.c.event_id == value.event_id)
                    .values(delivered=True, delivered_at=sa.func.utc_timestamp(6))
                )
        else:
            await self.outbox.hold(
                db, identity, value.event_body_sha256, error_code="receiver_held"
            )

    async def quarantine_wire(self, db: AsyncSession, wire: bytes, code: str) -> None:
        if len(wire) > 262_144 or code not in (
            "invalid_wire",
            "invalid_failure_wire",
            "failure_wire_oversize",
            "authentication_failed",
            "identity_conflict",
            "handler_failed",
        ):
            raise ValueError("bounded wire and fixed isolation code required")
        now = sa.func.utc_timestamp(6)
        statement = mysql.insert(quarantine).values(
            wire_sha256=hashlib.sha256(wire).hexdigest(),
            wire=wire,
            code=code,
            attempts=1,
            first_seen_at=now,
            last_seen_at=now,
        )
        await bind(db).append(
            statement.on_duplicate_key_update(
                attempts=quarantine.c.attempts + 1,
                last_seen_at=now,
            )
        )

    async def logical_attempts(self, db: AsyncSession, command: pb.MessagingEnvelope) -> int:
        await bind(db).validate()
        row = (
            (
                await db.execute(
                    sa.select(quarantine)
                    .where(
                        quarantine.c.logical_producer == command.producer,
                        quarantine.c.logical_message_id == command.message_id,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return 0
        if row["logical_body_sha256"] != command.body_sha256:
            raise MessageConflict("original logical failure identity conflict")
        return min(8, int(row["attempts"]))

    async def technical_failure(
        self,
        db: AsyncSession,
        command: pb.MessagingEnvelope,
        raw: bytes,
        wire: bytes,
    ) -> int:
        # This ledger commits after the failed admission transaction rolled back.
        # It is never a durable business acceptance or model retry authorization.
        parse_body(command, raw)
        await self.logical_attempts(db, command)
        now = sa.func.utc_timestamp(6)
        statement = mysql.insert(quarantine).values(
            wire_sha256=hashlib.sha256(wire).hexdigest(),
            wire=wire,
            code="handler_failed",
            logical_producer=command.producer,
            logical_message_id=command.message_id,
            logical_body_sha256=command.body_sha256,
            attempts=1,
            first_seen_at=now,
            last_seen_at=now,
        )
        await bind(db).append(
            statement.on_duplicate_key_update(
                attempts=sa.func.least(8, quarantine.c.attempts + 1),
                last_seen_at=now,
                code="handler_failed",
                logical_producer=command.producer,
                logical_message_id=command.message_id,
                logical_body_sha256=command.body_sha256,
            )
        )
        return await self.logical_attempts(db, command)

    async def payload(
        self,
        db: AsyncSession,
        reference: pb.MessagePayloadReference,
        authenticated_workload: str,
    ) -> bytes:
        await bind(db).validate()
        if (
            authenticated_workload != "qs-server"
            or reference.producer != "qs-ai"
            or reference.destination != authenticated_workload
            or not valid_number(reference.organization_id)
        ):
            raise MessageConflict("payload workload or reference rejected")
        row = (
            (
                await db.execute(
                    sa.select(outbox).where(
                        outbox.c.producer == reference.producer,
                        outbox.c.destination == reference.destination,
                        outbox.c.message_id == reference.message_id,
                        outbox.c.organization_id == int(reference.organization_id),
                        outbox.c.body_sha256 == reference.body_sha256,
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or len(row["body"]) != reference.body_length
            or hashlib.sha256(row["body"]).hexdigest() != reference.body_sha256
        ):
            raise MessageConflict("original payload unavailable or mismatched")
        return bytes(row["body"])
