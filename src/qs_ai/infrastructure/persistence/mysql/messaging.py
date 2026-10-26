"""MQ host storage. Every method borrows the caller's original active transaction.

Tables are declared separately so importing an adapter cannot install schema or
alter execution metadata. The host migration/lifecycle chooses when to attach them.
"""

import hashlib
from typing import Any, cast
from uuid import uuid4

import sqlalchemy as sa
from reliable_messaging.durable import (
    MessageConflict,
)
from reliable_messaging.sqlalchemy import bind
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.messaging_observations import (
    observations,
    record_observation,
)
from qs_ai.infrastructure.workflow_transport.messaging import (
    parse_body,
    valid_id,
    valid_number,
)
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive

metadata = sa.MetaData()
# Isolated storage fixtures may install the separate host-owned technical table.
# Importing this declaration never installs schema or opens a connection.
observations.to_metadata(metadata)
TEXT_ID = sa.String(128, collation="utf8mb4_bin")
HASH = mysql.CHAR(64, charset="ascii", collation="ascii_bin")
UTC = mysql.DATETIME(fsp=6)
outbox = sa.Table(
    "messaging_outbox",
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
    sa.Index(
        "idx_messaging_outbox_stage_available_at_message_id", "stage", "available_at", "message_id"
    ),
    sa.Index(
        "idx_messaging_outbox_producer_destination_aggregate_key_ca00d30f",
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
    "messaging_inbox",
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
    "messaging_quarantine",
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
        "logical_producer",
        "logical_message_id",
        name="uk_messaging_quarantine_logical_producer_logical_message_id",
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


class MessagingStore:
    async def read_command(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, raw: bytes
    ) -> Any | None:
        """Read an existing original decision without claiming or creating witness."""
        parse_body(envelope, raw)
        row = (
            (
                await db.execute(
                    sa.select(inbox).where(
                        inbox.c.producer == envelope.producer,
                        inbox.c.message_id == envelope.message_id,
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        if (row["body_sha256"], row["body"], row["kind"], row["aggregate_key"]) != (
            envelope.body_sha256,
            raw,
            envelope.kind,
            envelope.aggregate_key,
        ):
            raise MessageConflict("Original command identity differs")
        if row["decision"] not in ("accepted", "rejected", "held") or not row["receipt_id"]:
            raise RuntimeError("Original command decision remains unresolved")
        return row

    async def reserve_command(
        self,
        db: AsyncSession,
        envelope: pb.MessagingEnvelope,
        raw: bytes,
        wire: bytes,
        *,
        reservation_token: str | None = None,
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
        if reservation_token is not None and (
            not isinstance(reservation_token, str) or not valid_id(reservation_token)
        ):
            raise MessageConflict("original reservation identity invalid")
        # New transaction callers freeze this before half messages/the SQL root.
        # Historical callers retain their original allocation and row semantics.
        token = str(uuid4()) if reservation_token is None else reservation_token
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
        if row["reservation_token"] == token and row["decision"] == "processing":
            return None  # original owner executes once, on this same uncommitted transaction
        if row["decision"] not in ("accepted", "rejected", "held") or not row["receipt_id"]:
            raise MessageConflict("original command has no durable decision")
        await record_observation(db, "duplicate_command")
        return row  # duplicate BEFORE business CAS; never execute or refreeze again

    async def decide_transaction(
        self,
        db: AsyncSession,
        command: pb.MessagingEnvelope,
        receipt: FrozenArchive,
        *,
        organization_id: str,
    ) -> None:
        """Stage only the original Inbox decision; the host archives/witnesses it.

        No legacy Outbox row, signing, sending or root commit belongs here. The
        caller must first bind its SDK execution guard on this same root and
        commit business, exact archive and all witnesses together.
        """
        receipt.validate()
        envelope = receipt.envelope()
        value = parse_body(envelope, receipt.body).command_receipt
        if (
            command.producer != "qs-server"
            or command.destination != "qs-ai"
            or envelope.kind != pb.COMMAND_RECEIPT
            or envelope.producer != "qs-ai"
            or envelope.destination != "qs-server"
            or value.command_id != command.message_id
            or value.command_body_sha256 != command.body_sha256
            or envelope.aggregate_key != command.aggregate_key
            or envelope.correlation_command_id != command.message_id
            or not valid_number(organization_id)
            or str(receipt.organization_id) != organization_id
            or receipt.ordered
            or not receipt.requires_receipt
        ):
            raise MessageConflict("frozen receipt must bind the original command")
        await self._decide(db, command, envelope.message_id, value.decision)

    async def _decide(
        self,
        db: AsyncSession,
        command: pb.MessagingEnvelope,
        receipt_id: str,
        decision: pb.MessagingDecision,
    ) -> None:
        decisions = {pb.ACCEPTED: "accepted", pb.REJECTED: "rejected", pb.HELD: "held"}
        await bind(db).validate()
        result = await db.execute(
            sa.update(inbox)
            .where(
                inbox.c.producer == command.producer,
                inbox.c.message_id == command.message_id,
                inbox.c.body_sha256 == command.body_sha256,
                inbox.c.decision == "processing",
            )
            .values(decision=decisions[decision], receipt_id=receipt_id)
        )
        if cast(CursorResult[Any], result).rowcount != 1:
            raise MessageConflict("decision requires the original reserved command")

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
