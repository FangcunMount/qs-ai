"""Immutable original-root archives; no publishing, scheduling or pool ownership."""

import hashlib
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from reliable_messaging.transaction import (
    Binding,
    BindingError,
    Decision,
    Execution,
    Reference,
    branch_key,
    decode_reference,
    encode_reference,
    execution_key,
    resolve,
)
from reliable_messaging.transaction_mysql import MySQLTransactionGuard, MySQLTransactionStore
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.messaging import MessagingContractError
from qs_ai.infrastructure.workflow_transport.transaction_archive import (
    FrozenArchive,
    decode_archive,
    decode_branch,
)

if TYPE_CHECKING:
    from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore


def _values(e: Execution, branch_id: str) -> dict[str, Any]:
    branch = next((b for b in e.plan.branches if b.branch_id == branch_id), None)
    if branch is None:
        raise BindingError("archive branch missing from reservation winner")
    value = decode_branch(branch)
    envelope = value.envelope()
    if envelope.producer != "qs-ai" or envelope.destination != "qs-server":
        raise MessagingContractError("archive producer differs")
    reference = e.plan.reference(branch_id, e.fence, e.writer_epoch)
    return {
        "producer": envelope.producer.encode(),
        "destination": envelope.destination.encode(),
        "message_id": envelope.message_id.encode(),
        "organization_id": value.organization_id,
        "frozen_payload": branch.message.payload,
        "first_wire": branch.wire,
        "execution_key": execution_key(
            reference.owner, reference.operation_id, reference.writer_epoch, reference.fence
        ),
        "branch_key": branch_key(branch_id),
        "reference": encode_reference(reference),
    }


class ArchiveGuard:
    """The SDK guard owns root validation, not root commit or rollback."""

    def __init__(self, guard: MySQLTransactionGuard) -> None:
        self._guard = guard

    async def put(self, branch_id: str) -> None:
        await self._guard.validate()
        values = _values(self._guard.execution, branch_id)
        from sqlalchemy.dialects.mysql import insert

        statement = insert(archive).values(**values)
        await self._guard.db.execute(
            statement.on_duplicate_key_update(message_id=archive.c.message_id)
        )
        row = (
            (
                await self._guard.db.execute(
                    sa.select(archive).where(
                        archive.c.producer == values["producer"],
                        archive.c.destination == values["destination"],
                        archive.c.message_id == values["message_id"],
                    )
                )
            )
            .mappings()
            .one()
        )
        if dict(row) != values:
            raise MessagingContractError("original archive cannot be overwritten")

    async def commit_witness(self) -> None:
        """Stage only. Any failure requires the host to roll back the whole root."""
        await self._guard.commit_witness()


class TransactionArchive:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], binding: Binding) -> None:
        if binding.owner != "ai.mysql":
            raise BindingError("archive owner must be ai.mysql")
        self._sessions = sessions
        self._store = MySQLTransactionStore(sessions, binding)

    async def bind(self, db: AsyncSession, execution: Execution) -> ArchiveGuard:
        # Must precede business writes in the host's original root transaction.
        return ArchiveGuard(await self._store.bind(db, execution))

    async def read_committed(self, message_id: str) -> FrozenArchive | None:
        async with self._sessions() as db:
            row = (
                (
                    await db.execute(
                        sa.select(archive).where(
                            archive.c.producer == b"qs-ai",
                            archive.c.destination == b"qs-server",
                            archive.c.message_id == message_id.encode(),
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        # Presence blocks legacy fallback even if proof is prepared or damaged.
        value, reference = self._validate_row(dict(row), message_id)
        evidence = await self._store.evidence(reference)
        if resolve(reference, evidence) != Decision.COMMIT:
            raise MessagingContractError("archive has no exact committed witness")
        return value

    def _validate_row(
        self, row: Mapping[str, Any], message_id: str
    ) -> tuple[FrozenArchive, Reference]:
        ref = decode_reference(bytes(row["reference"]))
        value = decode_archive(bytes(row["frozen_payload"]), bytes(row["first_wire"]))
        message = value.message()
        envelope = value.envelope()
        if (
            ref.owner != self._store.binding.owner
            or ref.route_revision != "m3-1"
            or ref.scope != message.scope
            or ref.logical_topic != message.destination
            or ref.message_id != message_id
            or ref.message_fingerprint != bytes.fromhex(message.fingerprint)
            or ref.wire_hash != hashlib.sha256(value.wire).digest()
            or envelope.message_id != message_id
            or envelope.producer != "qs-ai"
            or envelope.destination != "qs-server"
            or row["organization_id"] != value.organization_id
            or bytes(row["execution_key"])
            != execution_key(ref.owner, ref.operation_id, ref.writer_epoch, ref.fence)
            or bytes(row["branch_key"]) != branch_key(ref.branch_id)
        ):
            raise MessagingContractError("archive exact reference differs")
        return value, ref

    async def payload(
        self, reference: pb.MessagePayloadReference, authenticated_workload: str
    ) -> bytes | None:
        """Only None permits legacy fallback; invalid/uncommitted rows raise."""
        if (
            authenticated_workload != "qs-server"
            or reference.producer != "qs-ai"
            or reference.destination != "qs-server"
        ):
            raise MessagingContractError("archive workload rejected")
        value = await self.read_committed(reference.message_id)
        if value is None:
            return None
        envelope = value.envelope()
        if (
            reference.organization_id != str(value.organization_id)
            or reference.body_sha256 != envelope.body_sha256
            or reference.body_length != len(value.body)
        ):
            raise MessagingContractError("archive exact payload differs")
        return value.body


class ArchivePayloadReader:
    """Explicit versioned reader; legacy data never becomes transaction evidence."""

    def __init__(self, current: TransactionArchive, legacy: "MessagingStore") -> None:
        self._current, self._legacy = current, legacy

    async def payload(
        self, db: AsyncSession, reference: pb.MessagePayloadReference, workload: str
    ) -> bytes:
        value = await self._current.payload(reference, workload)
        if value is not None:
            return value
        return await self._legacy.payload(db, reference, workload)
