"""Transport boundary awaiting original owner's transaction-bound admission seam."""

from typing import Any, Protocol

import grpc
from reliable_messaging.durable import MessageConflict
from reliable_messaging.protected import ProtectionError, TrustedSigner
from reliable_messaging.transaction import BindingError
from reliable_messaging.transport import Received, physical_topic_v2
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging_observations import record_payload_failure
from qs_ai.infrastructure.persistence.mysql.transaction_receipts import TransactionMessagingStore
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
)
from qs_ai.infrastructure.workflow_transport.messaging import (
    ACKS,
    COMMANDS,
    MessagingContractError,
    authenticate,
    parse_body,
)
from qs_ai.infrastructure.workflow_transport.mq_failure import failed_original
from qs_ai.infrastructure.workflow_transport.payloads import PayloadResolver


class CommandAdmission(Protocol):
    async def admit(
        self,
        db: AsyncSession,
        envelope: pb.MessagingEnvelope,
        body: pb.MessagingBody,
    ) -> AdmissionDecision:
        """Original owner applies existing business rules on db; never commits here.

        Return accepted or deterministic rejected after bounded persistence, without
        waiting for a model. Technical/unknown errors raise; they are not rejections.
        """
        ...


class CommandReceiver:
    def __init__(
        self,
        transactions: Transactions,
        store: TransactionMessagingStore,
        admission: CommandAdmission,
        resolver: PayloadResolver,
        *,
        decrypt_keys: dict[str, Any],
        trusted_signers: dict[str, TrustedSigner],
        signing_key: Any,
        qs_recipient_key: Any,
    ) -> None:
        self.transactions, self.store, self.admission = transactions, store, admission
        self.resolver = resolver
        self.decrypt_keys, self.trusted_signers = dict(decrypt_keys), dict(trusted_signers)
        self.signing_key, self.qs_recipient_key = signing_key, qs_recipient_key

    async def _body(self, envelope: pb.MessagingEnvelope) -> bytes:
        try:
            return await self.resolver.body(envelope)
        except Exception as error:
            if envelope.HasField("payload_reference"):
                kind = "payload_fetch_unavailable"
                if isinstance(error, MessagingContractError):
                    kind = "payload_fetch_reference_mismatch"
                elif (
                    isinstance(error, grpc.RpcError)
                    and error.code() == grpc.StatusCode.PERMISSION_DENIED
                ):
                    kind = "payload_fetch_workload_denied"
                await record_payload_failure(self.transactions, kind)
            raise  # the exact original error still decides isolation/REQ; no FIN here

    async def isolate(self, wire: bytes, code: str) -> None:
        async with self.transactions.open() as db:
            await db.begin()
            await self.store.quarantine_wire(db, wire, code)
            await db.commit()  # storage failure propagates: SDK cannot FIN

    async def resolve_command(
        self, wire: bytes
    ) -> tuple[pb.MessagingEnvelope, bytes, pb.MessagingBody] | None:
        """Authenticate and resolve before any business transaction or half message."""
        try:
            envelope = authenticate(
                wire,
                COMMANDS,
                decrypt_keys=self.decrypt_keys,
                trusted_signers=self.trusted_signers,
            )
            raw = await self._body(envelope)
            return envelope, raw, parse_body(envelope, raw)
        except (ProtectionError, MessagingContractError, ValueError):
            await self.isolate(wire, "authentication_failed")
            return None

    async def receive_command(self, received: Received) -> None:
        raise BindingError("Commands require the current transaction receiver")

    async def failed_command(self, wire: bytes) -> None:
        raise BindingError("Commands require the current transaction receiver")

    async def receive_ack(self, received: Received) -> None:
        if (
            received.provider != "rocketmq"
            or received.logical_topic != ACKS
            or received.physical_topic != physical_topic_v2(ACKS, "TRANSACTION", "qs.mysql", "m3-1")
        ):
            await self.isolate(received.wire, "transaction_route_mismatch")
            return
        try:
            envelope = authenticate(
                received.wire,
                ACKS,
                decrypt_keys=self.decrypt_keys,
                trusted_signers=self.trusted_signers,
            )
            raw = await self._body(envelope)
            async with self.transactions.open() as db:
                await db.begin()
                await self.store.confirm_event(db, envelope, raw)
                await db.commit()
        except (ProtectionError, MessagingContractError, MessageConflict, ValueError):
            await self.isolate(received.wire, "identity_conflict")

    async def invalid_wire(self, wire: bytes, code: str) -> None:
        await self.isolate(wire, code)

    async def failed_ack(self, wire: bytes) -> None:
        """A failed ACK is unknown, never a substitute for stored confirmation."""
        try:
            original = failed_original(
                wire,
                ACKS,
                rocketmq_topic=physical_topic_v2(ACKS, "TRANSACTION", "qs.mysql", "m3-1"),
            )
            envelope = authenticate(
                original,
                ACKS,
                decrypt_keys=self.decrypt_keys,
                trusted_signers=self.trusted_signers,
            )
            raw = await self._body(envelope)
            parse_body(envelope, raw)
        except (ProtectionError, MessagingContractError, ValueError):
            await self.isolate(wire, "invalid_failure_wire")
            return
        await self.isolate(wire, "handler_failed")
