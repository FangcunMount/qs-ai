"""Transport boundary awaiting original owner's transaction-bound admission seam."""

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from reliable_messaging.durable import MessageConflict
from reliable_messaging.nsq import Received
from reliable_messaging.protected import ProtectionError, TrustedSigner
from reliable_messaging.sqlalchemy import bind
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.workflow_transport.messaging import (
    ACKS,
    COMMANDS,
    FIELDS,
    MessagingContractError,
    authenticate,
    parse_body,
    prepare,
)
from qs_ai.infrastructure.workflow_transport.mq_failure import failed_original
from qs_ai.infrastructure.workflow_transport.payloads import PayloadResolver


@dataclass(frozen=True)
class AdmissionDecision:
    organization_id: str
    sequence: int
    decision: pb.MessagingDecision
    code: str = ""
    grpc_status_code: int = 0
    workflow_receipt: workflow.Receipt | None = None
    evaluation_receipt: workflow.EvaluationState | None = None


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
        store: MessagingStore,
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

    async def isolate(self, wire: bytes, code: str) -> None:
        async with self.transactions.open() as db:
            await db.begin()
            await self.store.quarantine_wire(db, wire, code)
            await db.commit()  # storage failure propagates: SDK cannot FIN

    async def receive_command(self, received: Received) -> None:
        try:
            envelope = authenticate(
                received.wire,
                COMMANDS,
                decrypt_keys=self.decrypt_keys,
                trusted_signers=self.trusted_signers,
            )
            raw = await self.resolver.body(envelope)
            body = parse_body(envelope, raw)
        except (ProtectionError, MessagingContractError, ValueError):
            await self.isolate(received.wire, "authentication_failed")
            return
        # Fetch timeouts/storage errors are technical Unknown and propagate to REQ.
        try:
            async with self.transactions.open() as db:
                await db.begin()
                original = bind(db)
                existing = await self.store.reserve_command(db, envelope, raw, received.wire)
                if existing is None:
                    attempts = await self.store.logical_attempts(db, envelope)
                    if attempts >= 8:
                        value = getattr(body, FIELDS[envelope.kind])
                        org = (
                            value.actor.org_id
                            if envelope.kind in (pb.START, pb.CHANGE)
                            else str(value.scope.organization_id)
                        )
                        result = AdmissionDecision(
                            org, 1, pb.HELD, "technical_budget_exhausted", 14
                        )
                    else:
                        result = await self.admission.admit(db, envelope, body)
                    await original.validate()  # reject owner commits/replacement/savepoint changes
                    await self._record_decision(db, envelope, result)
                await db.commit()  # business + Inbox + first receipt Outbox commit before FIN
        except MessageConflict:
            await self.isolate(received.wire, "identity_conflict")
        except Exception:
            async with self.transactions.open() as failure_db:
                await failure_db.begin()
                attempts = await self.store.technical_failure(
                    failure_db, envelope, raw, received.wire
                )
                await failure_db.commit()
            if attempts >= 8:
                # Persist a technical hold once; never recursively retry a failed hold.
                async with self.transactions.open() as hold_db:
                    await hold_db.begin()
                    duplicate = await self.store.reserve_command(
                        hold_db, envelope, raw, received.wire
                    )
                    if duplicate is None:
                        value = getattr(body, FIELDS[envelope.kind])
                        org = (
                            value.actor.org_id
                            if envelope.kind in (pb.START, pb.CHANGE)
                            else str(value.scope.organization_id)
                        )
                        await self._record_decision(
                            hold_db,
                            envelope,
                            AdmissionDecision(org, 1, pb.HELD, "technical_budget_exhausted", 14),
                        )
                    await hold_db.commit()
                return
            raise

    async def _record_decision(
        self,
        db: AsyncSession,
        envelope: pb.MessagingEnvelope,
        result: AdmissionDecision,
    ) -> None:
        if (
            result.decision not in (pb.ACCEPTED, pb.REJECTED, pb.HELD)
            or len(result.code.encode()) > 128
            or not 0 <= result.grpc_status_code <= 16
            or (result.workflow_receipt is not None and result.evaluation_receipt is not None)
        ):
            raise ValueError("invalid durable admission decision")
        receipt = pb.MessagingCommandReceipt(
            command_id=envelope.message_id,
            command_body_sha256=envelope.body_sha256,
            decision=result.decision,
            code=result.code,
            grpc_status_code=result.grpc_status_code,
        )
        if result.workflow_receipt is not None:
            receipt.workflow_receipt.CopyFrom(result.workflow_receipt)
        if result.evaluation_receipt is not None:
            receipt.evaluation_receipt.CopyFrom(result.evaluation_receipt)
        message = prepare(
            pb.COMMAND_RECEIPT,
            str(uuid4()),
            envelope.aggregate_key,
            pb.MessagingBody(command_receipt=receipt),
            organization_id=result.organization_id,
            signing_key=self.signing_key,
            recipient_key=self.qs_recipient_key,
            correlation=envelope.message_id,
        )
        await self.store.decide(
            db,
            envelope,
            message,
            organization_id=result.organization_id,
            sequence=result.sequence,
        )

    async def receive_ack(self, received: Received) -> None:
        try:
            envelope = authenticate(
                received.wire,
                ACKS,
                decrypt_keys=self.decrypt_keys,
                trusted_signers=self.trusted_signers,
            )
            raw = await self.resolver.body(envelope)
            async with self.transactions.open() as db:
                await db.begin()
                await self.store.confirm_event(db, envelope, raw)
                await db.commit()
        except (ProtectionError, MessagingContractError, MessageConflict, ValueError):
            await self.isolate(received.wire, "identity_conflict")

    async def invalid_wire(self, wire: bytes, code: str) -> None:
        await self.isolate(wire, code)

    async def failed_command(self, wire: bytes) -> None:
        """Archive physical failure; only a local durable budget can hold a command.

        Never call admission. Claimed NSQ attempts cannot create an Inbox decision
        or exhaust a trusted logical budget. Storage/fetch failures propagate to REQ.
        """
        try:
            original = failed_original(wire, COMMANDS)
            envelope = authenticate(
                original,
                COMMANDS,
                decrypt_keys=self.decrypt_keys,
                trusted_signers=self.trusted_signers,
            )
            raw = await self.resolver.body(envelope)
            body = parse_body(envelope, raw)
        except (ProtectionError, MessagingContractError, ValueError):
            await self.isolate(wire, "invalid_failure_wire")
            return
        try:
            async with self.transactions.open() as db:
                await db.begin()
                await self.store.quarantine_wire(db, wire, "handler_failed")
                if await self.store.logical_attempts(db, envelope) >= 8:
                    existing = await self.store.reserve_command(db, envelope, raw, original)
                    if existing is None:
                        value = getattr(body, FIELDS[envelope.kind])
                        org = (
                            value.actor.org_id
                            if envelope.kind in (pb.START, pb.CHANGE)
                            else str(value.scope.organization_id)
                        )
                        await self._record_decision(
                            db,
                            envelope,
                            AdmissionDecision(org, 1, pb.HELD, "technical_budget_exhausted", 14),
                        )
                await db.commit()
        except MessageConflict:
            await self.isolate(wire, "identity_conflict")

    async def failed_ack(self, wire: bytes) -> None:
        """A failed ACK is unknown, never a substitute for stored confirmation."""
        try:
            original = failed_original(wire, ACKS)
            envelope = authenticate(
                original,
                ACKS,
                decrypt_keys=self.decrypt_keys,
                trusted_signers=self.trusted_signers,
            )
            raw = await self.resolver.body(envelope)
            parse_body(envelope, raw)
        except (ProtectionError, MessagingContractError, ValueError):
            await self.isolate(wire, "invalid_failure_wire")
            return
        await self.isolate(wire, "handler_failed")
