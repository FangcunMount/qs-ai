"""One host-owned admission root; no planning, signing, sending or business retry.

The transport prepares all halves before invoking this local executor. Its
business action must use frozen identities and the original borrowed admission
adapters. This module does not assemble or enable the production receiver.
"""

import asyncio
import math
from collections.abc import Awaitable, Callable

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import BindingError, Execution, LocalOutcome, decide_local
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    build_receipt_body,
)
from qs_ai.infrastructure.workflow_transport.messaging import valid_id
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)

AdmissionAction = Callable[
    [AsyncSession, pb.MessagingEnvelope, pb.MessagingBody], Awaitable[AdmissionDecision]
]


class AdmissionRoot:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        inbox: MessagingStore,
        incoming: FrozenCommandInput,
        *,
        reservation_token: str,
        business: AdmissionAction,
        database_seconds: float = 2,
    ) -> None:
        if not valid_id(reservation_token):
            raise BindingError("Frozen Inbox reservation identity required")
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive evidence budget required")
        self._transactions, self._archives, self._store = transactions, archives, store
        self._inbox, self._incoming = inbox, incoming
        self._reservation_token, self._business = reservation_token, business
        self._database_seconds = database_seconds
        self._used = False

    async def _proof(self, execution: Execution) -> LocalOutcome:
        # A new store read boundary, never the former business root's snapshot.
        try:
            async with asyncio.timeout(self._database_seconds):
                evidence = tuple([await self._store.evidence(ref) for ref in execution.references])
                return decide_local(execution.references, evidence)
        except (Exception, asyncio.CancelledError):
            return LocalOutcome.UNKNOWN

    async def __call__(self, execution: Execution) -> LocalOutcome:
        if self._used or not execution.owned:
            raise BindingError("One owned execution per original admission root required")
        archives = read_admission_plan(self._incoming, execution.plan)
        receipt = next(a for a in archives if a.envelope().kind == pb.COMMAND_RECEIPT)
        expected = pb.MessagingBody.FromString(receipt.body)
        self._used = True
        committing, rolled_back, integrity_lost = False, False, False
        try:
            async with self._transactions.open() as db:
                await db.begin()
                original = db.get_transaction()
                try:
                    # First conditional write competes with the same execution's aborter.
                    guard = await self._archives.bind(db, execution)
                    duplicate = await self._inbox.reserve_command(
                        db,
                        self._incoming.envelope(),
                        self._incoming.body,
                        self._incoming.wire,
                        reservation_token=self._reservation_token,
                    )
                    if duplicate is not None:
                        # Already committed transaction requests are resolved before
                        # Execute. A historical Inbox is not new transaction evidence.
                        raise MessageConflict("Original Inbox responsibility already exists")
                    if expected.command_receipt.decision != pb.HELD:
                        result = await self._business(
                            db, self._incoming.envelope(), self._incoming.body_message()
                        )
                        if (
                            result.organization_id != self._incoming.organization_id
                            or result.sequence != receipt.sequence
                            or build_receipt_body(self._incoming.envelope(), result) != expected
                        ):
                            raise MessageConflict("Admission differs from frozen business result")
                    if (
                        original is None
                        or not original.is_active
                        or db.get_transaction() is not original
                    ):
                        integrity_lost = True
                        raise BindingError("Admission replaced the original root")
                    for branch in execution.plan.branches:
                        await guard.put(branch.branch_id)
                    await self._inbox.decide_transaction(
                        db,
                        self._incoming.envelope(),
                        receipt,
                        organization_id=self._incoming.organization_id,
                    )
                    await guard.commit_witness()
                    committing = True
                    await db.commit()  # Only this host boundary owns the root commit.
                    return LocalOutcome.COMMITTED
                except (Exception, asyncio.CancelledError):
                    if not committing:
                        if (
                            original is None
                            or not original.is_active
                            or db.get_transaction() is not original
                        ):
                            integrity_lost = True
                        if not integrity_lost:
                            await db.rollback()
                            rolled_back = True
                    raise
        except (Exception, asyncio.CancelledError):
            if committing or integrity_lost:
                return await self._proof(execution)
            if rolled_back:
                try:
                    async with asyncio.timeout(self._database_seconds):
                        if await self._store.abort(execution):
                            return LocalOutcome.ABORTED
                except (Exception, asyncio.CancelledError):
                    pass
            return LocalOutcome.UNKNOWN
