"""Authenticated command -> one SDK transaction; no automatic Retry/Replan or model redo.

This component is deliberately separate from legacy runtime assembly, which
remains fail-closed until all state and ACK producers have migrated.
"""

import asyncio
import math
from typing import Any, Protocol

from reliable_messaging.durable import MessageConflict
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    FrozenPlan,
    LocalExecutor,
    LocalOutcome,
    Request,
    Reservation,
    Result,
    State,
    decide_local,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from reliable_messaging.transport import Received, physical_topic_v2

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.change_planner import ChangeAdmissionPlanner
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.evaluation_planner import EvaluationAdmissionPlanner
from qs_ai.infrastructure.workflow_transport.hold_plan import freeze_technical_hold
from qs_ai.infrastructure.workflow_transport.messaging import COMMANDS
from qs_ai.infrastructure.workflow_transport.mq_failure import failed_original
from qs_ai.infrastructure.workflow_transport.mq_receiver import CommandReceiver
from qs_ai.infrastructure.workflow_transport.retry_planner import ParticipantRetryPlanner
from qs_ai.infrastructure.workflow_transport.start_planner import StartAdmissionPlanner
from qs_ai.infrastructure.workflow_transport.transaction_admission import (
    AdmissionRoot,
    make_change_root,
    make_evaluation_root,
    make_hold_root,
    make_retry_root,
    make_start_root,
)
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)

COMMAND_TOPIC = physical_topic_v2(COMMANDS, "TRANSACTION", "qs.mysql", "m3-1")


class CommandTransactions(Protocol):
    @property
    def store(self) -> MySQLTransactionStore: ...

    async def reserve(self, request: Request, candidate: FrozenPlan) -> Reservation: ...

    async def execute(self, execution: Execution, local: LocalExecutor) -> Result: ...


class CommandTransactionPending(RuntimeError):
    """Transport must not ACK; the existing execution requires evidence or explicit recovery."""


class TransactionCommandReceiver:
    def __init__(
        self,
        boundary: CommandReceiver,
        admission: WorkflowCommandAdmission,
        runtime: CommandTransactions,
        archives: TransactionArchive,
        *,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive command evidence budget required")
        if boundary.admission is not admission:
            raise ValueError("Original command admission binding required")
        self._boundary, self._admission, self._runtime, self._archives = (
            boundary,
            admission,
            runtime,
            archives,
        )
        self._database_seconds = database_seconds
        # Constructors are inert. Host lifecycle owns clients/pool/loop and worker budgets.
        common: dict[str, Any] = {
            "signing_key": boundary.signing_key,
            "recipient_key": boundary.qs_recipient_key,
        }
        start = StartAdmissionPlanner(
            boundary.transactions,
            admission.source,
            capacity=admission.participant_capacity,
            quota_baseline=admission.quota_baseline,
            models=admission.models,
            **common,
        )
        change = ChangeAdmissionPlanner(
            boundary.transactions,
            admission.source,
            capacity=admission.participant_capacity,
            quota_baseline=admission.quota_baseline,
            **common,
        )
        retry = ParticipantRetryPlanner(
            boundary.transactions,
            admission.source,
            capacity=admission.participant_capacity,
            quota_baseline=admission.quota_baseline,
            **common,
        )
        evaluation = EvaluationAdmissionPlanner(
            boundary.transactions,
            capacity=admission.evaluation_capacity,
            models=admission.models,
            quota_baseline=admission.quota_baseline,
            runtime_limits=admission.runtime_limits,
            **common,
        )
        self._planners: dict[
            int,
            StartAdmissionPlanner
            | ChangeAdmissionPlanner
            | ParticipantRetryPlanner
            | EvaluationAdmissionPlanner,
        ] = {
            pb.START: start,
            pb.CHANGE: change,
            pb.PARTICIPANT_RETRY: retry,
            pb.EVALUATION_START: evaluation,
            pb.EVALUATION_CANCEL: evaluation,
        }

    async def _existing(self, incoming: FrozenCommandInput) -> Any | None:
        async with (
            asyncio.timeout(self._database_seconds),
            self._boundary.transactions.open() as db,
        ):
            return await self._boundary.store.read_command(db, incoming.envelope(), incoming.body)

    async def _committed(self, incoming: FrozenCommandInput, execution: Execution) -> None:
        expected = read_admission_plan(incoming, execution.plan)
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise CommandTransactionPending("Exact original witness remains unresolved")
            for value in expected:
                current = await self._archives.read_committed(value.envelope().message_id)
                if current != value:
                    raise CommandTransactionPending("Exact committed command archive unavailable")
            decision = await self._existing(incoming)
            receipt = next(a for a in expected if a.envelope().kind == pb.COMMAND_RECEIPT)
            if decision is None or decision["receipt_id"] != receipt.envelope().message_id:
                raise CommandTransactionPending("Exact original Inbox decision unavailable")

    def _root(self, incoming: FrozenCommandInput, winner: FrozenPlan) -> AdmissionRoot:
        boundary = self._boundary
        args = (
            boundary.transactions,
            self._archives,
            self._runtime.store,
            boundary.store,
            incoming,
            winner,
            self._admission,
        )
        archives = read_admission_plan(incoming, winner)
        receipt = next(a for a in archives if a.envelope().kind == pb.COMMAND_RECEIPT)
        if pb.MessagingBody.FromString(receipt.body).command_receipt.decision == pb.HELD:
            return make_hold_root(*args[:6], database_seconds=self._database_seconds)
        factories = {
            pb.START: make_start_root,
            pb.CHANGE: make_change_root,
            pb.PARTICIPANT_RETRY: make_retry_root,
            pb.EVALUATION_START: make_evaluation_root,
            pb.EVALUATION_CANCEL: make_evaluation_root,
        }
        return factories[incoming.envelope().kind](*args, database_seconds=self._database_seconds)

    async def receive_command(self, received: Received) -> None:
        if (
            received.provider != "rocketmq"
            or received.logical_topic != COMMANDS
            or received.physical_topic != COMMAND_TOPIC
        ):
            await self._boundary.isolate(received.wire, "transaction_route_mismatch")
            return
        resolved = await self._boundary.resolve_command(received.wire)
        if resolved is None:
            return
        envelope, raw, _ = resolved
        incoming = FrozenCommandInput.from_authenticated(envelope, raw, received.wire)
        try:
            async with asyncio.timeout(self._database_seconds):
                existing = await self._runtime.store.lookup(incoming.request)
            if existing is None:
                if await self._existing(incoming) is not None:
                    # An old committed Inbox remains old evidence. No half,
                    # witness adoption, resealing or business invocation occurs.
                    return
                async with (
                    asyncio.timeout(self._database_seconds),
                    self._boundary.transactions.open() as db,
                ):
                    # This is the existing technical ledger's own short read
                    # transaction, not the guarded business execution.
                    await db.begin()
                    attempts = await self._boundary.store.logical_attempts(db, envelope)
                candidate = (
                    freeze_technical_hold(
                        incoming, self._boundary.signing_key, self._boundary.qs_recipient_key
                    )
                    if attempts >= 8
                    else await self._planners[envelope.kind].candidate(incoming)
                )
                existing = await self._runtime.reserve(incoming.request, candidate)
            if not existing.acquired:
                if existing.state == State.COMMITTED:
                    await self._committed(incoming, existing.execution)
                    return
                raise CommandTransactionPending(
                    "Existing command execution needs evidence or explicit recovery"
                )
            # Always build from the durable winner, never a losing candidate.
            local = self._root(incoming, existing.execution.plan)
            try:
                result = await self._runtime.execute(existing.execution, local)
            except TransactionSendError as error:
                # The fixed SDK carries terminal local proof even when the
                # final RPC or callback reported an error. Do not mislabel a
                # committed business transaction as a new technical failure.
                result = error.result
                if result.local != LocalOutcome.COMMITTED:
                    raise
            if result.local != LocalOutcome.COMMITTED:
                raise CommandTransactionPending(
                    "Command local transaction is not confirmed committed"
                )
            # LocalCommitted + FinalUnknown is legal; exact authority is checked
            # before ACK. The owner Checker retains Broker confirmation duty.
            await self._committed(incoming, existing.execution)
        except MessageConflict:
            await self._boundary.isolate(received.wire, "identity_conflict")
        except Exception:
            async with (
                asyncio.timeout(self._database_seconds),
                self._boundary.transactions.open() as db,
            ):
                await db.begin()
                await self._boundary.store.technical_failure(db, envelope, raw, received.wire)
                await db.commit()
            raise

    async def failed_command(self, wire: bytes) -> None:
        # An untrusted physical delivery count never authorizes a new plan,
        # retry, business call or technical-budget hold. Preserve for audit only.
        try:
            original = failed_original(wire, COMMANDS, rocketmq_topic=COMMAND_TOPIC)
        except ValueError:
            await self._boundary.isolate(wire, "invalid_failure_wire")
            return
        resolved = await self._boundary.resolve_command(original)
        if resolved is not None:
            await self._boundary.isolate(wire, "handler_failed")

    async def invalid_wire(self, wire: bytes, code: str) -> None:
        await self._boundary.isolate(wire, code)
