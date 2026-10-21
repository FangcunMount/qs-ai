"""Freeze original interpretation finalization; never invoke or retry a model."""

import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.artifact import ArtifactIdentity
from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.domain.interpretation.model import Session, Status
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.schema import jobs, questions, sessions
from qs_ai.infrastructure.workflow_transport.interpretation_state_plan import (
    InterpretationStateFacts,
    InterpretationStateIntent,
    freeze_interpretation_state_plan,
    read_interpretation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare, valid_id
from qs_ai.infrastructure.workflow_transport.state_projection import interpretation_state
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


@dataclass(frozen=True)
class InterpretationFinishInput:
    claim: Claim = field(repr=False)
    result: WorkflowResult = field(repr=False)

    def __post_init__(self) -> None:
        claim, result = self.claim, self.result
        MySQLExecutionStore._validate_finish(result)
        if (
            any(
                not isinstance(v, str) or not valid_id(v)
                for v in (claim.job_id, claim.run_id, claim.session.id)
            )
            or type(claim.fence) is not int
            or not 0 < claim.fence < 2**63
            or claim.session.active_run_id != claim.run_id
            or claim.session.status != Status.RUNNING
            or not isinstance(result.checkpoint_ref, str)
            or len(result.checkpoint_ref) > 128
            or type(result.can_skip) is not bool
            or (result.question is not None and not isinstance(result.question, str))
            or (result.failure_code is not None and not isinstance(result.failure_code, str))
        ):
            raise ValueError("Exact original interpretation finish input required")
        # The server's Claim contains a mutable domain Session. Never let later
        # domain transitions alter an input already used for reservation.
        object.__setattr__(self, "claim", replace(claim, session=replace(claim.session)))
        if result.artifact is not None:
            ArtifactIdentity(claim.run_id, result.artifact.invocation_id, result.artifact.id)

    @property
    def input_hash(self) -> bytes:
        return hashlib.sha256(
            b"qs-ai-interpretation-finish/v1\0"
            + _json({"claim": asdict(self.claim), "result": asdict(self.result)})
        ).digest()

    def intent(self, request_id: str) -> InterpretationStateIntent:
        return InterpretationStateIntent(
            self.claim.session.actor.org_id,
            self.claim.session.id,
            request_id,
            "finish/" + self.claim.run_id,
            self.claim.session.version,
            self.input_hash,
        )


def _source_hash(session: Session, job: Any, question_count: int = 0) -> str:
    # Heartbeats may extend lease_until while halves are prepared. This is not
    # a business-input change. Authority is freshly checked by the original guard.
    return hashlib.sha256(
        _json(
            {
                "session": asdict(session),
                "question_count": question_count,
                "job": {
                    k: job[k]
                    for k in (
                        "id",
                        "session_id",
                        "run_id",
                        "status",
                        "fence_token",
                        "attempt",
                        "answer",
                        "skipped",
                        "question_id",
                    )
                },
            }
        )
    ).hexdigest()


async def _source(
    store: MySQLExecutionStore,
    db: AsyncSession,
    incoming: InterpretationFinishInput,
    *,
    lock: bool,
) -> tuple[Session, str, int]:
    session = await store.read_claim(db, incoming.claim, lock=lock)
    query = select(jobs).where(jobs.c.id == incoming.claim.job_id)
    if lock:
        query = query.with_for_update()
    job = (await db.execute(query)).mappings().one()
    # This new explicit SDK entrypoint binds all original server-side identities.
    # The legacy guard's conditions and behavior are left unchanged.
    if (
        job["session_id"] != session.id
        or job["run_id"] != incoming.claim.run_id
        or job["answer"] != incoming.claim.answer
        or job["skipped"] != incoming.claim.skipped
        or job["question_id"] != incoming.claim.question_id
        or session != incoming.claim.session
    ):
        raise LeaseLost("Original finish source differs from its server claim")
    count = (
        await db.scalar(
            select(func.count()).select_from(questions).where(questions.c.session_id == session.id)
        )
        or 0
    )
    return session, _source_hash(session, job, count), count + 1


async def finish_request_id(
    store: MySQLExecutionStore,
    incoming: InterpretationFinishInput,
) -> str:
    async with store.transactions.open() as db:
        row = (
            (await db.execute(select(sessions).where(sessions.c.id == incoming.claim.session.id)))
            .mappings()
            .one()
        )
        request_id = row["request_id"]
        if (
            str(row["org_id"]) != incoming.claim.session.actor.org_id
            or not isinstance(request_id, str)
            or not valid_id(request_id)
        ):
            raise ValueError("Original scoped interpretation request binding required")
        return request_id


def _projection(
    incoming: InterpretationFinishInput, event_id: str, question_id: str | None
) -> Session:
    session, result = replace(incoming.claim.session), incoming.result
    if result.artifact is not None:
        session.complete()
    elif result.question is not None:
        if question_id is None:
            raise ValueError("First original question identity required")
        session.await_answer(question_id)
    else:
        session.block(result.failure_code or "model_not_connected")
    return session


def read_finish_plan(
    incoming: InterpretationFinishInput,
    intent: InterpretationStateIntent,
    winner: FrozenPlan,
) -> tuple[InterpretationStateFacts, FrozenArchive, str | None]:
    if incoming.intent(intent.request_id) != intent:
        raise ValueError("Original finish intent differs")
    facts, archive = read_interpretation_state_plan(intent, winner)
    try:
        context = json.loads(facts.business_context)
        if (
            set(context)
            != {
                "kind",
                "source_hash",
                "event_id",
                "question_id",
                "question_sequence",
                "checkpoint_ref",
                "failure_code",
                "can_skip",
            }
            or context["kind"] != "interpretation-finish/v1"
        ):
            raise ValueError
        if (
            not isinstance(context["source_hash"], str)
            or len(bytes.fromhex(context["source_hash"])) != 32
            or context["event_id"] != archive.envelope().message_id
            or context["checkpoint_ref"] != incoming.result.checkpoint_ref
            or context["failure_code"] != incoming.result.failure_code
            or context["can_skip"] is not incoming.result.can_skip
            or (context["question_id"] is not None and not valid_id(context["question_id"]))
            or (context["question_id"] is not None) != (incoming.result.question is not None)
            or (
                context["question_sequence"] is not None
                and (
                    type(context["question_sequence"]) is not int
                    or not 0 < context["question_sequence"] < 2**31
                )
            )
            or (context["question_sequence"] is not None) != (incoming.result.question is not None)
            or _json(context) != facts.business_context
        ):
            raise ValueError
        projected = _projection(incoming, context["event_id"], context["question_id"])
        value = interpretation_state(
            context["event_id"],
            intent.request_id,
            projected,
            dict(
                id=context["question_id"],
                text=incoming.result.question,
                can_skip=incoming.result.can_skip,
            )
            if incoming.result.question is not None
            else None,
            asdict(incoming.result.artifact) if incoming.result.artifact is not None else None,
        )
        expected = workflow.StateEvent(**asdict(value))
        actual = pb.MessagingBody.FromString(archive.body).interpretation_state
        if expected != actual:
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise ValueError("Frozen original interpretation finish differs") from None
    return facts, archive, context["question_id"]


class InterpretationFinishPlanner:
    def __init__(
        self,
        store: MySQLExecutionStore,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        self.store, self.signing_key, self.recipient_key = store, signing_key, recipient_key
        self.database_seconds = database_seconds

    async def _source_facts(
        self,
        incoming: InterpretationFinishInput,
        intent: InterpretationStateIntent,
    ) -> tuple[str, int]:
        async with asyncio.timeout(self.database_seconds):
            async with self.store.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                session, source_hash, sequence = await _source(self.store, db, incoming, lock=False)
                request_id = await db.scalar(
                    select(sessions.c.request_id).where(sessions.c.id == session.id)
                )
                if request_id != intent.request_id:
                    raise ValueError("Original interpretation request changed")
        return source_hash, sequence

    async def verify(
        self,
        incoming: InterpretationFinishInput,
        intent: InterpretationStateIntent,
        winner: FrozenPlan,
    ) -> None:
        """Fresh original source check without identity generation or signing."""
        facts, _, _ = read_finish_plan(incoming, intent, winner)
        source_hash, sequence = await self._source_facts(incoming, intent)
        context = json.loads(facts.business_context)
        if source_hash != context["source_hash"] or (
            incoming.result.question is not None and sequence != context["question_sequence"]
        ):
            raise LeaseLost("Original finish retry source differs from its frozen plan")

    async def candidate(
        self,
        incoming: InterpretationFinishInput,
        intent: InterpretationStateIntent,
    ) -> FrozenPlan:
        source_hash, sequence = await self._source_facts(incoming, intent)
        # No connection, locks, identity generation or Seal in the later DB callback.
        event_id, operation_id = str(uuid4()), str(uuid4())
        question_id = str(uuid4()) if incoming.result.question is not None else None
        facts = InterpretationStateFacts(
            intent,
            datetime.now(UTC),
            _json(
                dict(
                    kind="interpretation-finish/v1",
                    source_hash=source_hash,
                    event_id=event_id,
                    question_id=question_id,
                    question_sequence=sequence if incoming.result.question is not None else None,
                    checkpoint_ref=incoming.result.checkpoint_ref,
                    failure_code=incoming.result.failure_code,
                    can_skip=incoming.result.can_skip,
                )
            ),
        )
        session = _projection(incoming, event_id, question_id)
        result = incoming.result
        value = interpretation_state(
            event_id,
            intent.request_id,
            session,
            dict(id=question_id, text=result.question, can_skip=result.can_skip)
            if result.question is not None
            else None,
            asdict(result.artifact) if result.artifact is not None else None,
        )
        first = prepare(
            pb.INTERPRETATION_STATE,
            event_id,
            intent.request_id,
            pb.MessagingBody(interpretation_state=workflow.StateEvent(**asdict(value))),
            organization_id=intent.organization_id,
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_interpretation_state_plan(facts, operation_id, first)
        read_finish_plan(incoming, intent, winner)
        return winner


class InterpretationFinishWrite:
    def __init__(
        self,
        store: MySQLExecutionStore,
        incoming: InterpretationFinishInput,
        intent: InterpretationStateIntent,
        winner: FrozenPlan,
    ) -> None:
        self.facts, _, self.question_id = read_finish_plan(incoming, intent, winner)
        self.store, self.incoming, self.intent = store, incoming, intent
        candidate = incoming.result.artifact
        self.identity = (
            ArtifactIdentity(incoming.claim.run_id, candidate.invocation_id, candidate.id)
            if candidate is not None
            else None
        )

    async def __call__(self, db: AsyncSession, facts: InterpretationStateFacts) -> None:
        if facts != self.facts:
            raise ValueError("Original finish facts changed")
        session, source_hash, sequence = await _source(self.store, db, self.incoming, lock=True)
        context = json.loads(facts.business_context)
        request_id = await db.scalar(
            select(sessions.c.request_id).where(sessions.c.id == session.id).with_for_update()
        )
        if source_hash != context["source_hash"] or request_id != self.intent.request_id:
            raise LeaseLost("Frozen interpretation finish source changed")
        await self.store.finish_in(
            db,
            self.incoming.claim,
            self.incoming.result,
            question_id=self.question_id,
            question_sequence=context["question_sequence"],
            state_event_id=context["event_id"],
            at=facts.at,
            artifact_identity=self.identity,
        )
