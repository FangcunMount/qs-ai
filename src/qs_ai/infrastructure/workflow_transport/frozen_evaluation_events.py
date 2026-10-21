"""Keyless evaluation event transcription in the original guarded business root."""

from collections.abc import Mapping
from typing import Any, cast

from reliable_messaging.durable import MessageConflict
from reliable_messaging.sqlalchemy import bind
from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import insert, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.workflow_transport.evaluation_plan import EvaluationPlanFacts
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)


class FrozenEvaluationEvents:
    def __init__(self, incoming: FrozenCommandInput, plan: FrozenPlan) -> None:
        self._facts = EvaluationPlanFacts.from_plan(incoming, plan)
        states = [
            a
            for a in read_admission_plan(incoming, plan)
            if a.envelope().kind == pb.EVALUATION_STATE
        ]
        self._expected = (
            pb.MessagingBody.FromString(states[0].body).evaluation_state if states else None
        )
        self._plan_digest = plan.digest
        self._used = False
        self._db: AsyncSession | None = None
        self._root: AsyncSessionTransaction | None = None

    @property
    def plan_digest(self) -> bytes:
        return self._plan_digest

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        raise MessageConflict("Evaluation cannot transcribe interpretation history")

    async def flush(self, db: AsyncSession) -> None:
        root = db.get_transaction()
        if self._used or root is None or not root.is_active:
            raise MessageConflict("One active original evaluation root required")
        await bind(db).validate()
        self._db, self._root = db, root
        expected = self._expected
        markers = set(db.info.get("evaluation_events", set()))
        if markers != ({expected.run_id} if expected is not None else set()):
            raise MessageConflict("Exact original evaluation transcription required")
        if expected is None:
            self._used = True
            return
        row = (
            (
                await db.execute(
                    select(
                        evaluation_runs.c.organization_id,
                        evaluation_runs.c.definition_json,
                        evaluation_runs.c.progress_json,
                        evaluation_checkpoints.c.version,
                    )
                    .join(
                        evaluation_checkpoints,
                        evaluation_checkpoints.c.run_id == evaluation_runs.c.run_id,
                    )
                    .where(evaluation_runs.c.run_id == expected.run_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise MessageConflict("Original evaluation state missing")
        actual = evaluation_state(expected.run_id, row, expected.event_sequence)
        if actual != expected:
            raise MessageConflict("Original evaluation state differs from first frozen message")
        previous = (
            (
                await db.execute(
                    select(evaluation_sequences)
                    .where(evaluation_sequences.c.run_id == expected.run_id)
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        facts = self._facts
        if previous is None:
            if (facts.previous_sequence, facts.previous_version) != (0, 0):
                raise MessageConflict("Original evaluation sequence evidence missing")
            await db.execute(
                insert(evaluation_sequences).values(run_id=expected.run_id, sequence=0, version=0)
            )
        elif (previous["sequence"], previous["version"]) != (
            facts.previous_sequence,
            facts.previous_version,
        ):
            raise MessageConflict("Original evaluation sequence changed")
        result = await db.execute(
            update(evaluation_sequences)
            .where(
                evaluation_sequences.c.run_id == expected.run_id,
                evaluation_sequences.c.sequence == facts.previous_sequence,
                evaluation_sequences.c.version == facts.previous_version,
            )
            .values(sequence=expected.event_sequence, version=expected.version)
        )
        if cast(CursorResult[Any], result).rowcount != 1:
            raise MessageConflict("Original evaluation sequence CAS failed")
        if db.get_transaction() is not root or not root.is_active:
            raise MessageConflict("Original evaluation root was replaced")
        await bind(db).validate()
        db.info.pop("evaluation_events", None)
        self._used = True
