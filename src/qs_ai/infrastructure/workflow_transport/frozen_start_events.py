"""Transcribe one original interpretation state against its already frozen first event.

The result row remains business/refusal history. This adapter never signs,
sends, scans, stages a messaging Outbox row, or confirms business delivery.
"""

import json
from collections.abc import Mapping
from typing import Any, cast

from reliable_messaging.durable import MessageConflict
from reliable_messaging.sqlalchemy import bind
from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)


def _artifact_fields(raw: str) -> dict[str, str]:
    """MySQL JSON objects have no key order; an Artifact's field values are strings."""

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("Duplicate original Artifact field")
            value[key] = item
        return value

    if len(raw.encode()) > 131072:
        raise ValueError("Original Artifact exceeds its delivery bound")
    value = json.loads(raw, object_pairs_hook=unique)
    if (
        not isinstance(value, dict)
        or not value
        or any(not isinstance(k, str) or not isinstance(v, str) for k, v in value.items())
    ):
        raise ValueError("Exact original Artifact string fields required")
    return value


class InterpretationStateTranscription:
    """Keyless row transcription; its root adapter must validate the frozen plan."""

    def __init__(self, plan_digest: bytes, expected: workflow.StateEvent | None) -> None:
        if (
            type(plan_digest) is not bytes
            or len(plan_digest) != 32
            or (expected is not None and not isinstance(expected, workflow.StateEvent))
        ):
            raise MessageConflict("Exact frozen interpretation plan digest/state required")
        self._expected = (
            workflow.StateEvent.FromString(expected.SerializeToString())
            if expected is not None
            else None
        )
        self._plan_digest = plan_digest
        self._seen = False
        self._db: AsyncSession | None = None
        self._root: AsyncSessionTransaction | None = None

    @property
    def plan_digest(self) -> bytes:
        return self._plan_digest

    def _check_row(self, row: Mapping[Any, Any]) -> None:
        expected = self._expected
        if expected is None:
            raise MessageConflict("Unplanned interpretation state cannot be recorded")
        try:
            actual = workflow.StateEvent(**row["payload"])
            if expected.status == actual.status == "completed" and expected.artifact_json:
                if _artifact_fields(actual.artifact_json) != _artifact_fields(
                    expected.artifact_json
                ):
                    raise ValueError("Original Artifact fields differ")
                # Compare the remaining exact projection without re-encoding or
                # replacing the immutable first body/wire in its archive.
                actual.artifact_json = expected.artifact_json
            valid = (
                actual == expected
                and row["event_id"] == expected.event_id
                and row["session_id"] == expected.session_id
                and row["version"] == expected.version
                and row["delivered"] is False
                and type(row["attempts"]) is int
                and row["attempts"] == 0
                and type(row["mq_owned"]) is bool
                and (self._seen or row["mq_owned"] is False)
            )
        except (KeyError, TypeError, ValueError, RecursionError, UnicodeError):
            raise MessageConflict("Original interpretation state evidence invalid") from None
        if not valid:
            raise MessageConflict(
                "Original interpretation state differs or has old delivery responsibility"
            )

    async def _validate_root(self, db: AsyncSession) -> None:
        root = db.get_transaction()
        if root is None or not root.is_active:
            raise MessageConflict("Original active interpretation root required")
        if self._db is None:
            self._db, self._root = db, root
        if self._db is not db or self._root is not root:
            raise MessageConflict("Frozen interpretation transcription cannot cross roots")
        await bind(db).validate()

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        self._check_row(row)
        await self._validate_root(db)
        current = (
            (
                await db.execute(
                    select(result_outbox)
                    .where(result_outbox.c.event_id == row["event_id"])
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if current is None:
            raise MessageConflict("Original interpretation history missing")
        self._check_row(current)
        if self._seen:
            return
        result = await db.execute(
            update(result_outbox)
            .where(
                result_outbox.c.event_id == row["event_id"],
                result_outbox.c.session_id == row["session_id"],
                result_outbox.c.version == row["version"],
                result_outbox.c.mq_owned.is_(False),
                result_outbox.c.delivered.is_(False),
                result_outbox.c.attempts == 0,
            )
            .values(mq_owned=True)
        )
        if cast(CursorResult[Any], result).rowcount != 1:
            raise MessageConflict("Original interpretation history claim changed")
        self._seen = True

    async def flush(self, db: AsyncSession) -> None:
        if (self._expected is not None and not self._seen) or db.info.get("evaluation_events"):
            raise MessageConflict("Complete original interpretation transcription required")
        await self._validate_root(db)


class FrozenInterpretationEvents(InterpretationStateTranscription):
    def __init__(self, incoming: FrozenCommandInput, plan: FrozenPlan) -> None:
        archives = read_admission_plan(incoming, plan)
        if incoming.envelope().kind not in (pb.START, pb.CHANGE, pb.PARTICIPANT_RETRY):
            raise ValueError("Original interpretation or CHANGE state transcription required")
        states = [a for a in archives if a.envelope().kind == pb.INTERPRETATION_STATE]
        if len(states) > 1:
            raise ValueError("One original interpretation state per command required")
        expected = (
            pb.MessagingBody.FromString(states[0].body).interpretation_state if states else None
        )
        super().__init__(plan.digest, expected)


class FrozenStartEvents(FrozenInterpretationEvents):
    """Keep the original START-only public boundary and its test seam."""

    def __init__(self, incoming: FrozenCommandInput, plan: FrozenPlan) -> None:
        if incoming.envelope().kind != pb.START:
            raise ValueError("Only original START state transcription supported")
        super().__init__(incoming, plan)
