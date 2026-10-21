"""Pure visible-state projections; callers own reads, identities and first Seal.

Planning and original-root verification use these same business projections.
No database, clock, UUID generation or transport belongs in this module.
"""

import json
from collections.abc import Mapping
from typing import Any

from qs_ai.application.integration.events import StateEvent
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.interpretation.model import Session, Status


def interpretation_state(
    event_id: str,
    request_id: str,
    session: Session,
    question: Mapping[Any, Any] | None = None,
    artifact: Mapping[Any, Any] | None = None,
) -> StateEvent:
    artifact_json = ""
    if session.status == Status.COMPLETED:
        if artifact is None:
            raise ValueError("Completed session requires a durable artifact")
        artifact_json = json.dumps(artifact, ensure_ascii=False, separators=(",", ":"))
    return StateEvent(
        event_id,
        request_id,
        session.id,
        session.actor,
        session.testee_id,
        session.version,
        session.status,
        question_id=question["id"] if question else "",
        question=question["text"] if question else "",
        can_skip=question["can_skip"] if question else False,
        failure_code=session.failure_code or "",
        artifact_json=artifact_json,
    )


def next_evaluation_sequence(previous_sequence: int, previous_version: int, version: int) -> int:
    if previous_version > version:
        raise RuntimeError("Evaluation event version regressed")
    return previous_sequence + (previous_version != version)


def evaluation_state(
    run_id: str, row: Mapping[Any, Any], sequence: int
) -> pb.EvaluationRuntimeState:
    creation = json.loads(row["definition_json"])
    progress = row["progress_json"] or creation
    return pb.EvaluationRuntimeState(
        run_id=run_id,
        organization_id=str(row["organization_id"]),
        version=row["version"],
        event_sequence=sequence,
        status=progress["status"],
        release_fingerprint=creation["release_fingerprint"],
    )
