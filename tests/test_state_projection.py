"""Protect original visible-state bytes before moving first Seal out of commit hooks."""

import copy
import json
from dataclasses import asdict
from uuid import uuid4

import pytest

from qs_ai.domain.interpretation.model import Actor, Session, Status
from qs_ai.infrastructure.workflow_transport.state_projection import (
    evaluation_state,
    interpretation_state,
    next_evaluation_sequence,
)


def session():
    return Session(str(uuid4()), Actor("7", "user:9"), "12", ("17",), "explain")


def test_completed_projection_requires_original_artifact_and_keeps_its_exact_json():
    value = session()
    value.status, value.version = Status.COMPLETED, 5
    with pytest.raises(ValueError, match="durable artifact"):
        interpretation_state("first-event", "first-request", value)
    artifact = {"explanation": "原成果", "score": 0, "details": [None, False]}
    before = copy.deepcopy((value, artifact))
    event = interpretation_state("first-event", "first-request", value, artifact=artifact)
    assert event.artifact_json == '{"explanation":"原成果","score":0,"details":[null,false]}'
    assert (event.event_id, event.request_id, event.session_id, event.version) == (
        "first-event",
        "first-request",
        value.id,
        5,
    )
    assert (value, artifact) == before


def test_question_and_refusal_projection_does_not_invent_artifact_or_replace_identity():
    value = session()
    value.status, value.failure_code = Status.AWAITING_ANSWER, "original_failure"
    question = {"id": "original-question", "text": "原问题", "can_skip": False}
    first = interpretation_state("first-event", "first-request", value, question, {"unused": 1})
    assert asdict(first) == {
        "event_id": "first-event",
        "request_id": "first-request",
        "session_id": value.id,
        "actor": {"org_id": "7", "subject_id": "user:9"},
        "testee_id": "12",
        "version": 1,
        "status": "awaiting_answer",
        "question_id": "original-question",
        "question": "原问题",
        "can_skip": False,
        "failure_code": "original_failure",
        "artifact_json": "",
    }


@pytest.mark.parametrize("progress", [None, {}, {"status": "collecting"}])
def test_evaluation_projection_keeps_original_creation_fallback_and_first_sequence(progress):
    creation = {"status": "requested", "release_fingerprint": "sha256:" + "a" * 64}
    row = {
        "definition_json": json.dumps(creation),
        "progress_json": progress,
        "organization_id": 7,
        "version": 3,
    }
    before = copy.deepcopy(row)
    actual = evaluation_state("original-run", row, 9)
    assert (actual.run_id, actual.organization_id, actual.version, actual.event_sequence) == (
        "original-run",
        "7",
        3,
        9,
    )
    assert actual.status == (progress or creation)["status"]
    assert actual.release_fingerprint == creation["release_fingerprint"]
    assert row == before


def test_sequence_reuses_same_version_and_rejects_regression_without_new_event():
    assert next_evaluation_sequence(0, 0, 1) == 1
    assert next_evaluation_sequence(4, 2, 2) == 4
    assert next_evaluation_sequence(4, 2, 5) == 5
    with pytest.raises(RuntimeError, match="version regressed"):
        next_evaluation_sequence(4, 5, 2)
