"""CHANGE planning and original root must execute the same decoded command."""

from uuid import uuid4

import pytest

from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow.command_mapping import change_arguments
from qs_ai.domain.interpretation.model import Actor


@pytest.mark.parametrize("action", ["answer", "cancel"])
def test_change_arguments_preserve_optional_answer_and_original_identity(action):
    value = pb.ChangeCommand(
        actor=pb.Actor(org_id="7", subject_id="9"),
        command_id=str(uuid4()),
        session_id=str(uuid4()),
        action=action,
        expected_version=4,
        question_id=str(uuid4()),
        skip=True,
    )
    actor, session, command, key = change_arguments(value)
    assert (actor, session, key) == (Actor("7", "9"), value.session_id, value.command_id)
    assert command == (
        AnswerCommand(4, value.question_id, None, True) if action == "answer" else CancelCommand(4)
    )
    value.answer = "first answer"
    if action == "answer":
        assert change_arguments(value)[2].answer == "first answer"


@pytest.mark.parametrize("field", ["command_id", "session_id", "action"])
def test_change_mapping_retains_original_invalid_command_rejection(field):
    value = pb.ChangeCommand(command_id=str(uuid4()), session_id=str(uuid4()), action="cancel")
    setattr(value, field, "invalid")
    with pytest.raises(ValueError):
        change_arguments(value)
