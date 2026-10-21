"""Both retry planning and root must preserve original explicit retry authority."""

from uuid import uuid4

import pytest

from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow.command_mapping import participant_retry_command


def command():
    return pb.ParticipantRetryCommand(
        scope=pb.PublicationScope(organization_id=7, operator_user_id=9),
        session_id=str(uuid4()),
        command_id=str(uuid4()),
        expected_run_id=str(uuid4()),
        expected_version=4,
        reason="explicit confirmed retry",
        confirm=True,
        expected_provider_invocations=1,
        accept_result_unknown_risk=True,
    )


def test_retry_mapping_keeps_original_authorization_and_unknown_result_risk():
    value = command()
    result = participant_retry_command(value)
    assert result.scope.organization_id == 7 and result.scope.operator_user_id == 9
    assert (result.session_id, result.command_id, result.expected_run_id) == (
        value.session_id,
        value.command_id,
        value.expected_run_id,
    )
    assert (
        result.expected_provider_invocations == 1
        and result.confirm
        and result.accept_result_unknown_risk
    )


@pytest.mark.parametrize("wrong", ["confirm", "identity", "invocations", "oversized"])
def test_retry_mapping_does_not_turn_transport_recovery_into_retry_authority(wrong):
    value = command()
    if wrong == "confirm":
        value.confirm = False
    elif wrong == "identity":
        value.expected_run_id = "unknown"
    elif wrong == "invocations":
        value.expected_provider_invocations = 2
    else:
        value.reason = "x" * 8193
    with pytest.raises(ValueError):
        participant_retry_command(value)
