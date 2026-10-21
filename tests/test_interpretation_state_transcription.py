"""Standalone interpretation state transcribes a validated plan, without a fake command."""

from unittest.mock import Mock

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.infrastructure.workflow_transport import frozen_start_events as module
from tests.test_frozen_start_events import recorder_fixture


async def test_standalone_state_copies_first_projection_and_never_parses_a_fabricated_command(
    monkeypatch,
):
    p = recorder_fixture(monkeypatch)
    command_reader = Mock(side_effect=AssertionError("Standalone state has no command receipt"))
    monkeypatch.setattr(module, "read_admission_plan", command_reader)
    recorder = module.InterpretationStateTranscription(p.plan.digest, p.body)
    p.body.status = "blocked"
    await recorder.record_interpretation(p.db, p.row)
    await recorder.flush(p.db)
    assert (
        recorder.plan_digest == p.plan.digest
        and recorder._expected.status == p.row["payload"]["status"]
    )
    assert p.db.execute.await_count == 2 and p.binding.validate.await_count == 2
    command_reader.assert_not_called()


async def test_explicit_no_event_transcription_cannot_adopt_an_existing_history_row(monkeypatch):
    p = recorder_fixture(monkeypatch)
    recorder = module.InterpretationStateTranscription(p.plan.digest, None)
    await recorder.flush(p.db)
    p.db.execute.assert_not_called()
    with pytest.raises(MessageConflict):
        await recorder.record_interpretation(p.db, p.row)
    p.db.execute.assert_not_called()


@pytest.mark.parametrize("digest", [b"", None, bytearray(b"a" * 32)])
def test_standalone_transcription_requires_exact_immutable_plan_identity(digest):
    with pytest.raises(MessageConflict):
        module.InterpretationStateTranscription(digest, None)
