"""Frozen evaluation events use exact state/sequence CAS without signing or Outbox staging."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.infrastructure.workflow_transport import frozen_evaluation_events as events
from tests.test_evaluation_admission_planner import fixture as planning


def selected(row):
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    return result


async def fixture(monkeypatch):
    f = planning(monkeypatch)
    plan = await f.planner.candidate(f.incoming)
    recorder = events.FrozenEvaluationEvents(f.incoming, plan)
    source = {**f.read.source, "version": 2, "progress_json": {"status": "collecting"}}
    root = SimpleNamespace(is_active=True)
    changed = Mock(rowcount=1)
    db = SimpleNamespace(
        get_transaction=Mock(return_value=root),
        info={"evaluation_events": {str(f.read.scope.run_id)}},
        execute=AsyncMock(side_effect=[selected(source), selected(None), Mock(), changed]),
        commit=AsyncMock(),
    )
    binding = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(events, "bind", Mock(return_value=binding))
    return SimpleNamespace(**locals())


async def test_original_transcription_seeds_and_cas_updates_exact_sequence_without_wire_io(
    monkeypatch,
):
    f = await fixture(monkeypatch)
    await f.recorder.flush(f.db)
    calls = [c.args[0] for c in f.db.execute.await_args_list]
    assert [c.is_select for c in calls] == [True, True, False, False]
    assert calls[1]._for_update_arg is not None and calls[2].is_insert and calls[3].is_update
    assert calls[2].table.name == calls[3].table.name == "messaging_evaluation_sequences"
    assert f.db.info.get("evaluation_events") is None
    f.db.commit.assert_not_called()
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)


@pytest.mark.parametrize("damage", ["markers", "state", "sequence", "cas", "root"])
async def test_wrong_or_missing_evaluation_proof_never_finishes_transcription(monkeypatch, damage):
    f = await fixture(monkeypatch)
    if damage == "markers":
        f.db.info.clear()
    elif damage == "state":
        f.source["progress_json"]["status"] = "canceled"
    elif damage == "sequence":
        f.db.execute.side_effect = [selected(f.source), selected({"sequence": 1, "version": 1})]
    elif damage == "cas":
        f.changed.rowcount = 0
    else:
        f.root.is_active = False
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)
    f.db.commit.assert_not_called()


async def test_evaluation_cannot_claim_interpretation_history(monkeypatch):
    f = await fixture(monkeypatch)
    with pytest.raises(MessageConflict):
        await f.recorder.record_interpretation(f.db, {})
    f.db.execute.assert_not_called()
