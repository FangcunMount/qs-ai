"""State-only transcription competes on the original sequence without an Inbox."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.infrastructure.workflow_transport import frozen_evaluation_events as events
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_state_plan import fixture as planning


def selected(row):
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    return result


def fixture(monkeypatch):
    f = planning()
    row = {
        "organization_id": 7,
        "definition_json": json.dumps({"release_fingerprint": f.state.release_fingerprint}),
        "progress_json": {"status": "collecting"},
        "version": 3,
    }
    previous = {"sequence": 8, "version": 2}
    original = SimpleNamespace(is_active=True)
    changed = Mock(rowcount=1)
    db = SimpleNamespace(
        info={"evaluation_events": {f.intent.run_id}},
        get_transaction=Mock(return_value=original),
        execute=AsyncMock(side_effect=[selected(row), selected(previous), changed]),
        commit=AsyncMock(),
    )
    binding = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(events, "bind", Mock(return_value=binding))
    recorder = events.FrozenEvaluationStateEvents(f.intent, f.plan, route_resolver=legacy_routes())
    return SimpleNamespace(**locals())


async def test_noncommand_winner_transcribes_only_original_state_and_sequence_without_commit(
    monkeypatch,
):
    f = fixture(monkeypatch)
    assert not f.db.execute.called
    await f.recorder.flush(f.db)
    assert "evaluation_events" not in f.db.info
    assert f.db.execute.await_count == 3
    statements = [c.args[0] for c in f.db.execute.await_args_list]
    assert statements[1]._for_update_arg is not None and statements[2].is_update
    assert statements[2].table.name == "messaging_evaluation_sequences"
    f.db.commit.assert_not_called()


@pytest.mark.parametrize(
    "damage", ["state", "version", "organization", "sequence", "missing", "cas", "markers"]
)
async def test_changed_or_missing_original_projection_cannot_commit_frozen_state(
    monkeypatch, damage
):
    f = fixture(monkeypatch)
    if damage == "state":
        f.row["progress_json"] = {"status": "blocked"}
    elif damage == "version":
        f.row["version"] = 4
    elif damage == "organization":
        f.row["organization_id"] = 8
    elif damage == "sequence":
        f.previous["sequence"] = 9
    elif damage == "missing":
        f.db.execute.side_effect = [selected(f.row), selected(None)]
    elif damage == "cas":
        f.changed.rowcount = 0
    else:
        f.db.info["evaluation_events"].add("unplanned-run")
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)
    f.db.commit.assert_not_called()


async def test_failed_transcription_cannot_move_its_plan_to_a_different_original_root(monkeypatch):
    f = fixture(monkeypatch)
    f.row["version"] = 4
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)
    f.row["version"] = 3
    f.db.get_transaction.return_value = SimpleNamespace(is_active=True)
    f.db.execute.reset_mock(side_effect=True)
    f.db.execute.side_effect = [selected(f.row), selected(f.previous), f.changed]
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)
    f.db.execute.assert_not_called()
