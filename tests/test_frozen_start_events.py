"""Frozen state transcribes original business history without Seal, PUB or MQ Outbox."""

from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.durable import MessageConflict
from sqlalchemy.dialects import mysql

from qs_ai.application.integration.events import StateEvent
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.workflow_transport import frozen_start_events as frozen
from qs_ai.infrastructure.workflow_transport.transaction_command import freeze_admission_plan
from tests.test_admission_plan import fixture


def recorder_fixture(monkeypatch):
    incoming, receipt, state = fixture()
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2), (state, 2)))
    body = pb.MessagingBody.FromString(state.body).interpretation_state
    event = StateEvent(
        body.event_id,
        body.request_id,
        body.session_id,
        Actor(body.actor.org_id, body.actor.subject_id),
        body.testee_id,
        body.version,
        body.status,
    )
    row = {
        "event_id": event.event_id,
        "session_id": event.session_id,
        "version": event.version,
        "payload": asdict(event),
        "mq_owned": False,
        "delivered": False,
        "attempts": 0,
    }
    root = SimpleNamespace(is_active=True)
    db = SimpleNamespace(
        info={},
        get_transaction=Mock(return_value=root),
        execute=AsyncMock(return_value=SimpleNamespace(rowcount=1)),
    )
    binding = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(frozen, "bind", lambda _: binding)
    selected = Mock()
    selected.mappings.return_value.one_or_none.return_value = row
    updated = SimpleNamespace(rowcount=1)
    db.execute.side_effect = [selected, updated, selected]
    recorder = frozen.FrozenStartEvents(incoming, plan)
    return SimpleNamespace(**locals())


async def test_frozen_state_claims_history_without_creating_another_publication(monkeypatch):
    f = recorder_fixture(monkeypatch)
    assert f.recorder.plan_digest == f.plan.digest
    await f.recorder.record_interpretation(f.db, f.row)
    await f.recorder.flush(f.db)
    assert f.db.execute.await_count == 2
    statement = f.db.execute.await_args.args[0]
    sql = statement.compile(dialect=mysql.dialect())
    assert sql.params["mq_owned"] is True
    assert "interpretation_result_outbox" in str(sql) and "messaging_outbox" not in str(sql)
    assert f.event.event_id in sql.params.values()
    f.row["mq_owned"] = True
    await f.recorder.record_interpretation(f.db, f.row)
    assert f.db.execute.await_count == 3


@pytest.mark.parametrize("changed", ["id", "payload", "delivered", "attempts", "mq_owned"])
async def test_changed_or_old_state_cannot_be_adopted_as_new_transaction_history(
    monkeypatch, changed
):
    f = recorder_fixture(monkeypatch)
    if changed == "id":
        f.row["event_id"] = str(uuid4())
    elif changed == "payload":
        f.row["payload"] = {**f.row["payload"], "status": "blocked"}
    else:
        f.row[changed] = 1 if changed == "attempts" else True
    with pytest.raises(MessageConflict):
        await f.recorder.record_interpretation(f.db, f.row)
    f.db.execute.assert_not_called()


async def test_missing_planned_state_is_not_silently_committed(monkeypatch):
    f = recorder_fixture(monkeypatch)
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)
    f.db.execute.assert_not_called()


async def test_frozen_state_cannot_cross_root_or_start_evaluation_in_commit_hook(monkeypatch):
    f = recorder_fixture(monkeypatch)
    await f.recorder.record_interpretation(f.db, f.row)
    f.db.get_transaction.return_value = SimpleNamespace(is_active=True)
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)
    f.db.get_transaction.return_value = f.root
    f.db.info["evaluation_events"] = {str(uuid4())}
    with pytest.raises(MessageConflict):
        await f.recorder.flush(f.db)


async def test_history_claim_lost_cas_is_a_conflict(monkeypatch):
    f = recorder_fixture(monkeypatch)
    f.updated.rowcount = 0
    with pytest.raises(MessageConflict):
        await f.recorder.record_interpretation(f.db, f.row)
