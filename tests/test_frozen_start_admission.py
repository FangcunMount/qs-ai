"""The original admission service borrows only the persisted winner's resources."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import BindingError

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.frozen_start_admission import FrozenStartAdmission
from qs_ai.infrastructure.workflow_transport.frozen_start_events import FrozenStartEvents
from tests.test_start_plan_facts import facts_fixture


def fixture():
    incoming, facts, plan = facts_fixture()
    base = WorkflowCommandAdmission(*(SimpleNamespace() for _ in range(6)))
    action = FrozenStartAdmission(incoming, plan, base)
    recorder = FrozenStartEvents(incoming, plan)
    root = SimpleNamespace(is_active=True)
    db = SimpleNamespace(
        info={"state_events": recorder},
        scalar=AsyncMock(return_value=facts.at.date()),
        get_transaction=Mock(return_value=root),
        in_transaction=Mock(return_value=True),
        commit=AsyncMock(),
    )
    action._admission.admit = AsyncMock(return_value="original decision")
    return SimpleNamespace(**locals())


async def test_frozen_action_calls_original_admission_on_same_db_and_exact_original_input():
    f = fixture()
    assert f.action.facts == f.facts
    result = await f.action(f.db, f.incoming.envelope(), f.incoming.body_message())
    assert result == "original decision"
    f.action._admission.admit.assert_awaited_once_with(
        f.db, f.incoming.envelope(), f.incoming.body_message()
    )
    assert f.db.scalar.await_args.args[0].is_select
    f.db.commit.assert_not_called()


async def test_borrowed_service_reuses_original_session_and_leaves_root_commit_with_receiver():
    f = fixture()
    original = Transactions.borrowed(f.db)
    service = f.action._service(original)
    assert service._new_identity("session") == f.facts.session_id
    assert service._new_identity("run") == f.facts.run_id
    async with service.uows.open() as uow:
        assert uow.db is f.db
        await uow.commit()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("wrong", ["envelope", "body", "missing-events", "other-plan"])
async def test_frozen_action_rejects_changed_input_or_transcription_before_business(wrong):
    f = fixture()
    envelope, body = f.incoming.envelope(), f.incoming.body_message()
    if wrong == "envelope":
        envelope.message_id = str(uuid4())
    elif wrong == "body":
        body.start.goal = "changed"
    elif wrong == "missing-events":
        f.db.info.clear()
    else:
        other = replace(f.plan, operation_id=str(uuid4()))
        f.db.info["state_events"] = FrozenStartEvents(f.incoming, other)
    with pytest.raises(BindingError):
        await f.action(f.db, envelope, body)
    f.db.scalar.assert_not_called()
    f.action._admission.admit.assert_not_called()


async def test_utc_budget_day_change_aborts_plan_before_original_business():
    f = fixture()
    f.db.scalar.return_value = f.facts.at.date() + timedelta(days=1)
    with pytest.raises(ConfigurationPlanConflict):
        await f.action(f.db, f.incoming.envelope(), f.incoming.body_message())
    f.action._admission.admit.assert_not_called()
    f.db.commit.assert_not_called()
