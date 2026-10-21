"""Retry authority stays bound to the original participant and exact source audit."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.transaction import BindingError

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.frozen_change_admission import FrozenChangeAdmission
from qs_ai.infrastructure.workflow_transport.frozen_retry_admission import FrozenRetryAdmission
from qs_ai.infrastructure.workflow_transport.frozen_start_events import FrozenInterpretationEvents
from tests.test_participant_retry_planner import fixture as planning


async def fixture(monkeypatch):
    f = planning(monkeypatch)
    plan = await f.planner.candidate(f.incoming)
    base = WorkflowCommandAdmission(f.read.source, *(SimpleNamespace() for _ in range(5)))
    action = FrozenRetryAdmission(f.incoming, plan, base)
    db = f.read.f.db
    db.info = {"state_events": FrozenInterpretationEvents(f.incoming, plan)}
    db.scalar = AsyncMock(return_value=f.at.replace(tzinfo=None))
    db.in_transaction = Mock(return_value=True)
    action._admission.admit = AsyncMock(return_value="original retry decision")
    return SimpleNamespace(**locals())


async def test_root_uses_current_original_participant_without_calling_remote_client(monkeypatch):
    f = await fixture(monkeypatch)
    f.f.read.source.authorize.side_effect = AssertionError("no remote call in root")
    assert (
        await f.action(f.db, f.f.incoming.envelope(), f.f.incoming.body_message())
        == "original retry decision"
    )
    assert f.db.execute.await_args.args[0]._for_update_arg is not None
    assert f.f.read.source.authorize.await_count == 1
    store = f.action._store(Transactions.borrowed(f.db))
    assert store._new_run() == f.action.facts.run_id
    from qs_ai.infrastructure.persistence.mysql import retry_admission as adapter
    from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork

    monkeypatch.setattr(adapter, "MySQLUnitOfWork", MySQLUnitOfWork)
    uow = store._unit(f.db)
    assert uow.db is f.db and uow._job_id == f.action.facts.job_id
    await uow.commit()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("wrong", ["target", "owner", "expired", "recorder"])
async def test_wrong_participant_or_expired_observation_cannot_authorize_retry(monkeypatch, wrong):
    from qs_ai.domain.interpretation.model import Actor

    f = await fixture(monkeypatch)
    if wrong == "target":
        f.f.read.f.session.testee_id = "20"
    elif wrong == "owner":
        f.f.read.f.session.actor = Actor("7", "11")
    elif wrong == "expired":
        f.db.scalar.return_value += timedelta(seconds=61)
    else:
        f.db.info.clear()
    with pytest.raises((BindingError, ConfigurationPlanConflict)):
        await f.action(f.db, f.f.incoming.envelope(), f.f.incoming.body_message())
    f.action._admission.admit.assert_not_called()
    f.db.commit.assert_not_called()


async def test_change_action_cannot_adopt_retry_plan_after_shared_context_extraction(monkeypatch):
    f = await fixture(monkeypatch)
    with pytest.raises(BindingError):
        FrozenChangeAdmission(f.f.incoming, f.plan, f.base)
