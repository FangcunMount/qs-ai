"""The original CHANGE root has only frozen authorization, clocks and identities."""

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
from qs_ai.infrastructure.workflow_transport.frozen_start_events import FrozenInterpretationEvents
from tests.test_change_admission_planner import fixture as planning


async def fixture(monkeypatch):
    f = planning(monkeypatch)
    plan = await f.planner.candidate(f.incoming)
    base = WorkflowCommandAdmission(f.read.source, *(SimpleNamespace() for _ in range(5)))
    action = FrozenChangeAdmission(f.incoming, plan, base)
    db = f.read.db
    db.info = {"state_events": FrozenInterpretationEvents(f.incoming, plan)}
    db.scalar = AsyncMock(return_value=f.at.replace(tzinfo=None))
    root = SimpleNamespace(is_active=True)
    db.get_transaction, db.in_transaction = Mock(return_value=root), Mock(return_value=True)
    action._admission.admit = AsyncMock(return_value="original decision")
    return SimpleNamespace(**locals())


async def test_exact_frozen_root_checks_locked_current_target_without_remote_client(monkeypatch):
    f = await fixture(monkeypatch)
    f.f.read.source.authorize.side_effect = AssertionError("external client forbidden in root")
    result = await f.action(f.db, f.f.incoming.envelope(), f.f.incoming.body_message())
    assert result == "original decision"
    assert f.db.execute.await_args.args[0]._for_update_arg is not None
    assert f.f.read.source.authorize.await_count == 1  # only the earlier read plan
    service = f.action._service(Transactions.borrowed(f.db))
    await service.source.authorize(f.f.read.actor, "19", ("42",))
    assert service._new_identity("run") == f.action.facts.run_id
    async with service.uows.open() as uow:
        assert uow.db is f.db
        await uow.commit()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("wrong", ["expired", "target", "input", "recorder"])
async def test_wrong_target_or_stale_proof_cannot_start_original_business(monkeypatch, wrong):
    f = await fixture(monkeypatch)
    envelope, body = f.f.incoming.envelope(), f.f.incoming.body_message()
    if wrong == "expired":
        f.db.scalar.return_value += timedelta(seconds=61)
    elif wrong == "target":
        f.f.read.session.testee_id = "20"
    elif wrong == "input":
        body.change.expected_version += 1
    else:
        f.db.info.clear()
    with pytest.raises((BindingError, ConfigurationPlanConflict)):
        await f.action(f.db, envelope, body)
    f.action._admission.admit.assert_not_called()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("change", ["owner", "missing"])
async def test_current_locked_ownership_or_absence_cannot_use_a_former_read_snapshot(
    monkeypatch, change
):
    from qs_ai.domain.interpretation.model import Actor

    f = await fixture(monkeypatch)
    if change == "owner":
        f.f.read.session.actor = Actor("7", "10")
    else:
        selected = Mock()
        selected.mappings.return_value.one_or_none.return_value = None
        f.db.execute.side_effect = None
        f.db.execute.return_value = selected
    result = await f.action(f.db, f.f.incoming.envelope(), f.f.incoming.body_message())
    assert result.grpc_status_code == (7 if change == "owner" else 5)
    f.action._admission.admit.assert_not_called()
