"""Review follows the shared dispatcher without weakening original outcome proof."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Evidence,
    Execution,
    LocalOutcome,
    Reservation,
    Result,
    State,
)

from qs_ai.infrastructure.workflow_transport import evaluation_governance_dispatch as dispatch
from qs_ai.infrastructure.workflow_transport.review_state_plan import read_review_state_plan
from tests.test_review_state_plan import fixture as frozen


def fixture(monkeypatch):
    f = frozen()
    incoming, plan, at = f["incoming"], f["winner"], f["frozen"].at
    _, _, value = read_review_state_plan(incoming, plan)
    owned = Execution(plan, 1, "m3-1", b"x" * 32, at + timedelta(seconds=60))
    store = SimpleNamespace(
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=Reservation(owned, True, State.PREPARED)),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
    )
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=value))
    expected = object()
    root = SimpleNamespace(view=expected, business_error=None)
    root_factory = Mock(return_value=root)
    monkeypatch.setattr(dispatch, "ReviewStateRoot", root_factory)
    send = dispatch.GovernanceStateDispatcher(
        object(), archives, runtime, signing_key=f["signing"], recipient_key=f["recipient"]
    )
    send._review_planner.candidate = AsyncMock(return_value=plan)
    send._planner.candidate = AsyncMock(side_effect=AssertionError("Wrong business planner"))
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("final_unknown", [False, True])
async def test_review_dispatch_uses_only_its_original_root_and_exact_winner(
    monkeypatch, final_unknown
):
    f = fixture(monkeypatch)
    if final_unknown:
        f.runtime.execute.side_effect = TransactionSendError(
            "final_unknown", Result(LocalOutcome.COMMITTED, ())
        )
    assert await f.send(f.incoming, f.at) is f.expected
    assert f.root_factory.call_args.args[3:5] == (f.incoming, f.plan)
    f.runtime.execute.assert_awaited_once_with(f.owned, f.root)
    f.send._review_planner.candidate.assert_awaited_once_with(f.incoming, f.at)
    f.send._planner.candidate.assert_not_called()
    f.store.lookup.assert_awaited_once_with(f.incoming.intent.request)
    f.archives.read_committed.assert_awaited_once_with(f.value.envelope().message_id)


@pytest.mark.parametrize("damage", ["existing", "witness", "archive", "view", "not_started"])
async def test_review_never_manufactures_success_or_repeats_an_unknown_invocation(
    monkeypatch, damage
):
    f = fixture(monkeypatch)
    if damage == "existing":
        f.store.lookup.return_value = Reservation(f.owned, False, State.PREPARED)
    elif damage == "witness":
        f.store.evidence.side_effect = None
        f.store.evidence.return_value = None
    elif damage == "archive":
        f.archives.read_committed.return_value = None
    elif damage == "view":
        f.root.view = None
    else:
        f.runtime.execute.side_effect = TransactionSendError(
            "not_started", Result(LocalOutcome.NOT_STARTED, ())
        )
    with pytest.raises((dispatch.GovernanceTransactionPending, TransactionSendError)):
        await f.send(f.incoming, f.at)
    assert f.runtime.execute.await_count <= 1
    if damage == "existing":
        f.send._review_planner.candidate.assert_not_called()
        f.runtime.reserve.assert_not_called()
        f.root_factory.assert_not_called()
