"""First receipt scope, exact SDK evidence and original prepared response stay distinct."""

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Binding,
    BindingError,
    Evidence,
    Execution,
    LocalOutcome,
    Reservation,
    Result,
    State,
)

from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.workflow_transport import solution_preparation_dispatch as dispatch
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import read_preparation_plan
from tests import test_solution_preparation_state
from tests.route_fixtures import legacy_routes
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_native_prompts import draft as draft
from tests.test_solution_preparation_plan import original as original

planning = test_solution_preparation_state.planning


@pytest.fixture
async def sent(planning, monkeypatch):
    f = planning
    candidate = await f.planner.candidate(f.incoming, f.p.draft.saved_at)
    _, facts, first = read_preparation_plan(
        f.incoming, candidate.winner, route_resolver=legacy_routes()
    )
    owned = Execution(
        candidate.winner, 1, "m3-1", b"x" * 32, f.p.draft.saved_at + timedelta(seconds=60)
    )
    store = SimpleNamespace(
        binding=Binding("ai.mysql", "original", "m3-1"),
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=Reservation(owned, True, State.PREPARED)),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        routing=legacy_routes(),
    )
    value = json.loads(candidate.target_json)
    root = SimpleNamespace(value=value, business_error=None)
    root_factory = Mock(return_value=root)
    monkeypatch.setattr(dispatch, "SolutionPreparationRoot", root_factory)
    classify = AsyncMock(return_value=False)
    monkeypatch.setattr(dispatch, "input_conflicts", classify)
    archive = SimpleNamespace(read_committed=AsyncMock(return_value=first))
    send = dispatch.SolutionPreparationDispatcher(
        f.store, archive, runtime, signing_key=f.signing, recipient_key=f.recipient
    )
    replay = AsyncMock(return_value=None)
    planned = AsyncMock(return_value=candidate)
    send._planner.replay = replay
    send._planner.candidate = planned
    return SimpleNamespace(**locals())


async def test_new_preparation_uses_one_root_and_its_own_response_after_exact_proof(sent):
    p = sent
    assert await p.send(p.f.incoming, p.f.p.draft.saved_at) is p.value
    p.runtime.execute.assert_awaited_once_with(p.owned, p.root)
    assert p.root_factory.call_args.args[3:6] == (p.f.incoming, p.owned.plan, p.candidate)
    p.replay.assert_awaited_once_with(p.f.incoming)


async def test_final_unknown_keeps_local_commit_without_reexecuting_preparation(sent):
    p = sent
    p.runtime.execute.side_effect = TransactionSendError(
        "final_unknown", Result(LocalOutcome.COMMITTED, ())
    )
    assert await p.send(p.f.incoming, p.f.p.draft.saved_at) is p.value
    assert p.runtime.reserve.await_count == p.runtime.execute.await_count == 1


@pytest.mark.parametrize("protocol", [False, True])
async def test_first_receipt_replay_does_not_rebuild_assets_or_adopt_legacy_witness(sent, protocol):
    p = sent
    p.replay.return_value = p.value
    if protocol:
        p.store.lookup.return_value = Reservation(p.owned, False, State.COMMITTED)
    assert await p.send(p.f.incoming, p.f.p.draft.saved_at) is p.value
    p.planned.assert_not_called()
    p.runtime.reserve.assert_not_called()
    p.runtime.execute.assert_not_called()
    p.root_factory.assert_not_called()
    assert p.store.lookup.await_count == (1 if protocol else 2)
    if protocol:
        assert p.replay.await_args.kwargs == {"expected_target": p.facts.target_digest}
    else:
        p.archive.read_committed.assert_not_called()


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED])
async def test_prepared_or_aborted_execution_cannot_adopt_matching_old_receipt(sent, state):
    p = sent
    p.store.lookup.return_value = Reservation(p.owned, False, state)
    p.replay.return_value = p.value
    with pytest.raises(dispatch.PreparationTransactionPending):
        await p.send(p.f.incoming, p.f.p.draft.saved_at)
    p.planned.assert_not_called()
    p.runtime.reserve.assert_not_called()
    p.runtime.execute.assert_not_called()


@pytest.mark.parametrize("missing", ["witness", "archive", "receipt", "target"])
async def test_committed_record_requires_first_archive_exact_evidence_and_target_receipt(
    sent, missing
):
    p = sent
    p.store.lookup.return_value = Reservation(p.owned, False, State.COMMITTED)
    p.replay.return_value = p.value
    if missing == "witness":
        p.store.evidence.side_effect = lambda ref: Evidence(ref, State.PREPARED, State.PREPARED)
    elif missing == "archive":
        p.archive.read_committed.return_value = None
    elif missing == "receipt":
        p.replay.side_effect = [p.value, None]
    else:
        p.replay.side_effect = [p.value, MessageConflict("contradictory first receipt")]
    with pytest.raises(dispatch.PreparationTransactionPending):
        await p.send(p.f.incoming, p.f.p.draft.saved_at)
    p.runtime.reserve.assert_not_called()
    p.runtime.execute.assert_not_called()


async def test_wrong_original_receipt_actor_is_rejected_before_protocol_lookup(sent):
    p = sent
    p.replay.side_effect = NotFound("Original command unavailable")
    with pytest.raises(NotFound):
        await p.send(p.f.incoming, p.f.p.draft.saved_at)
    p.store.lookup.assert_not_called()
    p.runtime.reserve.assert_not_called()


@pytest.mark.parametrize(
    "local", [LocalOutcome.NOT_STARTED, LocalOutcome.UNKNOWN, LocalOutcome.ABORTED]
)
async def test_captured_response_is_not_returned_for_noncommitted_result(sent, local):
    p = sent
    p.runtime.execute.return_value = Result(local, ())
    with pytest.raises(dispatch.PreparationTransactionPending):
        await p.send(p.f.incoming, p.f.p.draft.saved_at)
    assert p.root.value is p.value and p.runtime.execute.await_count == 1
    p.archive.read_committed.assert_not_called()


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED, State.COMMITTED])
async def test_reservation_loser_only_reads_original_responsibility(sent, state):
    p = sent
    p.runtime.reserve.return_value = Reservation(p.owned, False, state)
    p.replay.side_effect = [None, p.value, p.value]
    if state == State.COMMITTED:
        assert await p.send(p.f.incoming, p.f.p.draft.saved_at) is p.value
    else:
        with pytest.raises(dispatch.PreparationTransactionPending):
            await p.send(p.f.incoming, p.f.p.draft.saved_at)
    p.runtime.execute.assert_not_called()
    p.root_factory.assert_not_called()


@pytest.mark.parametrize("where", ["lookup", "reserve"])
@pytest.mark.parametrize("proven", [False, True])
async def test_input_mismatch_needs_authoritative_binding_and_raw_identity(sent, where, proven):
    p = sent
    getattr(p.store if where == "lookup" else p.runtime, where).side_effect = ValueError(
        "unstructured SDK refusal"
    )
    p.classify.return_value = proven
    with pytest.raises(DraftConflict if proven else dispatch.PreparationTransactionPending):
        await p.send(p.f.incoming, p.f.p.draft.saved_at)
    p.classify.assert_awaited_once_with(
        p.f.store.transactions, p.store.binding, p.f.incoming.intent.request
    )
    p.runtime.execute.assert_not_called()


async def test_binding_error_never_becomes_business_conflict_or_new_execution(sent):
    p = sent
    p.store.lookup.side_effect = BindingError("wrong binding")
    with pytest.raises(BindingError):
        await p.send(p.f.incoming, p.f.p.draft.saved_at)
    p.classify.assert_not_called()
    p.runtime.execute.assert_not_called()
