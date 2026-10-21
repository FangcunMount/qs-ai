"""Creation replay never substitutes row existence for an exact SDK witness."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
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

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.workflow_transport import transactional_evaluation_requests as dispatch
from tests.test_creation_state_plan import frozen as frozen
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_run_creation_planning import context as context


@pytest.fixture
async def sent(frozen, monkeypatch):
    f = frozen
    owned = Execution(f["winner"], 1, "m3-1", b"x" * 32, f["facts"].at + timedelta(seconds=60))
    store = SimpleNamespace(
        binding=Binding("ai.mysql", "original", "m3-1"),
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=Reservation(owned, True, State.PREPARED)),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
    )
    view = object()
    local = SimpleNamespace(view=view, business_error=None)
    root = Mock(return_value=local)
    monkeypatch.setattr(dispatch, "CreationStateRoot", root)
    classify = AsyncMock(return_value=False)
    monkeypatch.setattr(dispatch, "input_conflicts", classify)
    creator = SimpleNamespace(transactions=object())
    archives = SimpleNamespace(
        read_committed=AsyncMock(
            return_value=(dispatch.read_creation_state_plan(f["incoming"], f["winner"]))[2]
        )
    )
    sender = dispatch.TransactionalEvaluationRequests(
        creator,
        archives,
        runtime,
        signing_key=f["signing"],
        recipient_key=f["recipient"],
    )
    sender._planner.candidate = AsyncMock(return_value=(f["winner"], f["plan"]))
    sender._receipt = AsyncMock(return_value=None)

    async def create(**kwargs):
        return await sender.create(
            f["incoming"].scope,
            f["incoming"].release,
            f["incoming"].reason,
            f["f"].at,
            confirm=kwargs.get("confirm", True),
        )

    return SimpleNamespace(**locals())


async def test_new_create_returns_its_original_root_response_after_exact_proof(sent):
    p = sent
    assert await p.create() is p.view
    p.runtime.execute.assert_awaited_once_with(p.owned, p.local)
    assert p.root.call_args.args[3:6] == (p.f["incoming"], p.f["winner"], p.f["plan"])
    p.sender._planner.candidate.assert_awaited_once_with(p.f["incoming"], p.f["f"].at)


async def test_final_unknown_keeps_committed_creation_without_new_execution(sent):
    p = sent
    p.runtime.execute.side_effect = TransactionSendError(
        "final_reply_unknown", Result(LocalOutcome.COMMITTED, ())
    )
    assert await p.create() is p.view
    assert p.runtime.execute.await_count == 1 and p.runtime.reserve.await_count == 1


@pytest.mark.parametrize("protocol", [False, True])
async def test_replay_after_progress_returns_original_current_view_without_new_wire(sent, protocol):
    p = sent
    current = object()
    p.sender._receipt.return_value = current
    if protocol:
        p.store.lookup.return_value = Reservation(p.owned, False, State.COMMITTED)
    assert await p.create() is current
    p.sender._planner.candidate.assert_not_called()
    p.runtime.reserve.assert_not_called()
    p.runtime.execute.assert_not_called()
    p.root.assert_not_called()
    assert p.archives.read_committed.await_count == int(protocol)
    assert p.store.lookup.await_count == (1 if protocol else 2)


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED])
async def test_existing_unresolved_or_aborted_execution_cannot_adopt_business_row(sent, state):
    p = sent
    p.store.lookup.return_value = Reservation(p.owned, False, state)
    p.sender._receipt.return_value = object()
    with pytest.raises(dispatch.CreationTransactionPending):
        await p.create()
    p.sender._receipt.assert_not_called()
    p.sender._planner.candidate.assert_not_called()
    p.runtime.reserve.assert_not_called()
    p.runtime.execute.assert_not_called()


@pytest.mark.parametrize("missing", ["witness", "archive", "receipt"])
async def test_committed_replay_requires_exact_witness_archive_and_original_receipt(sent, missing):
    p = sent
    p.store.lookup.return_value = Reservation(p.owned, False, State.COMMITTED)
    p.sender._receipt.return_value = object()
    if missing == "witness":
        p.store.evidence.side_effect = lambda ref: Evidence(ref, State.PREPARED, State.PREPARED)
    elif missing == "archive":
        p.archives.read_committed.return_value = None
    else:
        p.sender._receipt.return_value = None
    with pytest.raises(dispatch.CreationTransactionPending):
        await p.create()
    p.runtime.execute.assert_not_called()
    p.runtime.reserve.assert_not_called()


async def test_legacy_read_lookup_race_refuses_prepared_sdk_responsibility(sent):
    p = sent
    p.store.lookup.side_effect = [None, Reservation(p.owned, False, State.PREPARED)]
    p.sender._receipt.return_value = object()
    with pytest.raises(dispatch.CreationTransactionPending):
        await p.create()
    assert p.store.lookup.await_count == 2
    p.runtime.reserve.assert_not_called()


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED, State.COMMITTED])
async def test_reservation_loser_only_reads_original_terminal_proof_and_never_executes(sent, state):
    p = sent
    p.runtime.reserve.return_value = Reservation(p.owned, False, state)
    current = object()
    p.sender._receipt.side_effect = [None, current]
    if state == State.COMMITTED:
        assert await p.create() is current
    else:
        with pytest.raises(dispatch.CreationTransactionPending):
            await p.create()
    p.runtime.execute.assert_not_called()
    p.root.assert_not_called()


@pytest.mark.parametrize(
    "state", [LocalOutcome.UNKNOWN, LocalOutcome.NOT_STARTED, LocalOutcome.ABORTED]
)
async def test_noncommitted_result_never_returns_success_or_retries_business(sent, state):
    p = sent
    p.runtime.execute.return_value = Result(state, ())
    with pytest.raises(dispatch.CreationTransactionPending):
        await p.create()
    assert p.runtime.execute.await_count == 1
    p.archives.read_committed.assert_not_called()


@pytest.mark.parametrize("where", ["lookup", "reserve"])
@pytest.mark.parametrize("proven", [False, True])
async def test_input_conflict_needs_authoritative_identity_not_exception_text(sent, where, proven):
    p = sent
    error = ValueError("unstructured SDK refusal")
    getattr(p.store if where == "lookup" else p.runtime, where).side_effect = error
    p.classify.return_value = proven
    with pytest.raises(CheckpointConflict if proven else ValueError):
        await p.create()
    p.classify.assert_awaited_once_with(
        p.creator.transactions, p.store.binding, p.f["incoming"].intent.request
    )
    p.runtime.execute.assert_not_called()


async def test_binding_error_and_missing_confirmation_never_enter_planning(sent):
    p = sent
    with pytest.raises(ValueError):
        await p.create(confirm=False)
    p.store.lookup.assert_not_called()
    p.store.lookup.side_effect = BindingError("wrong original binding")
    with pytest.raises(BindingError):
        await p.create()
    p.classify.assert_not_called()
    p.sender._planner.candidate.assert_not_called()
