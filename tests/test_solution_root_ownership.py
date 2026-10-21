"""Characterize original Solution response, root commit and replay before extracting apply_in."""

from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solutions import ModelSelection, PrepareSolution
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql import solutions


@pytest.fixture
def original(monkeypatch):
    scope, solution_id = DraftScope(1, 42), uuid4()
    command = PrepareSolution(command_id=uuid4(), reason="原准备意图", expected_revision=3)
    at = datetime(2026, 10, 21, tzinfo=UTC)
    values = asdict(ModelSelection("deepseek-v4-pro", 4000, 30000, ""))
    state = dict(
        solution_id=str(solution_id),
        organization_id=1,
        revision=3,
        prepared=None,
        draft_id=str(uuid4()),
        draft_revision=2,
        reason="原先保存原因",
        generation=values,
        semantic=values,
    )
    current = SimpleNamespace(revision=2)
    response, order = {"original": "response"}, []

    async def execute(statement):
        order.append("update" if statement.is_update else "insert")
        return SimpleNamespace(rowcount=1)

    async def commit():
        order.append("commit")

    root = SimpleNamespace(is_active=True)
    db = SimpleNamespace(
        get_transaction=Mock(return_value=root),
        in_nested_transaction=Mock(return_value=False),
        connection=AsyncMock(),
        execute=AsyncMock(side_effect=execute),
        commit=AsyncMock(side_effect=commit),
        rollback=AsyncMock(),
    )

    @asynccontextmanager
    async def opened():
        yield db

    settings = SimpleNamespace(
        governance_models=("deepseek-v4-pro",),
        models=object(),
        evaluation=SimpleNamespace(candidate_mode_enabled=False),
    )
    store = solutions.MySQLSolutions(SimpleNamespace(open=opened), settings)
    prior = AsyncMock(return_value=None)
    read = AsyncMock(return_value=state)
    draft = AsyncMock(return_value=current)
    checked = AsyncMock(side_effect=lambda *_: order.append("check"))

    async def prepare(*args, **kwargs):
        assert args[0] is db and args[1] is scope and args[2] is state
        assert args[3:5] == (command.command_id, at)
        assert kwargs["execution_mode"] == "serial_v1"
        order.append("prepare")
        return {"run_id": "first-run"}

    async def hydrate(*args):
        assert args[0] is db and args[1] is scope
        order.append("hydrate")
        return response

    prepared, hydrated = AsyncMock(side_effect=prepare), AsyncMock(side_effect=hydrate)
    monkeypatch.setattr(solutions, "prior", prior)
    monkeypatch.setattr(solutions, "read_state", read)
    monkeypatch.setattr(solutions, "read_draft", draft)
    monkeypatch.setattr(solutions, "prepare_assets", prepared)
    monkeypatch.setattr(solutions, "hydrate", hydrated)
    monkeypatch.setattr(store, "_check_selections", checked)
    return SimpleNamespace(**locals())


async def test_original_preparation_response_is_validated_before_single_caller_commit(original):
    p = original
    assert await p.store.apply(p.scope, p.solution_id, p.command, p.at) is p.response
    assert p.order == ["check", "prepare", "update", "insert", "hydrate", "commit"]
    p.db.connection.assert_awaited_once_with(
        execution_options={"isolation_level": "READ COMMITTED"}
    )
    assert p.prior.await_count == 2
    p.read.assert_awaited_once_with(p.db, p.scope, p.solution_id, lock=True)
    p.draft.assert_awaited_once_with(p.db, p.scope, UUID(p.state["draft_id"]), lock=True)
    statements = [v.args[0] for v in p.db.execute.await_args_list]
    assert statements[0].table is solutions.solutions and statements[1].table is solutions.commands
    update, insert = (v.compile().params for v in statements)
    assert update["revision"] == 4 and p.state["updated_at"] == p.at.isoformat()
    assert update["state_json"] == insert["receipt_json"] == solutions.encode(p.state)
    assert insert["command_id"] == str(p.command.command_id)
    p.db.commit.assert_awaited_once()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize("late", [False, True])
async def test_original_receipt_replay_returns_without_business_writes_or_root_commit(
    original, late
):
    p = original
    p.prior.side_effect = [None, p.state] if late else [p.state]
    assert await p.store.apply(p.scope, p.solution_id, p.command, p.at) is p.response
    assert p.order == ["hydrate"]
    assert p.read.await_count == int(late)
    p.draft.assert_not_called()
    p.checked.assert_not_called()
    p.prepared.assert_not_called()
    p.db.execute.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("conflict", ["revision", "prepared", "draft"])
async def test_original_preparation_conflicts_do_not_write_or_commit(original, conflict):
    p = original
    if conflict == "revision":
        p.state["revision"] += 1
    elif conflict == "prepared":
        p.state["prepared"] = {"run_id": "original"}
    else:
        p.current.revision += 1
    with pytest.raises(DraftConflict):
        await p.store.apply(p.scope, p.solution_id, p.command, p.at)
    p.checked.assert_not_called()
    p.prepared.assert_not_called()
    p.db.execute.assert_not_called()
    p.db.commit.assert_not_called()


async def test_original_response_failure_never_commits_staged_preparation(original):
    p = original
    p.hydrated.side_effect = ValueError("Original response audit failed")
    with pytest.raises(ValueError):
        await p.store.apply(p.scope, p.solution_id, p.command, p.at)
    assert p.db.execute.await_count == 2
    p.db.commit.assert_not_called()


async def test_borrowed_solution_apply_does_not_configure_or_commit_the_callers_root(original):
    p = original
    result = await p.store.apply_in(p.db, p.scope, p.solution_id, p.command, p.at)
    assert result.value is p.response and result.wrote
    assert p.order == ["check", "prepare", "update", "insert", "hydrate"]
    p.db.connection.assert_not_called()
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize("late", [False, True])
async def test_borrowed_replay_reports_no_new_write_or_commit(original, late):
    p = original
    p.prior.side_effect = [None, p.state] if late else [p.state]
    result = await p.store.apply_in(p.db, p.scope, p.solution_id, p.command, p.at)
    assert result.value is p.response and not result.wrote
    p.db.connection.assert_not_called()
    p.db.execute.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("invalid", ["missing", "inactive", "savepoint"])
async def test_borrowed_apply_rejects_non_root_before_any_business_read(original, invalid):
    p = original
    if invalid == "missing":
        p.db.get_transaction.return_value = None
    elif invalid == "inactive":
        p.root.is_active = False
    else:
        p.db.in_nested_transaction.return_value = True
    with pytest.raises(RuntimeError):
        await p.store.apply_in(p.db, p.scope, p.solution_id, p.command, p.at)
    p.prior.assert_not_called()
    p.db.execute.assert_not_called()
    p.db.commit.assert_not_called()
