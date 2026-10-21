"""Original worker admission: eligibility, fences, terminal history and owned commits."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.domain.interpretation.model import Actor, Session, Status
from qs_ai.infrastructure.persistence.mysql import execution as module


@pytest.fixture
def original(monkeypatch):
    events, candidates, source_sessions, source_jobs, guards, states = [], [], [], [], [], []
    missing = set()
    current = 0

    def add(*, snapshot=False):
        value = Session(
            str(uuid4()),
            Actor("1", "actor:first"),
            "7",
            ("42",),
            "original goal",
            workflow_version="qs-snapshot-v1" if snapshot else "interpretation-v1",
        )
        value.queue(str(uuid4()))
        source_sessions.append(value)
        candidate = SimpleNamespace(id=str(uuid4()), session_id=value.id)
        candidates.append(candidate)
        source_jobs.append(
            dict(
                id=candidate.id,
                session_id=value.id,
                run_id=value.active_run_id,
                attempt=0,
                answer=None,
                skipped=False,
                question_id=None,
            )
        )
        guards.append(dict(fence=4 + len(guards), expired=True))
        return candidate

    add()
    root = SimpleNamespace(is_active=True)
    db = AsyncMock(spec=AsyncSession)
    db.get_transaction = Mock(return_value=root)
    db.in_nested_transaction = Mock(return_value=False)
    binding = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(module, "bind", lambda _: binding)

    def result(value):
        row = Mock()
        row.mappings.return_value.first.return_value = value
        row.mappings.return_value.one.return_value = value
        return row

    async def execute(statement):
        nonlocal current
        if statement.is_select:
            table = statement.get_final_froms()[0]
            if table is module.jobs and len(statement.selected_columns) == 2:
                assert statement._limit_clause.value == 20
                events.append("candidates")
                return SimpleNamespace(all=lambda: list(candidates))
            values = statement.compile().params.values()
            if table is module.sessions:
                current = next(i for i, c in enumerate(candidates) if c.session_id in values)
                events.append("session")
                return result(None if "session" in missing else dict(index=current))
            if table is module.jobs:
                events.append("job")
                return result(None if "job" in missing else source_jobs[current])
            assert table is module.execution_leases
            events.append("lease")
            return result(guards[current])
        events.append(statement.table.name)
        return SimpleNamespace(rowcount=1)

    async def commit():
        events.append("commit")

    db.execute.side_effect, db.commit.side_effect = execute, commit

    @asynccontextmanager
    async def opened():
        events.append("open")
        try:
            yield db
        finally:
            events.append("close")

    tx = SimpleNamespace(open=opened)
    store = module.MySQLExecutionStore(tx)
    monkeypatch.setattr(module, "session_from", lambda row: replace(source_sessions[row["index"]]))

    async def save(_uow, value):
        states.append(replace(value))
        events.append("state")

    monkeypatch.setattr(module.MySQLUnitOfWork, "save", save)
    record = AsyncMock()
    acquire, release = AsyncMock(return_value=True), AsyncMock()
    monkeypatch.setattr(module, "record", record)
    monkeypatch.setattr(module, "acquire_capacity", acquire)
    monkeypatch.setattr(module, "release_capacity", release)
    return SimpleNamespace(**locals())


async def test_original_claim_keeps_source_identity_fence_attempt_and_single_owned_commit(original):
    p = original
    claim = await p.store.claim(30)
    assert claim.job_id == p.source_jobs[0]["id"] and claim.run_id == p.source_jobs[0]["run_id"]
    assert (
        claim.fence == 5 and claim.session.version == 3 and claim.session.status == Status.RUNNING
    )
    assert p.events[:4] == ["open", "candidates", "session", "job"]
    assert p.events[-2:] == ["commit", "close"]
    assert len(p.states) == 1 and p.record.await_args.kwargs["attempt"] == 1
    assert p.record.await_args.args[4] == f"claim:{claim.job_id}:5"
    p.db.commit.assert_awaited_once()


async def test_original_empty_candidates_do_not_commit_or_emit_state(original):
    p = original
    p.candidates.clear()
    assert await p.store.claim(30) is None
    assert p.events == ["open", "candidates", "close"] and not p.states
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("why", ["session", "job", "run", "status"])
async def test_original_missing_or_stale_candidate_does_not_gain_execution(original, why):
    p = original
    if why in ("session", "job"):
        p.missing.add(why)
    elif why == "run":
        p.source_jobs[0]["run_id"] = str(uuid4())
    else:
        p.source_sessions[0].status = Status.BLOCKED
    assert await p.store.claim(30) is None and not p.states
    p.record.assert_not_called()
    p.db.commit.assert_not_called()


async def test_original_live_lease_cannot_be_reclaimed(original):
    p = original
    p.guards[0]["expired"] = False
    assert await p.store.claim(30) is None and not p.states
    p.record.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("snapshot", [False, True])
async def test_original_exhausted_attempts_write_only_final_two_transition_history(
    original, snapshot
):
    p = original
    if snapshot:
        p.source_sessions[0].workflow_version = "qs-snapshot-v1"
    p.source_jobs[0]["attempt"] = 3
    assert await p.store.claim(30) is None
    assert len(p.states) == 1 and p.states[0].version == 4
    assert p.states[0].status == Status.BLOCKED and p.states[0].failure_code == "attempts_exhausted"
    assert p.release.await_count == int(snapshot)
    p.acquire.assert_not_called()
    p.record.assert_not_called()
    p.db.commit.assert_awaited_once()


async def test_original_capacity_refusal_defers_without_consuming_attempt_or_state(original):
    p = original
    p.source_sessions[0].workflow_version = "qs-snapshot-v1"
    p.acquire.return_value = False
    assert await p.store.claim(30) is None and not p.states
    p.record.assert_not_called()
    p.db.commit.assert_awaited_once()


async def test_original_deferral_continues_next_candidate_in_same_host_session(original):
    p = original
    p.source_sessions[0].workflow_version = "qs-snapshot-v1"
    second = p.add(snapshot=True)
    p.acquire.side_effect = [False, True]
    claim = await p.store.claim(30)
    assert claim.job_id == second.id and claim.fence == 6 and len(p.states) == 1
    assert p.events.count("open") == p.events.count("close") == 1
    assert p.events.count("commit") == 2
    assert p.states[0].id == second.session_id


@pytest.mark.parametrize("failure", ["record", "state"])
async def test_original_admission_error_never_commits_or_returns_claim(
    original, monkeypatch, failure
):
    p = original
    if failure == "record":
        p.record.side_effect = ConnectionError("original milestone refused")
    else:
        monkeypatch.setattr(
            module.MySQLUnitOfWork,
            "save",
            AsyncMock(side_effect=ConnectionError("original state refused")),
        )
    with pytest.raises(ConnectionError):
        await p.store.claim(30)
    p.db.commit.assert_not_called()
    assert p.events[-1] == "close"


async def test_original_commit_error_does_not_return_execution_to_worker(original):
    p = original
    p.db.commit.side_effect = ConnectionError("original commit reply unavailable")
    with pytest.raises(ConnectionError):
        await p.store.claim(30)
    assert p.events[-1] == "close"


async def test_original_short_ttl_refused_before_opening_host_database(original):
    p = original
    with pytest.raises(ValueError):
        await p.store.claim(2)
    assert p.events == []


@pytest.mark.parametrize("kind", ["claimed", "deferred", "exhausted"])
async def test_borrowed_claim_stages_original_branch_without_commit_or_new_session(
    original, kind, monkeypatch
):
    from datetime import UTC, datetime

    p = original
    p.source_sessions[0].workflow_version = "qs-snapshot-v1"
    if kind == "deferred":
        p.acquire.return_value = False
    elif kind == "exhausted":
        p.source_jobs[0]["attempt"] = 3
    first_id, at = str(uuid4()), datetime(2026, 10, 22, tzinfo=UTC)
    saved = []

    async def save(uow, value):
        assert uow._state_event_id == first_id
        saved.append(value.version)

    monkeypatch.setattr(module.MySQLUnitOfWork, "save", save)
    staged = await p.store.claim_in(
        p.db, p.candidates[0].id, p.candidates[0].session_id, 30, state_event_id=first_id, at=at
    )
    assert staged.status == kind
    assert p.binding.validate.await_count == 2 and p.db.get_transaction() is p.root
    assert (staged.claim is not None) == (kind == "claimed")
    assert "original goal" not in repr(staged)
    if kind == "claimed":
        assert saved == [3] and p.acquire.await_args.args[3] == at
    elif kind == "exhausted":
        assert saved == [4] and p.release.await_args.args[2] == at
    else:
        assert not saved
    for method in (p.db.commit, p.db.rollback, p.db.begin):
        method.assert_not_called()
    assert "open" not in p.events and "close" not in p.events


@pytest.mark.parametrize("fault", ["handle", "nested", "ended", "inactive"])
async def test_borrowed_claim_refuses_wrong_root_before_sql(original, fault):
    from reliable_messaging.transaction import BindingError

    p = original
    if fault == "nested":
        p.db.in_nested_transaction.return_value = True
    elif fault == "ended":
        p.db.get_transaction.return_value = None
    elif fault == "inactive":
        p.root.is_active = False
    with pytest.raises(BindingError):
        await p.store.claim_in(
            object() if fault == "handle" else p.db,
            p.candidates[0].id,
            p.candidates[0].session_id,
            30,
        )
    p.db.execute.assert_not_called()


async def test_borrowed_claim_detects_original_root_replacement_before_handoff(
    original, monkeypatch
):
    from reliable_messaging.transaction import BindingError

    p = original

    async def replace_root(uow, value):
        p.db.get_transaction.return_value = SimpleNamespace(is_active=True)

    monkeypatch.setattr(module.MySQLUnitOfWork, "save", replace_root)
    with pytest.raises(BindingError):
        await p.store.claim_in(p.db, p.candidates[0].id, p.candidates[0].session_id, 30)
    p.db.commit.assert_not_called()


async def test_owned_scanner_starts_next_root_after_committed_deferral_on_same_session(original):
    p = original
    p.source_sessions[0].workflow_version = "qs-snapshot-v1"
    second = p.add(snapshot=True)
    p.acquire.side_effect = [False, True]

    async def commit():
        p.events.append("commit")
        p.db.get_transaction.return_value = None

    async def begin():
        p.db.get_transaction.return_value = SimpleNamespace(is_active=True)

    p.db.commit.side_effect = commit
    p.db.begin = AsyncMock(side_effect=begin)
    claim = await p.store.claim(30)
    assert claim.job_id == second.id and p.events.count("open") == 1
    assert p.db.begin.await_count == 1 and p.db.commit.await_count == 2
