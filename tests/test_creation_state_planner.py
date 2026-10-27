"""Read-only creation closes its snapshot before Seal and uses only the guarded root."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.infrastructure.workflow_transport import creation_state as creation
from qs_ai.infrastructure.workflow_transport.creation_state_plan import read_creation_state_plan
from tests.route_fixtures import legacy_routes
from tests.test_creation_state_plan import frozen as frozen
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_run_creation_planning import context as context


@pytest.fixture
async def planned(frozen, monkeypatch):
    f = frozen
    active, order = False, []
    db = SimpleNamespace(connection=AsyncMock(), commit=AsyncMock())

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield db
        finally:
            active = False

    async def read(*args):
        assert active and args == (
            db,
            f["incoming"].scope.run_id,
            f["incoming"].release,
            1,
            "user:42",
            f["incoming"].reason,
            f["f"].at,
        )
        order.append("plan")
        return f["plan"]

    creator = SimpleNamespace(
        transactions=SimpleNamespace(open=opened), plan_in=AsyncMock(side_effect=read)
    )
    seal = creation.prepare

    def after_read(*args, **kwargs):
        assert not active
        order.append("seal")
        return seal(*args, **kwargs)

    monkeypatch.setattr(creation, "prepare", Mock(side_effect=after_read))
    planner = creation.CreationStatePlanner(
        creator,
        signing_key=f["signing"],
        recipient_key=f["recipient"],
        route_resolver=legacy_routes(),
    )
    return SimpleNamespace(**locals())


async def test_creation_plan_is_read_only_and_first_seal_occurs_after_read_close(planned):
    p = planned
    winner, business = await p.planner.candidate(p.f["incoming"], p.f["f"].at)
    assert business is p.f["plan"]
    _, facts, _ = read_creation_state_plan(p.f["incoming"], winner, route_resolver=legacy_routes())
    facts.validate(p.f["incoming"], business)
    assert p.order == ["plan", "seal"]
    p.db.commit.assert_not_called()
    p.db.connection.assert_awaited_once_with(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )


@pytest.mark.parametrize("invalid", ["clock", "source"])
async def test_invalid_planning_never_seals_or_writes(planned, invalid):
    p = planned
    if invalid == "source":
        p.creator.plan_in.side_effect = ValueError("Original assets unavailable")
    at = p.f["f"].at
    with pytest.raises(ValueError):
        await p.planner.candidate(
            p.f["incoming"], at.replace(tzinfo=None) if invalid == "clock" else at
        )
    assert "seal" not in p.order
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("changed", [False, True])
async def test_creation_root_revalidates_exact_raw_source_on_same_guarded_root(
    frozen, monkeypatch, changed
):
    f = frozen
    root = creation.CreationStateRoot(
        object(),
        object(),
        object(),
        f["incoming"],
        f["winner"],
        f["plan"],
        route_resolver=legacy_routes(),
    )
    db, view = object(), object()
    current = (
        replace(f["plan"], definition_json=f["plan"].definition_json + " ")
        if changed
        else f["plan"]
    )
    read = AsyncMock(return_value=current)
    apply = AsyncMock()
    visible = AsyncMock(return_value=view)
    monkeypatch.setattr(creation, "plan_run_creation", read)
    monkeypatch.setattr(creation, "apply_run_creation", apply)
    monkeypatch.setattr(creation, "read_view", visible)
    assert root._isolation_level == "REPEATABLE READ"
    if changed:
        with pytest.raises(ValueError):
            await root._business(db, f["facts"])
        assert isinstance(root.business_error, ValueError) and root.view is None
        apply.assert_not_called()
        visible.assert_not_called()
    else:
        await root._business(db, f["facts"])
        apply.assert_awaited_once_with(db, f["plan"])
        visible.assert_awaited_once_with(db, f["incoming"].scope)
        assert root.view is view and root.business_error is None
    assert read.await_args.args[0] is db
    assert read.await_args.args[-1].isoformat() == f["f"].at.isoformat()
    assert read.await_args.kwargs["execution_mode"] == "candidate_v2"
