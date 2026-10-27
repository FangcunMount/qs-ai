"""Generic root consumes an owned plan once; only precise host storage can prove commit."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.transaction import BindingError, LocalOutcome

from qs_ai.infrastructure.workflow_transport import interpretation_state_root as module
from tests.route_fixtures import legacy_routes
from tests.test_interpretation_state_plan import frozen as frozen


async def test_root_constructor_is_inert_and_owned_executor_cannot_be_called_twice(
    frozen, monkeypatch
):
    tx = SimpleNamespace(open=AsyncMock(side_effect=AssertionError("Constructor opens no session")))
    root = module.InterpretationStateRoot(
        tx,
        object(),
        object(),
        frozen.intent,
        frozen.plan,
        AsyncMock(),
        route_resolver=legacy_routes(),
    )
    tx.open.assert_not_called()
    commit = AsyncMock(return_value=LocalOutcome.UNKNOWN)
    monkeypatch.setattr(module, "commit_original", commit)
    owned = SimpleNamespace(owned=True, plan=frozen.plan)
    assert await root(owned) == LocalOutcome.UNKNOWN
    with pytest.raises(BindingError):
        await root(owned)
    assert commit.await_count == 1


@pytest.mark.parametrize("owned", [False, True])
async def test_wrong_execution_never_enters_original_business_or_primary(frozen, owned):
    tx = SimpleNamespace(
        open=AsyncMock(side_effect=AssertionError("Wrong execution cannot open primary"))
    )
    business = AsyncMock()
    root = module.InterpretationStateRoot(
        tx, object(), object(), frozen.intent, frozen.plan, business, route_resolver=legacy_routes()
    )
    execution = SimpleNamespace(
        owned=owned, plan=SimpleNamespace(digest=b"not the original digest")
    )
    with pytest.raises(BindingError):
        await root(execution)
    tx.open.assert_not_called()
    business.assert_not_called()
