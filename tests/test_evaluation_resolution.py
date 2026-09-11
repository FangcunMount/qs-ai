from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.domain.evaluation.resolution import (
    ResultUnknownResolution,
    UnknownExecution,
    resolve_unknown,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy

AT = datetime(2026, 9, 12, tzinfo=UTC)


def decision(**changes):
    values = dict(
        execution_id="execution:1",
        decision="authorize_replacement",
        actor="user:1",
        reason="核对后批准",
        acknowledged_duplicate_call_and_cost_risk=True,
        resolved_at=AT,
    )
    values.update(changes)
    return ResultUnknownResolution(**values)


def test_last_resolution_allows_collecting_but_preserves_unknown_evidence():
    original = (UnknownExecution("execution:1", "generation", 1, AT),)
    value = decision()
    result = resolve_unknown("blocked", original, (), value, load_execution_policy())
    assert (result.status, result.unresolved_count) == ("collecting", 0)
    assert result.resolutions == (value,)
    assert original == (UnknownExecution("execution:1", "generation", 1, AT),)


def test_multiple_unknowns_remain_blocked_until_all_resolved():
    original = tuple(UnknownExecution(f"execution:{n}", "generation", 1, AT) for n in (1, 2))
    result = resolve_unknown("blocked", original, (), decision(), load_execution_policy())
    assert (result.status, result.unresolved_count) == ("blocked", 1)
    result = resolve_unknown(
        "blocked",
        original,
        result.resolutions,
        decision(execution_id="execution:2"),
        load_execution_policy(),
    )
    assert result.status == "collecting"


@pytest.mark.parametrize("kind", ["generation", "semantic"])
def test_authorization_cannot_extend_frozen_budget_but_cancel_is_allowed(kind):
    original = (UnknownExecution("execution:1", kind, 2, AT),)
    with pytest.raises(ValueError, match="budget"):
        resolve_unknown("blocked", original, (), decision(), load_execution_policy())
    result = resolve_unknown(
        "blocked", original, (), decision(decision="cancel_run"), load_execution_policy()
    )
    assert result.status == "canceled"


@pytest.mark.parametrize(
    "changes",
    [
        {"acknowledged_duplicate_call_and_cost_risk": False},
        {"acknowledged_duplicate_call_and_cost_risk": 1},
        {"actor": ""},
        {"reason": " "},
        {"reason": "中" * 334},
        {"decision": "retry"},
        {"resolved_at": AT.replace(tzinfo=None)},
    ],
)
def test_resolution_requires_explicit_audit(changes):
    with pytest.raises(ValueError):
        decision(**changes)


@pytest.mark.parametrize("case", ["duplicate", "wrong_target", "early", "not_blocked"])
def test_invalid_resolution_is_rejected(case):
    original = (UnknownExecution("execution:1", "generation", 1, AT),)
    value, prior, status = decision(), (), "blocked"
    if case == "duplicate":
        prior = (value,)
    elif case == "wrong_target":
        value = replace(value, execution_id="execution:missing")
    elif case == "early":
        value = replace(value, resolved_at=AT - timedelta(seconds=1))
    else:
        status = "collecting"
    with pytest.raises(ValueError):
        resolve_unknown(status, original, prior, value, load_execution_policy())
