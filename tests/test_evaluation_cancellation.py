from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.domain.evaluation.cancellation import CancellationDecision
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint

AT = datetime(2026, 9, 13, tzinfo=UTC)


def decision(**changes):
    return replace(CancellationDecision("user:42", "停止后续评测", False, AT), **changes)


@pytest.mark.parametrize("status", ["requested", "collecting", "blocked", "awaiting_review"])
def test_cancellation_distinguishes_discard_from_stopping_execution(status):
    value = decision(discard=status == "awaiting_review")
    assert value.cause(status, 0, None) == (
        "operator_discarded" if status == "awaiting_review" else "operator_canceled"
    )
    with pytest.raises(ValueError):
        replace(value, discard=not value.discard).cause(status, 0, None)


@pytest.mark.parametrize("status", ["approved", "rejected", "canceled", "unknown"])
def test_terminal_or_unknown_status_cannot_be_canceled(status):
    with pytest.raises(ValueError):
        decision().cause(status, 0, None)


@pytest.mark.parametrize("count", [1, -1, False])
def test_unresolved_or_invalid_call_count_requires_reconciliation(count):
    with pytest.raises(ValueError):
        decision().cause("blocked", count, None)


def test_cancellation_never_interrupts_a_dispatched_model_call():
    cp = ExecutionCheckpoint(
        "execution:1",
        "generation",
        "case:1",
        1,
        "",
        1,
        "worker:1",
        "invocation:1",
        "prepared",
        AT,
        AT + timedelta(minutes=1),
    )
    assert decision().cause("collecting", 0, cp) == "operator_canceled"
    with pytest.raises(ValueError):
        decision().cause("collecting", 0, cp.mark_dispatching("worker:1", AT))
    with pytest.raises(ValueError):
        decision(canceled_at=AT - timedelta(seconds=1)).cause("collecting", 0, cp)


@pytest.mark.parametrize(
    "change",
    [
        {"actor": "system:untrusted"},
        {"reason": " "},
        {"reason": "中" * 334},
        {"reason": "<unsafe>"},
        {"discard": 0},
        {"canceled_at": AT.replace(tzinfo=None)},
    ],
)
def test_cancellation_audit_requires_valid_operator_reason_and_time(change):
    with pytest.raises(ValueError):
        decision(**change)
