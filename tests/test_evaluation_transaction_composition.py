"""Complete composition is inert and retains original process budgets."""

from unittest.mock import Mock

import pytest

from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.bootstrap.evaluation_transactions import transaction_evaluation_worker
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor


def build(**changes):
    transactions = Mock()
    transactions.open.side_effect = AssertionError("Composition cannot open a database root")
    runtime = Mock()
    runtime.start.side_effect = AssertionError("Only the application lifecycle starts the SDK")
    capacity = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    cursor = RecoveryCursor()
    options = dict(
        signing_key=object(),
        recipient_key=object(),
        candidate_limit=1,
        capacity=capacity,
        recovery_cursor=cursor,
    )
    options.update(changes)
    worker = transaction_evaluation_worker(
        transactions, Mock(), runtime, Mock(), Mock(), Mock(), "worker:first", **options
    )
    return worker, transactions, runtime, capacity, cursor


def test_complete_evaluation_composition_cannot_start_network_or_borrow_a_root():
    worker, tx, runtime, capacity, cursor = build()
    tx.open.assert_not_called()
    runtime.assert_not_called()
    assert not runtime.mock_calls
    assert worker.transactions is tx and worker.capacity is capacity
    assert worker.candidate_limit == 1 and worker.recovery_cursor is cursor
    # Every accepted Run mode and maintenance responsibility is bound together.
    assert all(
        x is not None
        for x in (
            worker._serial_runtime,
            worker._candidate_runtime,
            worker._preflight_dispatch,
            worker._serial_recovery_dispatch,
            worker._candidate_recovery_dispatch,
            worker._cancel_drain_dispatch,
        )
    )
    assert worker._prepare_dispatch is None


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("nan")])
def test_invalid_original_database_budget_refuses_composition(budget):
    with pytest.raises(ValueError):
        build(database_seconds=budget)


@pytest.mark.parametrize("limit", [0, True, 33])
def test_original_candidate_limit_cannot_be_expanded_by_composition(limit):
    with pytest.raises(ValueError):
        build(candidate_limit=limit)
