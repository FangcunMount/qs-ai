"""Low-cardinality operational readings, without participant or report data."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


class OperationalMetrics(Protocol):
    async def collect(self) -> dict[str, float]: ...


@dataclass(frozen=True)
class TransactionMetricsSnapshot:
    values: Mapping[str, int | float]
    outcomes: Mapping[tuple[str, str], int]
    checks: Mapping[str, int]
    recovery_confirmed: Mapping[str, int]
    recovery_audits: Mapping[str, int]


class TransactionMetrics(Protocol):
    def transaction_metrics_snapshot(self) -> TransactionMetricsSnapshot | None: ...
