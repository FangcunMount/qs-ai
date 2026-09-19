"""Low-cardinality operational readings, without participant or report data."""

from typing import Protocol


class OperationalMetrics(Protocol):
    async def collect(self) -> dict[str, float]: ...
