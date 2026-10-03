"""Borrowed read-only payload access, independent of storage and RPC assembly."""

from typing import Any, Protocol


class PayloadAccess(Protocol):
    """The RPC adapter passes its original reference without re-encoding it.

    Storage validates the exact persisted identity, target, length and hash.
    Failure observations preserve the original error and carry no retry authority.
    Implementations borrow host resources; this port has no pool lifecycle.
    """

    def valid_reference(self, reference: Any) -> bool: ...

    async def payload(self, reference: Any, authenticated_workload: str) -> bytes: ...

    async def record_failure(self, kind: str) -> bool: ...
