"""Explicit management adapter for migrated governance operations.

Remaining writers retain their original implementation. Production assembly
must stay disabled until all writers and retained responsibilities are ready.
"""

from collections.abc import Awaitable, Callable
from datetime import datetime
from uuid import UUID, uuid4

from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.workflow_transport.evaluation_governance_state import (
    GovernanceStateOperation,
)

GovernanceDispatch = Callable[[GovernanceStateOperation, datetime], Awaitable[EvaluationView]]


class TransactionalEvaluationManagement(MySQLEvaluationManagement):
    def __init__(
        self,
        original: MySQLEvaluationManagement,
        dispatch: GovernanceDispatch,
        *,
        new_id: Callable[[], UUID] = uuid4,
    ) -> None:
        super().__init__(
            original.transactions,
            original.capacity,
            original.models,
            original.quota_baseline,
            original.runtime_limits,
        )
        self._dispatch, self._new_id = dispatch, new_id

    async def finalize(
        self,
        scope: ManagementScope,
        expected_version: int,
        expected_passed: bool,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        operation = GovernanceStateOperation(
            "finalize",
            scope,
            self._new_id(),
            expected_version,
            reason,
            confirm,
            expected_passed,
        )
        return await self._dispatch(operation, at)

    async def reopen(
        self,
        scope: ManagementScope,
        expected_version: int,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        operation = GovernanceStateOperation(
            "reopen",
            scope,
            self._new_id(),
            expected_version,
            reason,
            confirm,
        )
        return await self._dispatch(operation, at)
