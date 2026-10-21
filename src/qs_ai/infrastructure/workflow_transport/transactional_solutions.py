"""Explicit Solution transaction adapter; default DI and host lifecycle stay unchanged."""

from datetime import datetime
from typing import Any
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solutions import CreateSolution, PrepareSolution, SaveSolution
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.workflow_transport.solution_preparation_dispatch import (
    SolutionPreparationDispatcher,
)
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationInput,
)


class TransactionalSolutions(MySQLSolutions):
    def __init__(self, original: MySQLSolutions, dispatch: SolutionPreparationDispatcher) -> None:
        super().__init__(original.transactions, original.settings)
        self._original, self._dispatch = original, dispatch

    async def apply(
        self,
        scope: DraftScope,
        solution_id: UUID,
        command: CreateSolution | SaveSolution | PrepareSolution,
        at: datetime,
    ) -> dict[str, Any]:
        if not isinstance(command, PrepareSolution):
            return await self._original.apply(scope, solution_id, command, at)
        incoming = SolutionPreparationInput(scope, solution_id, command)
        return await self._dispatch(incoming, at)
