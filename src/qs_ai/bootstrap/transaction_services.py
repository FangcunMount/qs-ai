"""One application sender, with request-scoped original business transaction adapters."""

from dataclasses import dataclass, field
from typing import Any

from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_governance_dispatch import (
    GovernanceStateDispatcher,
)
from qs_ai.infrastructure.workflow_transport.solution_preparation_dispatch import (
    SolutionPreparationDispatcher,
)
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
    TransactionalEvaluationManagement,
)
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_requests import (
    TransactionalEvaluationRequests,
)
from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
    TransactionalInterpretationExecutionStore,
)
from qs_ai.infrastructure.workflow_transport.transactional_solutions import TransactionalSolutions


@dataclass(frozen=True)
class TransactionServices:
    runtime: TransactionRuntime = field(repr=False)
    archives: TransactionArchive = field(repr=False)
    signing_key: Any = field(repr=False)
    recipient_key: Any = field(repr=False)
    worker_owner: str
    database_seconds: float = 2

    def keys(self) -> dict[str, Any]:
        return dict(
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            database_seconds=self.database_seconds,
        )

    def execution(self, original: MySQLExecutionStore) -> TransactionalInterpretationExecutionStore:
        return TransactionalInterpretationExecutionStore(
            original, self.archives, self.runtime, **self.keys()
        )

    def management(self, original: MySQLEvaluationManagement) -> TransactionalEvaluationManagement:
        dispatch = GovernanceStateDispatcher(
            original.transactions,
            self.archives,
            self.runtime,
            runtime_limits=original.runtime_limits,
            resolution_management=original,
            **self.keys(),
        )
        return TransactionalEvaluationManagement(original, dispatch)

    def requests(self, creator: MySQLRunCreator) -> TransactionalEvaluationRequests:
        return TransactionalEvaluationRequests(creator, self.archives, self.runtime, **self.keys())

    def solutions(self, original: MySQLSolutions) -> TransactionalSolutions:
        return TransactionalSolutions(
            original,
            SolutionPreparationDispatcher(original, self.archives, self.runtime, **self.keys()),
        )
