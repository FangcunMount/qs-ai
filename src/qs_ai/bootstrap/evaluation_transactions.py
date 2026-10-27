"""Complete evaluation transaction composition, borrowing one host and SDK owner."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from qs_ai.application.execution.model_capacity import ModelCapacity
from qs_ai.application.interpretation.provider import MessagesGateway
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.cancellation_drain_runtime import (
    CancellationDrainRuntime,
)
from qs_ai.infrastructure.workflow_transport.candidate_completion_sender import (
    CandidateCompletionSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_completion_state import (
    CandidateCompletionPlanner,
)
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
    CandidateDispatchRecovery,
    CandidateDispatchSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_state import (
    CandidateDispatchPlanner,
)
from qs_ai.infrastructure.workflow_transport.candidate_recovery_runtime import (
    CandidateRecoveryRuntime,
)
from qs_ai.infrastructure.workflow_transport.candidate_runtime import CandidateRuntime
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_dispatch import (
    PreflightStateDispatcher,
)
from qs_ai.infrastructure.workflow_transport.serial_generation_sender import SerialGenerationSender
from qs_ai.infrastructure.workflow_transport.serial_generation_state import SerialGenerationPlanner
from qs_ai.infrastructure.workflow_transport.serial_recovery_runtime import SerialRecoveryRuntime
from qs_ai.infrastructure.workflow_transport.serial_runtime import SerialRuntime
from qs_ai.infrastructure.workflow_transport.serial_semantic_sender import SerialSemanticSender
from qs_ai.infrastructure.workflow_transport.serial_semantic_state import SerialSemanticPlanner
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
    WorkerDispatchRecovery,
    WorkerDispatchSender,
)
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import WorkerDispatchPlanner


def transaction_evaluation_worker(
    transactions: Transactions,
    archives: TransactionArchive,
    runtime: AuthorizedStateTransactions,
    gateway: MessagesGateway,
    routes: RouteAssets,
    schemas: SchemaAssets,
    owner: str,
    *,
    signing_key: Any,
    recipient_key: Any,
    candidate_limit: int,
    capacity: ModelCapacity,
    recovery_cursor: RecoveryCursor,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    database_seconds: float = 2,
) -> EvaluationWorker:
    """Construct all writers together; no network, SQL, background work or Start.

    The application still owns the SDK's lifecycle. The persisted due work is
    the explicit host policy for exact old Aborted recovery, never Checker
    permission to repeat an already committed model dispatch.
    """
    keys = dict(
        signing_key=signing_key, recipient_key=recipient_key, database_seconds=database_seconds
    )
    serial_dispatch = WorkerDispatchSender(
        transactions,
        archives,
        runtime,
        schemas,
        owner,
        WorkerDispatchPlanner(
            transactions, schemas, clock=clock, **keys, route_resolver=runtime.routing
        ),
        capacity=capacity,
    )
    serial = SerialRuntime(
        serial_dispatch,
        SerialGenerationSender(
            transactions,
            archives,
            runtime,
            SerialGenerationPlanner(transactions, routes, **keys, route_resolver=runtime.routing),
        ),
        SerialSemanticSender(
            transactions,
            archives,
            runtime,
            SerialSemanticPlanner(transactions, **keys, route_resolver=runtime.routing),
        ),
        dispatch_recovery=WorkerDispatchRecovery(serial_dispatch),
    )
    candidate_dispatch = CandidateDispatchSender(
        transactions,
        archives,
        runtime,
        owner,
        CandidateDispatchPlanner(
            transactions,
            schemas,
            limit=candidate_limit,
            clock=clock,
            **keys,
            route_resolver=runtime.routing,
        ),
        capacity=capacity,
    )
    candidate = CandidateRuntime(
        candidate_dispatch,
        CandidateCompletionSender(
            transactions,
            archives,
            runtime,
            CandidateCompletionPlanner(transactions, **keys, route_resolver=runtime.routing),
        ),
        dispatch_recovery=CandidateDispatchRecovery(candidate_dispatch),
    )
    return EvaluationWorker(
        transactions,
        gateway,
        routes,
        schemas,
        owner,
        enabled=True,
        candidate_limit=candidate_limit,
        capacity=capacity,
        recovery_cursor=recovery_cursor,
        clock=clock,
        preflight_dispatch=PreflightStateDispatcher(transactions, archives, runtime, **keys),
        serial_runtime=serial,
        candidate_runtime=candidate,
        serial_recovery_dispatch=SerialRecoveryRuntime(
            transactions, archives, runtime, routes, recover_aborted=True, **keys
        ),
        candidate_recovery_dispatch=CandidateRecoveryRuntime(
            transactions, archives, runtime, recover_aborted=True, **keys
        ),
        cancel_drain_dispatch=CancellationDrainRuntime(
            transactions, archives, runtime, recover_aborted=True, **keys
        ),
    )
