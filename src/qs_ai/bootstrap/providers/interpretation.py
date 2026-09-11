from dishka import Provider, Scope, provide

from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.interpretation.ports import (
    EvidenceSource,
    ExecutionStore,
    IdentityVerifier,
    UnitOfWorkFactory,
    Workflow,
)
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.infrastructure.interpretation.unconfigured import (
    UnconfiguredEvidenceSource,
    UnconfiguredIdentity,
    UnconfiguredWorkflow,
)
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory


class InterpretationProvider(Provider):
    identity = provide(UnconfiguredIdentity, provides=IdentityVerifier, scope=Scope.APP)
    source = provide(UnconfiguredEvidenceSource, provides=EvidenceSource, scope=Scope.APP)
    workflow = provide(UnconfiguredWorkflow, provides=Workflow, scope=Scope.REQUEST)
    uows = provide(MySQLUnitOfWorkFactory, provides=UnitOfWorkFactory, scope=Scope.REQUEST)
    store = provide(MySQLExecutionStore, provides=ExecutionStore, scope=Scope.REQUEST)
    service = provide(InterpretationService, scope=Scope.REQUEST)
    worker = provide(ExecuteNext, scope=Scope.REQUEST)
