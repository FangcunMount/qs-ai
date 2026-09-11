from dishka import Provider, Scope, provide

from qs_ai.application.evaluation.management import EvaluationManagementStore
from qs_ai.application.integration.events import (
    DeliverResults,
    EventStore,
    ResultReceiver,
)
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.workflow_transport.results import UnconfiguredReceiver


class IntegrationProvider(Provider):
    evaluation_management = provide(
        MySQLEvaluationManagement, provides=EvaluationManagementStore, scope=Scope.REQUEST
    )

    @provide(scope=Scope.REQUEST)
    def store(self, transactions: Transactions, settings: Settings) -> EventStore:
        return MySQLResultOutbox(transactions, settings.delivery.max_retry_seconds)

    receiver = provide(UnconfiguredReceiver, provides=ResultReceiver, scope=Scope.APP)
    deliver = provide(DeliverResults, scope=Scope.REQUEST)
