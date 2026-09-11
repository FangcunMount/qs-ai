from dishka import Provider, Scope, provide

from qs_ai.application.integration.events import (
    DeliverResults,
    EventStore,
    ResultReceiver,
)
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.workflow_transport.results import UnconfiguredReceiver


class IntegrationProvider(Provider):
    store = provide(MySQLResultOutbox, provides=EventStore, scope=Scope.REQUEST)
    receiver = provide(UnconfiguredReceiver, provides=ResultReceiver, scope=Scope.APP)
    deliver = provide(DeliverResults, scope=Scope.REQUEST)
