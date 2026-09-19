from collections.abc import AsyncIterator

from dishka import Provider, Scope, from_context, provide

from qs_ai.application.operations.health import CheckReadiness, DatabaseProbe
from qs_ai.application.operations.metrics import OperationalMetrics
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, MySQLProbe, Transactions
from qs_ai.infrastructure.persistence.mysql.metrics import MySQLOperationalMetrics


class RuntimeProvider(Provider):
    settings = from_context(provides=Settings, scope=Scope.APP)

    @provide(scope=Scope.APP)
    async def database(self, settings: Settings) -> AsyncIterator[Database]:
        url = settings.database_url.get_secret_value() if settings.database_url else None
        database = Database(url, **settings.database.model_dump())
        try:
            yield database
        finally:
            await database.close()


class PersistenceProvider(Provider):
    probe = provide(MySQLProbe, provides=DatabaseProbe, scope=Scope.REQUEST)
    transactions = provide(Transactions, scope=Scope.REQUEST)
    metrics = provide(MySQLOperationalMetrics, provides=OperationalMetrics, scope=Scope.REQUEST)


class OperationsProvider(Provider):
    readiness = provide(CheckReadiness, scope=Scope.REQUEST)
