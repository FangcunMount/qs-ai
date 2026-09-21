from collections.abc import AsyncIterator

from dishka import Provider, Scope, from_context, provide

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.evaluation.execution_mode import EvaluationRuntimeLimits, ExecutionMode
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.application.governance.configuration_status import ConfigurationStatus
from qs_ai.application.governance.quotas import QuotaBaseline, QuotaStore, QuotaValues
from qs_ai.application.operations.health import CheckReadiness, DatabaseProbe
from qs_ai.application.operations.metrics import OperationalMetrics
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.configuration_status import MySQLConfigurationStatus
from qs_ai.infrastructure.persistence.mysql.database import Database, MySQLProbe, Transactions
from qs_ai.infrastructure.persistence.mysql.metrics import MySQLOperationalMetrics
from qs_ai.infrastructure.persistence.mysql.quotas import MySQLQuotas


class RuntimeProvider(Provider):
    settings = from_context(provides=Settings, scope=Scope.APP)

    @provide(scope=Scope.APP)
    def evaluation_execution_mode(self, settings: Settings) -> ExecutionMode:
        return ExecutionMode(
            "candidate_v2" if settings.evaluation.candidate_mode_enabled else "serial_v1"
        )

    @provide(scope=Scope.APP)
    def evaluation_runtime_limits(self, settings: Settings) -> EvaluationRuntimeLimits:
        return EvaluationRuntimeLimits(
            min(
                settings.evaluation.parallel_calls,
                settings.evaluation.per_run_parallel_calls,
                settings.evaluation.concurrency,
            )
        )

    @provide(scope=Scope.APP)
    def model_capacity(self, settings: Settings) -> ModelCapacity:
        return ModelCapacity(
            {
                key: ProviderCapacity(**value)
                for key, value in settings.model_capacity.model_dump().items()
            },
            settings.evaluation.parallel_calls,
        )

    @provide(scope=Scope.APP)
    async def database(self, settings: Settings) -> AsyncIterator[Database]:
        url = settings.database_url.get_secret_value() if settings.database_url else None
        database = Database(url, **settings.database.model_dump())
        try:
            yield database
        finally:
            await database.close()


class PersistenceProvider(Provider):
    configuration_status = provide(
        MySQLConfigurationStatus, provides=ConfigurationStatus, scope=Scope.REQUEST
    )
    quotas = provide(MySQLQuotas, provides=QuotaStore, scope=Scope.REQUEST)

    @provide(scope=Scope.APP)
    def quota_baseline(self, settings: Settings) -> QuotaBaseline:
        defaults = QuotaValues(
            ParticipantCapacityPolicy(**settings.participant_capacity.model_dump()),
            EvaluationCapacityPolicy(
                settings.evaluation.daily_provider_calls, settings.evaluation.max_active_runs
            ),
        )
        ceilings = (
            QuotaValues.parse(settings.quota_ceilings.model_dump())
            if settings.quota_ceilings
            else defaults
        )
        return QuotaBaseline(defaults, ceilings)

    @provide(scope=Scope.APP)
    def optional_quota_baseline(self, baseline: QuotaBaseline) -> QuotaBaseline | None:
        return baseline

    probe = provide(MySQLProbe, provides=DatabaseProbe, scope=Scope.REQUEST)
    transactions = provide(Transactions, scope=Scope.REQUEST)
    metrics = provide(MySQLOperationalMetrics, provides=OperationalMetrics, scope=Scope.REQUEST)


class OperationsProvider(Provider):
    readiness = provide(CheckReadiness, scope=Scope.REQUEST)
