from dishka import Provider, Scope, provide

from qs_ai.application.evaluation.management import EvaluationManagementStore
from qs_ai.application.evaluation.requests import EvaluationRequests
from qs_ai.application.governance.profile_registration import ProfileRegistrar
from qs_ai.application.governance.prompt_drafts import PromptDraftStore
from qs_ai.application.governance.prompt_freeze import PromptFreezer
from qs_ai.application.governance.publication import PublicationStore
from qs_ai.application.governance.suite_registration import SuiteRegistrar
from qs_ai.application.integration.events import (
    DeliverResults,
    EventStore,
    ResultReceiver,
)
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_requests import MySQLEvaluationRequests
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import MySQLSuiteRegistrar
from qs_ai.infrastructure.persistence.mysql.profile_registrations import MySQLProfileRegistrar
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import MySQLPromptFreezer
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.workflow_transport.results import UnconfiguredReceiver


class IntegrationProvider(Provider):
    suite_registrar = provide(MySQLSuiteRegistrar, provides=SuiteRegistrar, scope=Scope.REQUEST)
    profile_registrar = provide(
        MySQLProfileRegistrar, provides=ProfileRegistrar, scope=Scope.REQUEST
    )
    prompt_freezer = provide(MySQLPromptFreezer, provides=PromptFreezer, scope=Scope.REQUEST)
    prompt_drafts = provide(MySQLPromptDrafts, provides=PromptDraftStore, scope=Scope.REQUEST)
    publications = provide(MySQLPublications, provides=PublicationStore, scope=Scope.REQUEST)
    run_creator = provide(MySQLRunCreator, scope=Scope.REQUEST)
    evaluation_requests = provide(
        MySQLEvaluationRequests, provides=EvaluationRequests, scope=Scope.REQUEST
    )
    evaluation_management = provide(
        MySQLEvaluationManagement, provides=EvaluationManagementStore, scope=Scope.REQUEST
    )

    @provide(scope=Scope.REQUEST)
    def store(self, transactions: Transactions, settings: Settings) -> EventStore:
        return MySQLResultOutbox(transactions, settings.delivery.max_retry_seconds)

    receiver = provide(UnconfiguredReceiver, provides=ResultReceiver, scope=Scope.APP)
    deliver = provide(DeliverResults, scope=Scope.REQUEST)
