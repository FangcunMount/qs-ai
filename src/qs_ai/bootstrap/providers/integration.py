from dishka import Provider, Scope, provide

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy, EvaluationCapacityReader
from qs_ai.application.evaluation.catalog import EvaluationCatalog
from qs_ai.application.evaluation.diagnostics import EvaluationDiagnostics
from qs_ai.application.evaluation.management import EvaluationManagementStore
from qs_ai.application.evaluation.planning import EvaluationPlanner
from qs_ai.application.evaluation.requests import EvaluationRequests
from qs_ai.application.governance.asset_catalog import AssetCatalog
from qs_ai.application.governance.asset_references import PolicyReferences
from qs_ai.application.governance.flow import FlowReader
from qs_ai.application.governance.profile_lifecycle import ProfileLifecycleReader
from qs_ai.application.governance.profile_registration import ProfileRegistrar
from qs_ai.application.governance.prompt_drafts import PromptDraftStore
from qs_ai.application.governance.prompt_freeze import PromptFreezer
from qs_ai.application.governance.prompt_lifecycle import PromptLifecycleReader
from qs_ai.application.governance.publication import PublicationStore
from qs_ai.application.governance.semantic_drafts import SemanticDrafts
from qs_ai.application.governance.solution_models import EditableModelPolicy
from qs_ai.application.governance.solutions import SolutionStore
from qs_ai.application.governance.suite_registration import SuiteRegistrar
from qs_ai.application.integration.events import (
    DeliverResults,
    EventStore,
    ResultReceiver,
)
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.asset_catalog import MySQLAssetCatalog
from qs_ai.infrastructure.persistence.mysql.asset_references import MySQLPolicyReferences
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_capacity import MySQLEvaluationCapacity
from qs_ai.infrastructure.persistence.mysql.evaluation_catalog import MySQLEvaluationCatalog
from qs_ai.infrastructure.persistence.mysql.evaluation_diagnostics import MySQLEvaluationDiagnostics
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_planning import MySQLEvaluationPlanner
from qs_ai.infrastructure.persistence.mysql.evaluation_requests import MySQLEvaluationRequests
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import MySQLSuiteRegistrar
from qs_ai.infrastructure.persistence.mysql.flow import MySQLFlowReader
from qs_ai.infrastructure.persistence.mysql.profile_lifecycle import MySQLProfileLifecycle
from qs_ai.infrastructure.persistence.mysql.profile_registrations import MySQLProfileRegistrar
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import MySQLPromptFreezer
from qs_ai.infrastructure.persistence.mysql.prompt_lifecycle import MySQLPromptLifecycleReader
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.semantic_drafts import MySQLSemanticDrafts
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.workflow_transport.results import UnconfiguredReceiver


class IntegrationProvider(Provider):
    flows = provide(MySQLFlowReader, provides=FlowReader, scope=Scope.REQUEST)
    solutions = provide(MySQLSolutions, provides=SolutionStore, scope=Scope.REQUEST)

    @provide(scope=Scope.APP)
    def editable_models(self, settings: Settings) -> EditableModelPolicy:
        return EditableModelPolicy(settings.governance_models)

    @provide(scope=Scope.APP)
    def evaluation_capacity_policy(self, settings: Settings) -> EvaluationCapacityPolicy:
        return EvaluationCapacityPolicy(
            settings.evaluation.daily_provider_calls, settings.evaluation.max_active_runs
        )

    prompt_lifecycle = provide(
        MySQLPromptLifecycleReader, provides=PromptLifecycleReader, scope=Scope.REQUEST
    )
    profile_lifecycle = provide(
        MySQLProfileLifecycle, provides=ProfileLifecycleReader, scope=Scope.REQUEST
    )
    policy_references = provide(
        MySQLPolicyReferences, provides=PolicyReferences, scope=Scope.REQUEST
    )
    asset_catalog = provide(MySQLAssetCatalog, provides=AssetCatalog, scope=Scope.REQUEST)
    evaluation_planner = provide(
        MySQLEvaluationPlanner, provides=EvaluationPlanner, scope=Scope.REQUEST
    )
    suite_registrar = provide(MySQLSuiteRegistrar, provides=SuiteRegistrar, scope=Scope.REQUEST)
    profile_registrar = provide(
        MySQLProfileRegistrar, provides=ProfileRegistrar, scope=Scope.REQUEST
    )
    prompt_freezer = provide(MySQLPromptFreezer, provides=PromptFreezer, scope=Scope.REQUEST)
    semantic_drafts = provide(MySQLSemanticDrafts, provides=SemanticDrafts, scope=Scope.REQUEST)
    prompt_drafts = provide(MySQLPromptDrafts, provides=PromptDraftStore, scope=Scope.REQUEST)
    publications = provide(MySQLPublications, provides=PublicationStore, scope=Scope.REQUEST)
    run_creator = provide(MySQLRunCreator, scope=Scope.REQUEST)
    evaluation_requests = provide(
        MySQLEvaluationRequests, provides=EvaluationRequests, scope=Scope.REQUEST
    )
    evaluation_capacity = provide(
        MySQLEvaluationCapacity, provides=EvaluationCapacityReader, scope=Scope.REQUEST
    )
    evaluation_catalog = provide(
        MySQLEvaluationCatalog, provides=EvaluationCatalog, scope=Scope.REQUEST
    )
    evaluation_diagnostics = provide(
        MySQLEvaluationDiagnostics, provides=EvaluationDiagnostics, scope=Scope.REQUEST
    )
    evaluation_management = provide(
        MySQLEvaluationManagement, provides=EvaluationManagementStore, scope=Scope.REQUEST
    )

    @provide(scope=Scope.REQUEST)
    def store(self, transactions: Transactions, settings: Settings) -> EventStore:
        return MySQLResultOutbox(transactions, settings.delivery.max_retry_seconds)

    receiver = provide(UnconfiguredReceiver, provides=ResultReceiver, scope=Scope.APP)
    deliver = provide(DeliverResults, scope=Scope.REQUEST)
