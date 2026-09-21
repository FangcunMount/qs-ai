from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import httpx
from dishka import Provider, Scope, provide

from qs_ai.application.execution.configuration import PublishedReportWorkflow
from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.interpretation.ports import Workflow
from qs_ai.config import Settings
from qs_ai.infrastructure.interpretation.unconfigured import UnconfiguredWorkflow
from qs_ai.infrastructure.models.router import ModelGatewayRouter
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    MySQLExecutionConfigurations,
)
from qs_ai.infrastructure.workflows.report import create_report_workflow


class GenerationProvider(Provider):
    @provide(scope=Scope.REQUEST)
    async def workflow(
        self, settings: Settings, transactions: Transactions
    ) -> AsyncIterator[Workflow]:
        options = settings.generation
        if not options.enabled:
            yield UnconfiguredWorkflow()
            return
        legacy_ready = bool(
            options.endpoint
            and urlsplit(options.endpoint).scheme == "https"
            and urlsplit(options.endpoint).hostname
            and settings.effective_deepseek_api_key
            and settings.effective_deepseek_api_key.get_secret_value().strip()
        )
        if not settings.grpc.access_address or not (legacy_ready or settings.models.bindings):
            raise ValueError("Generation requires provider bindings and QS authorization")
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            gateway = ModelGatewayRouter(client, settings)
            generation = DurableGeneration(
                MySQLExecutionStore(transactions), gateway, JSONModelCallCodec()
            )
            yield PublishedReportWorkflow(
                MySQLExecutionConfigurations(transactions), generation, create_report_workflow
            )
