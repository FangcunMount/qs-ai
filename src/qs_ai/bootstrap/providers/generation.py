from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import httpx
from dishka import Provider, Scope, provide

from qs_ai.application.execution.configuration import PublishedReportWorkflow
from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.interpretation.ports import Workflow
from qs_ai.config import Settings
from qs_ai.infrastructure.interpretation.unconfigured import UnconfiguredWorkflow
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    MySQLExecutionConfigurations,
)
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses
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
        if (
            not options.endpoint
            or urlsplit(options.endpoint).scheme != "https"
            or not settings.model_api_key
            or not settings.model_api_key.get_secret_value().strip()
            or not settings.grpc.access_address
        ):
            raise ValueError(
                "Generation requires HTTPS endpoint, model credential and QS authorization"
            )
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            gateway = DeepSeekResponses(
                client, options.endpoint, settings.model_api_key.get_secret_value()
            )
            generation = DurableGeneration(
                MySQLExecutionStore(transactions), gateway, JSONModelCallCodec()
            )
            yield PublishedReportWorkflow(
                MySQLExecutionConfigurations(transactions), generation, create_report_workflow
            )
