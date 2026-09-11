from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import httpx
from dishka import Provider, Scope, provide

from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.execution.report_workflow import ReportWorkflow
from qs_ai.application.interpretation.ports import Workflow
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.config import Settings
from qs_ai.infrastructure.interpretation.unconfigured import UnconfiguredWorkflow
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses


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
        release = load_migrated_release(options.profile_id, options.profile_version)
        package = load_prompt(release.render_policy.template_id, release.render_policy.version)
        route = ModelRoute(
            **options.model_dump(exclude={"enabled", "endpoint", "profile_id", "profile_version"})
        )
        if route.route != release.provider_route:
            raise ValueError("Generation route does not match published profile")
        parser = QSOutputParser()
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            gateway = DeepSeekResponses(
                client, options.endpoint, settings.model_api_key.get_secret_value()
            )
            generation = DurableGeneration(
                MySQLExecutionStore(transactions), gateway, JSONModelCallCodec()
            )
            yield ReportWorkflow(generation, release, package, route, parser.schema(), parser)
