"""Evaluation-only dependencies; no implicit activation in HTTP or report workers."""

from collections.abc import AsyncIterator
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from dishka import Provider, Scope, provide

from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses


class EvaluationProvider(Provider):
    @provide(scope=Scope.APP)
    def recovery_cursor(self) -> RecoveryCursor:
        return RecoveryCursor()

    routes = provide(MySQLRouteAssets, provides=RouteAssets, scope=Scope.REQUEST)
    schemas = provide(MySQLSchemaAssets, provides=SchemaAssets, scope=Scope.REQUEST)

    @provide(scope=Scope.REQUEST)
    async def worker(
        self,
        settings: Settings,
        transactions: Transactions,
        routes: RouteAssets,
        schemas: SchemaAssets,
        recovery_cursor: RecoveryCursor,
    ) -> AsyncIterator[EvaluationWorker]:
        endpoint = settings.generation.endpoint
        if not settings.evaluation.enabled:
            raise ValueError("Evaluation execution is disabled")
        if (
            not endpoint
            or urlsplit(endpoint).scheme != "https"
            or not urlsplit(endpoint).hostname
            or not settings.model_api_key
            or not settings.model_api_key.get_secret_value().strip()
            or not settings.database_url
        ):
            raise ValueError("Evaluation requires HTTPS endpoint, model credential and database")
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            yield EvaluationWorker(
                transactions,
                DeepSeekResponses(client, endpoint, settings.model_api_key.get_secret_value()),
                routes,
                schemas,
                "evaluation:" + str(uuid4()),
                enabled=True,
                recovery_cursor=recovery_cursor,
            )
