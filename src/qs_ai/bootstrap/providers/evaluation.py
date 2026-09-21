"""Evaluation-only dependencies; no implicit activation in HTTP or report workers."""

from collections.abc import AsyncIterator
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from dishka import Provider, Scope, provide

from qs_ai.application.execution.model_capacity import ModelCapacity
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.config import Settings
from qs_ai.infrastructure.models.router import ModelGatewayRouter
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker


class EvaluationProvider(Provider):
    @provide(scope=Scope.APP)
    def recovery_cursor(self) -> RecoveryCursor:
        return RecoveryCursor()

    @provide(scope=Scope.REQUEST)
    async def worker(
        self,
        settings: Settings,
        transactions: Transactions,
        routes: RouteAssets,
        schemas: SchemaAssets,
        recovery_cursor: RecoveryCursor,
        capacity: ModelCapacity,
    ) -> AsyncIterator[EvaluationWorker]:
        endpoint = settings.generation.endpoint
        if not settings.evaluation.enabled:
            raise ValueError("Evaluation execution is disabled")
        legacy_ready = bool(
            endpoint
            and urlsplit(endpoint).scheme == "https"
            and urlsplit(endpoint).hostname
            and settings.effective_deepseek_api_key
            and settings.effective_deepseek_api_key.get_secret_value().strip()
        )
        if not settings.database_url or not (legacy_ready or settings.models.bindings):
            raise ValueError("Evaluation requires provider bindings and database")
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            yield EvaluationWorker(
                transactions,
                ModelGatewayRouter(client, settings),
                routes,
                schemas,
                "evaluation:" + str(uuid4()),
                enabled=True,
                recovery_cursor=recovery_cursor,
                candidate_limit=settings.evaluation.per_run_parallel_calls,
                capacity=capacity,
            )
