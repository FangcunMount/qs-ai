from contextlib import asynccontextmanager

from dishka import Provider, Scope, provide
from fastapi.testclient import TestClient

from qs_ai.application.operations.metrics import OperationalMetrics
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.metrics import MySQLOperationalMetrics
from qs_ai.main import create_app
from qs_ai.transport.http.metrics import render_metrics


def test_metrics_endpoint_without_database_reports_failure_not_empty_backlog():
    with TestClient(create_app(Settings(_env_file=None, database_url=None))) as client:
        response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
    assert "qs_ai_database_up 0\n" in response.text
    assert "qs_ai_ready_jobs" not in response.text


def test_aggregate_endpoint_does_not_emit_unrecognized_identity_labels():
    class Reader:
        async def collect(self):
            return {"database_up": 1.0, "ready_jobs": 3.0, "testee_id": 1234.0}

    class MetricsProvider(Provider):
        @provide(scope=Scope.REQUEST, provides=OperationalMetrics, override=True)
        def reader(self) -> Reader:
            return Reader()

    app = create_app(Settings(_env_file=None, database_url=None), providers=(MetricsProvider(),))
    with TestClient(app) as client:
        response = client.get("/metrics")
    assert "qs_ai_ready_jobs 3\n" in response.text
    assert "testee" not in response.text
    assert "1234" not in response.text
    assert "/metrics" not in app.openapi()["paths"]


async def test_database_failure_does_not_leak_credentials_or_partial_readings():
    class FailingTransactions:
        @asynccontextmanager
        async def open(self):
            raise RuntimeError("mysql://user:private-password@example/db private report body")
            yield

    result = await MySQLOperationalMetrics(FailingTransactions()).collect()
    assert result == {"database_up": 0.0}
    assert "private" not in render_metrics(result)
