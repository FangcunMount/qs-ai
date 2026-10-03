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


def test_mq_metrics_use_existing_di_and_never_claim_missing_storage_is_empty():
    settings = Settings(
        _env_file=None,
        database_url=None,
        messaging={
            "enabled": True,
            "nsqd": {"127.0.0.1:4150": "http://127.0.0.1:4151"},
            "signing_key_file": "inert-not-read",
            "decrypt_key_files": {"inert": "inert-not-read"},
            "qs_signer_files": {"inert": "inert-not-read"},
            "qs_recipient_key_file": "inert-not-read",
        },
    )
    with TestClient(create_app(settings)) as client:
        response = client.get("/metrics")
    assert response.status_code == 200
    assert "qs_ai_database_up 0\n" in response.text
    assert "qs_ai_mq_enabled 1\n" in response.text
    assert "qs_ai_mq_observation_available 0\n" in response.text
    assert "qs_ai_mq_staged_events" not in response.text
    assert "qs_ai_mq_quarantine_" not in response.text
    assert "inert-not-read" not in response.text


async def test_database_failure_does_not_leak_credentials_or_partial_readings():
    class FailingTransactions:
        @asynccontextmanager
        async def open(self):
            raise RuntimeError("mysql://user:private-password@example/db private report body")
            yield

    result = await MySQLOperationalMetrics(
        FailingTransactions(), Settings(_env_file=None)
    ).collect()
    assert result == {"database_up": 0.0, "mq_enabled": 0.0, "mq_observation_available": 0.0}
    assert "private" not in render_metrics(result)
