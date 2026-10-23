from time import monotonic
from unittest.mock import Mock

from dishka import Provider, Scope, provide
from fastapi.testclient import TestClient

from qs_ai.application.operations.metrics import TransactionMetrics, TransactionMetricsSnapshot
from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.bootstrap.transaction_observation import ProtocolMonitor
from qs_ai.bootstrap.transaction_protection import ProtocolSample
from qs_ai.config import Settings
from qs_ai.main import create_app
from qs_ai.transport.http.transaction_metrics import render_transaction_metrics


def test_protocol_render_whitelists_aggregate_schema_and_retains_unavailable_debt():
    snapshot = TransactionMetricsSnapshot(
        {
            "observation_available": 0,
            "confirmation_gap_branches": 7,
            "tenant_id": 1234,
            "unaged_confirmation_gap_branches": float("nan"),
        },
        {("final", "unknown"): 10000, ("wire", "secret"): 1234},
        {"unknown": 2, "participant_id": 1234},
        {"commit": 3},
        {"recorded": 3},
    )
    output = render_transaction_metrics(snapshot)
    assert 'confirmation_gap_branches{owner="ai.mysql"} 7\n' in output
    assert 'outcomes_total{owner="ai.mysql",stage="final",outcome="unknown"} 10000\n' in output
    assert "tenant" not in output and "participant" not in output
    assert "1234" not in output and "secret" not in output and "NaN" not in output
    assert "unaged_confirmation_gap_branches" not in output
    assert render_transaction_metrics(None) == ""


def test_snapshot_normalizes_python_admitted_futures_without_inventing_rpc_failures():
    raw = {
        "ready": True,
        "running_checks": 1,
        "pending_checks": 16,
        "final_unknown_results": 10000,
        "callback_errors": 9,
    }
    driver = Mock()
    driver.stats.return_value = raw

    async def read(budget):
        raise AssertionError("metrics snapshot opened a database session")

    monitor = ProtocolMonitor(read, driver.stats, workers=1, queue=16)
    monitor.policy.observe(
        monotonic(),
        ProtocolSample(gaps=3, confirmation_age=30),
        failed=False,
        binding_conflict=False,
        full=False,
        freshness=12,
    )
    runtime = TransactionRuntime(driver, admitted=True, monitor=monitor)
    snapshot = runtime.transaction_metrics_snapshot()
    assert snapshot is not None
    assert snapshot.values["running_checks"] == 1 and snapshot.values["queued_checks"] == 15
    assert snapshot.values["confirmation_gap_branches"] == 3
    assert snapshot.outcomes[("final", "unknown")] == 10000
    assert "recovery_rpc_failures_total" not in render_transaction_metrics(snapshot)
    assert raw["ready"] is True


def test_existing_http_metrics_injects_protocol_reader_without_identity_labels():
    reads = []

    class Telemetry:
        def transaction_metrics_snapshot(self):
            reads.append(True)
            return TransactionMetricsSnapshot(
                {"observation_available": 0, "confirmation_gap_branches": 7}, {}, {}, {}, {}
            )

    class TelemetryProvider(Provider):
        @provide(scope=Scope.APP, provides=TransactionMetrics, override=True)
        def reader(self) -> Telemetry:
            return Telemetry()

    app = create_app(Settings(_env_file=None, database_url=None), providers=(TelemetryProvider(),))
    with TestClient(app) as client:
        response = client.get("/metrics")
    assert response.status_code == 200 and len(reads) == 1
    assert 'confirmation_gap_branches{owner="ai.mysql"} 7\n' in response.text
    assert "qs_ai_database_up 0\n" in response.text
    assert "/metrics" not in app.openapi()["paths"]
