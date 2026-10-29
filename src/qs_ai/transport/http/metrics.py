"""Internal Prometheus text endpoint; fixed aggregate names, no dynamic labels."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from fastapi.responses import Response

from qs_ai.application.operations.diagnostics import render_process_metrics
from qs_ai.application.operations.metrics import OperationalMetrics, TransactionMetrics
from qs_ai.transport.http.transaction_metrics import render_transaction_metrics

router = APIRouter(route_class=DishkaRoute)


# Fixed names also prevent an adapter from accidentally exposing identifying labels.
DESCRIPTIONS = {
    "mq_enabled": "Whether MQ is configured; this does not prove delivery readiness.",
    "mq_observation_available": "Whether the committed MQ snapshot is available.",
    "mq_pending_interpretation_receipts": "MQ interpretation states awaiting QS business ACK.",
    "mq_oldest_pending_interpretation_receipt_seconds": (
        "Oldest MQ interpretation state awaiting ACK."
    ),
    "mq_held_commands": "Technically held Inbox commands, separate from business refusal.",
    "mq_duplicate_observations_available": "Whether durable duplicate observations exist.",
    "mq_payload_error_observations_available": "Whether durable payload error observations exist.",
    "mq_observations_history_complete": "Recording does not reconstruct missing history.",
    "mq_observations_recording_since_epoch_seconds": "UTC start of durable recording coverage.",
    "mq_recorded_duplicate_command": "Committed duplicate command observations since installation.",
    "mq_recorded_duplicate_ack": "Committed duplicate stored-ACK observations since installation.",
    "mq_recorded_payload_fetch_unavailable": "Durably recorded unavailable reference reads.",
    "mq_recorded_payload_fetch_reference_mismatch": "Recorded remote reference mismatches.",
    "mq_recorded_payload_fetch_workload_denied": "Durably recorded fetch workload refusals.",
    "mq_recorded_payload_serve_reference_mismatch": "Durably recorded local reference mismatches.",
    "mq_recorded_payload_serve_workload_denied": "Durably recorded local workload refusals.",
    "mq_recorded_payload_serve_storage_unavailable": "Recorded local storage read failures.",
    "mq_quarantine_identity_conflict_records": (
        "Retained identity-conflict records, not lifetime errors."
    ),
    "mq_quarantine_authentication_failed_records": "Retained authentication-failure records.",
    "mq_quarantine_invalid_wire_records": "Retained invalid-wire records.",
    "mq_quarantine_invalid_failure_wire_records": "Retained invalid failure-wire records.",
    "mq_quarantine_handler_failed_records": "Retained technical handler-failure records.",
    "database_up": "Whether the entire read-only metrics snapshot succeeded.",
    "ready_jobs": "Queued execution jobs whose scheduled time has arrived.",
    "oldest_ready_job_seconds": "Age of the oldest due execution job, zero when empty.",
    "expired_job_leases": "Execution jobs with expired leases awaiting recovery.",
    "pending_results": "Result events not yet acknowledged by QS.",
    "oldest_pending_result_seconds": "Age of the oldest unacknowledged result event.",
    "pending_results_without_timestamp": "Pending events missing their creation timestamp.",
    "unresolved_model_calls": "Current unknown calls or dispatches older than five minutes.",
    "database_account_connections": "Connections for this database and account, including scrape.",
    "provider_response_samples_5m": "Recorded model responses created in the last five minutes.",
    "provider_response_max_seconds_5m": "Maximum full provider response time in that window.",
    "capacity_rejections_24h": "Participant daily capacity refusals created in the last 24 hours.",
}


def render_metrics(values: dict[str, float]) -> str:
    lines: list[str] = []
    for name, description in DESCRIPTIONS.items():
        if name in values:
            metric = f"qs_ai_{name}"
            lines.extend(
                (
                    f"# HELP {metric} {description}",
                    f"# TYPE {metric} gauge",
                    f"{metric} {values[name]:g}",
                )
            )
    return "\n".join(lines) + "\n"


@router.get("/metrics", include_in_schema=False)
async def metrics(
    reader: FromDishka[OperationalMetrics], protocol: FromDishka[TransactionMetrics]
) -> Response:
    return Response(
        render_metrics(await reader.collect())
        + render_process_metrics()
        + render_transaction_metrics(protocol.transaction_metrics_snapshot()),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
