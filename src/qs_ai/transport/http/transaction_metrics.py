"""Fixed transaction telemetry schema; no DB work or identifying labels."""

import math

from qs_ai.application.operations.metrics import TransactionMetricsSnapshot

PREFIX = "reliable_messaging_transaction_"
VALUES = {
    "sampler_running": (
        "Host sampler has not actually terminated; includes real DB work after cancellation."
    ),
    "sdk_ready": "SDK readiness independent of host protection and admission.",
    "sdk_stopping": "SDK shutdown has started.",
    "sdk_stopped": (
        "SDK client resources actually stopped; host sampler ownership is reported separately."
    ),
    "observation_inflight": (
        "A real metadata read is still owned by this sampler; cancellation "
        "does not release its capacity."
    ),
    "observation_available": (
        "Last metadata sampling succeeded; failed sampling never means zero pending."
    ),
    "observation_fresh": "Cached successful metadata is within the host sampling budget.",
    "observation_errors_total": "Metadata samples that returned errors or invalid values.",
    "reserved_executions": "Prepared executions that have not begun half preparation.",
    "preparing_executions": "Prepared executions that have begun half preparation.",
    "prepared_expiry_age_seconds": (
        "Oldest time since preparing execution lease expiry; not send age or rollback proof."
    ),
    "confirmation_gap_branches": (
        "Terminal branches with half audit but no positive final audit; not Broker backlog."
    ),
    "oldest_confirmation_observation_age_seconds": (
        "Oldest half audit age, otherwise final audit age; exposure age lower "
        "bound, not actual Broker pending age."
    ),
    "unaged_confirmation_gap_branches": (
        "Confirmation audit gaps without an audit timestamp; not fresh zero-age work."
    ),
    "untracked_aborted_branches": (
        "Aborted branches with no half outcome; may be unsent or have lost their half audit."
    ),
    "unobserved_committed_branches": "Committed branches lacking Prepared half audit.",
    "metadata_conflicts": "Contradictory terminal audit metadata or missing witnesses.",
    "admission_protected": "Host protection episode is latched until a matching named review.",
    "protection_generation": "Process-local protection episode generation; never a metric label.",
    "admission_open": (
        "Host new-send admission is open with fresh evidence; Checker may remain live when zero."
    ),
    "active_sends": "Actual SDK active sends.",
    "running_checks": "Actual SDK running check workers.",
    "queued_checks": (
        "Admitted checks outside running workers, including waiting or "
        "completion capacity; not extra worker capacity."
    ),
    "check_workers_limit": "Configured check worker limit.",
    "check_queue_limit": "Configured waiting check limit.",
    "reconciled_executions_total": (
        "Executions durably aborted by SDK maintenance; excludes host business journal maintenance."
    ),
}

OUTCOMES = (
    ("half", "prepared"),
    ("half", "rejected"),
    ("half", "unknown"),
    ("local", "committed"),
    ("local", "aborted"),
    ("local", "unknown"),
    ("final", "commit_confirmed"),
    ("final", "rollback_confirmed"),
    ("final", "unknown"),
)
CHECKS = (
    "commit",
    "rollback",
    "unknown",
    "invalid",
    "rejected",
    "database_error",
    "callback_error",
)


def render_transaction_metrics(snapshot: TransactionMetricsSnapshot | None) -> str:
    if snapshot is None:
        return ""
    lines: list[str] = []

    def family(name: str, help_text: str, kind: str, rows: list[tuple[str, int | float]]) -> None:
        safe = [
            (labels, value)
            for labels, value in rows
            if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0
        ]
        if not safe:
            return
        metric = PREFIX + name
        lines.extend((f"# HELP {metric} {help_text}", f"# TYPE {metric} {kind}"))
        lines.extend(f'{metric}{{owner="ai.mysql"{labels}}} {value:g}' for labels, value in safe)

    for name, description in VALUES.items():
        if name in snapshot.values:
            family(
                name,
                description,
                "counter" if name.endswith("_total") else "gauge",
                [("", snapshot.values[name])],
            )
    family(
        "outcomes_total",
        (
            "Cumulative local execution outcomes and half/final branch outcomes; "
            "never current backlog."
        ),
        "counter",
        [
            (f',stage="{stage}",outcome="{outcome}"', snapshot.outcomes[(stage, outcome)])
            for stage, outcome in OUTCOMES
            if (stage, outcome) in snapshot.outcomes
        ],
    )
    family(
        "checks_total",
        "Cumulative SDK check decisions and errors, not pending work.",
        "counter",
        [
            (f',outcome="{outcome}"', snapshot.checks[outcome])
            for outcome in CHECKS
            if outcome in snapshot.checks
        ],
    )
    family(
        "recovery_rpc_confirmed_total",
        "Positively acknowledged recovery RPCs, not Checker decisions or consumer receipts.",
        "counter",
        [
            (f',outcome="{outcome}"', snapshot.recovery_confirmed[outcome])
            for outcome in ("commit", "rollback")
            if outcome in snapshot.recovery_confirmed
        ],
    )
    family(
        "recovery_audits_total",
        "Positive recovery RPC audit persistence outcomes; missing audit is an observation gap.",
        "counter",
        [
            (f',outcome="{outcome}"', snapshot.recovery_audits[outcome])
            for outcome in ("recorded", "error", "unsupported")
            if outcome in snapshot.recovery_audits
        ],
    )
    # Python has no independent recovery RPC failure counter. Do not synthesize
    # Go's recovery_rpc_failures_total from the broader callback_errors field.
    return "\n".join(lines) + ("\n" if lines else "")
