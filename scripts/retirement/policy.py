"""Reviewed fixed scope. No user-supplied patterns or SQL predicates."""

COLLECTIONS = (
    "ai_explanation_generations",
    "ai_explanation_runs",
    "ai_explanation_artifacts",
    "ai_explanation_profiles",
    "ai_explanation_prompt_evaluations",
    "ai_explanation_prompt_evaluation_rechecks",
    "ai_explanation_prompt_evaluation_daily_budgets",
    "ai_explanation_participant_daily_budgets",
    "ai_explanation_participant_active_capacity",
)
EVENTS = (
    "interpretation.ai_explanation.requested",
    "interpretation.ai_explanation.retry.requested",
    "interpretation.ai_explanation.lease_recovery.requested",
    "interpretation.ai_explanation.generated",
    "interpretation.ai_explanation.failed",
    "interpretation.ai_explanation.prompt_evaluation.step_requested",
)
SHARED_TABLES = ("domain_event_outbox", "event_delivery_dead_letter", "retry_event_hold")
TECHNICAL_TABLES = (
    "checkpoint_leases",
    "execution_leases",
    "evaluation_checkpoints",
    "checkpoint_migrations",
    "checkpoints",
    "checkpoint_blobs",
    "checkpoint_writes",
)


def old_event(row: dict, *, payload: bool = False) -> bool:
    import json

    if not payload:
        return row.get("event_type") in EVENTS
    try:
        envelope = json.loads(row["payload_json"])
    except (ValueError, TypeError, KeyError):
        return False
    # QS transport uses the component-base messaging envelope's top-level type.
    return isinstance(envelope, dict) and envelope.get("type") in EVENTS
