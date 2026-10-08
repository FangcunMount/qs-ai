"""Immutable physical-name contract for the 0038/0040 storage boundary."""

import re

OLD_HEAD = "0038_messaging_observations"
INTERMEDIATE_HEAD = "0039_data_consolidation"
NEW_HEAD = "0040_module_table_names"
BATCH_SIZE = 500
RETENTION_DAYS = 30
COPY_DEADLINE_SECONDS = 25 * 60
AUTO_INCREMENT_TABLES = ("evaluation_admission_locks", "participant_admission_locks")
OBSERVATION_SEEDS = (
    "duplicate_command",
    "duplicate_ack",
    "payload_fetch_unavailable",
    "payload_fetch_reference_mismatch",
    "payload_fetch_workload_denied",
    "payload_serve_reference_mismatch",
    "payload_serve_workload_denied",
    "payload_serve_storage_unavailable",
)

RENAMES = {
    "clarifications": "interpretation_clarifications",
    "evidence_sets": "interpretation_evidence_sets",
    "idempotency_requests": "interpretation_idempotency_requests",
    "result_outbox": "interpretation_result_outbox",
    "model_calls": "execution_model_calls",
    "participant_retries": "execution_participant_retries",
    "prompt_draft_freezes": "governance_prompt_draft_freezes",
    "profile_registrations": "governance_profile_registrations",
    "solution_revisions": "governance_solutions",
    "solution_commands": "governance_solution_commands",
    "semantic_draft_commands": "governance_semantic_draft_commands",
    "configuration_publications": "publication_records",
    "configuration_publication_pointers": "publication_pointers",
    "configuration_publication_changes": "publication_changes",
    "evaluation_admission_locks": "quota_evaluation_admission_locks",
    "evaluation_capacity_reservations": "quota_evaluation_capacity_reservations",
    "participant_admission_locks": "quota_participant_admission_locks",
    "participant_capacity_reservations": "quota_participant_capacity_reservations",
    "organization_quota_versions": "quota_organization_versions",
    "organization_quota_pointers": "quota_organization_pointers",
    "organization_quota_commands": "quota_organization_commands",
    "ai_messaging_outbox": "messaging_outbox",
    "ai_messaging_inbox": "messaging_inbox",
    "ai_messaging_quarantine": "messaging_quarantine",
    "ai_messaging_evaluation_sequences": "messaging_evaluation_sequences",
    "ai_messaging_observations": "messaging_observations",
    "runtime_milestones": "operations_runtime_milestones",
}
ASSETS = {
    "profile_assets": ("profile", "profile_id", "version", "definition_json"),
    "prompt_assets": ("prompt", "template_id", "version", "package_json"),
    "route_assets": ("route", "route", "revision", "definition_json"),
    "schema_assets": ("schema", "schema_id", "version", "definition_json"),
    "evaluation_policy_assets": (None, "asset_id", "version", "definition_json"),
    "semantic_prompt_assets": ("semantic_prompt", "asset_id", "version", "markdown"),
}
MERGES = {
    **dict.fromkeys(ASSETS, "governance_asset_versions"),
    "prompt_drafts": "governance_draft_heads",
    "semantic_draft_heads": "governance_draft_heads",
    "prompt_draft_revisions": "governance_draft_versions",
    "semantic_draft_versions": "governance_draft_versions",
    "external_requests": "interpretation_sessions",
    "evaluation_run_policies": "evaluation_runs",
    "evaluation_generation_completions": "evaluation_completions",
    "evaluation_semantic_completions": "evaluation_completions",
}


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value):
        raise ValueError("Invalid schema/table identifier")
    return f"`{value}`"


def qualified(schema: str, table: str) -> str:
    return f"{identifier(schema)}.{identifier(table)}"


def physical(old_name: str, head: str) -> str:
    if head == OLD_HEAD:
        return old_name
    if head == NEW_HEAD:
        return MERGES.get(old_name, RENAMES.get(old_name, old_name))
    if head == INTERMEDIATE_HEAD:
        return MERGES.get(old_name, old_name)
    raise ValueError("Unknown storage layout")
