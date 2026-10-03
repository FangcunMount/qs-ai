"""Reject empty, incomplete or skipped required MQ scenario evidence."""

import json
import sys
from pathlib import Path
from xml.etree import ElementTree

required = {
    "test_index_id_is_verified_by_raw_config_and_pinned_for_start",
    "test_index_metadata_match_alone_cannot_authorize_loading",
    "test_every_runtime_config_field_is_part_of_identity",
    "test_platform_and_all_layer_identities_are_checked",
    "test_docker_empty_defaults_do_not_hide_nonempty_config",
    "test_bad_asset_never_loads_or_touches_existing_service",
    "test_retention_protects_actual_loaded_ids_not_export_ids",
    "test_rollback_uses_pinned_actual_id_before_probe_and_restores_state",
    "test_binding_drift_blocks_rollback_before_any_command",
    "test_legacy_mutable_tag_must_still_match_original_id",
    "test_existing_release_binding_and_successful_releases_are_immutable",
    "test_client_reuses_validator_and_refuses_existing_release_directory",
    "test_gzip_layer_blob_retains_uncompressed_diff_id",
    "test_ambiguous_archives_fail_before_loading",
    "test_receipt_publication_is_atomic_and_failed_publish_can_resume",
    "test_release_reference_is_bounded_and_never_shell_code",
    "test_older_successful_release_cannot_be_reapplied_even_outside_current_state",
    "test_dependency_direction_and_no_cycles",
    "test_binding_adds_only_existing_settings_and_individual_readonly_mounts",
    "test_bad_binding_is_rejected_without_disclosing_contents",
    "test_ambiguous_or_oversized_binding_is_rejected",
    "test_changed_release_binding_and_disabled_rollback_are_rejected",
    "test_key_preflight_failure_never_stops_or_replaces_old_service",
    "test_real_key_files_are_validated_without_starting_resources",
    "test_enabled_compose_keeps_one_service_original_tls_and_stop_budget",
    "test_manual_mq_rollback_checks_old_key_files_before_stopping_current",
    "test_messaging_disabled_apply_cannot_replace_confirmed_mq_owner",
    "test_messaging_inventory_is_read_only_and_borrowed_pool_survives",
    "test_messaging_inventory_missing_table_is_unavailable_not_empty",
    "test_messaging_inventory_truncation_never_claims_complete",
    "test_messaging_inventory_keeps_unknown_facts_without_body_or_retry_authorization",
    "test_handoff_manifest_rejects_ambiguous_or_unbounded_input",
    "test_handoff_manifest_rejects_symlink",
    "test_handoff_invalid_review_refused_before_database",
    "test_handoff_database_diagnostics_are_redacted_and_owned_pool_closed",
    "test_handoff_dry_run_is_storage_read_only_and_retains_host_pool",
    "test_reviewed_source_drift_stops_before_transfer",
    "test_apply_preserves_exhausted_budget_and_repeat_never_seals",
    "test_partial_apply_retains_first_commit_and_stops_on_next_drift",
    "test_manifest_digest_and_stopped_attestation_precede_apply",
    "test_ready_relay_does_not_scan_or_transfer_historical_results",
    "test_delivered_original_is_not_revived",
    "test_transfer_is_atomic_and_source_compare_is_exact",
    "test_storage_failure_rolls_back_single_row_transfer",
    "test_bad_source_identity_is_rejected_before_sealing",
    "test_start_refusal_uses_only_original_persisted_event",
    "test_missing_or_mismatched_evidence_is_technical_unknown",
    "test_accepted_start_does_not_consult_current_state",
    "test_unavailable_configuration_keeps_original_refusal_and_zero_dispatch",
    "test_start_refusal_replay_survives_publication_recovery",
    "test_refusal_receipt_failure_rolls_back_original_business_and_inbox",
    "test_mq_startup_schema_transaction_rejects_write_and_preserves_pool",
    "test_mq_duplicate_observations_share_original_commit_and_conflict_is_not_duplicate",
    "test_mq_duplicate_ack_records_only_after_exact_confirmation_and_rolls_back",
    "test_mq_payload_fetch_audit_retains_original_error_and_never_admits",
    "test_mq_payload_serve_audit_preserves_read_rollback_and_fixed_rpc_error",
    "test_mq_audit_storage_failure_never_masks_payload_failure_or_fabricates_counts",
    "test_mq_partial_technical_ledger_is_unavailable_and_kind_is_fixed",
    "test_mq_health_exposes_only_committed_organization_outbox",
    "test_mq_health_disabled_never_reads_messaging_tables",
    "test_mq_health_failure_propagates_without_partial_or_global_counts",
    "test_mq_health_reader_uses_original_di_without_bootstrap_changes",
    "test_runtime_health_counts_are_organization_scoped",
    "test_mq_metrics_use_existing_di_and_never_claim_missing_storage_is_empty",
    "test_mq_snapshot_distinguishes_due_confirmation_hold_and_org",
    "test_mq_snapshot_sees_only_commit_and_never_settles_or_waits_for_writer",
    "test_mq_security_records_are_gauges_without_invented_duplicate_history",
    "test_mq_snapshot_failure_discards_core_and_partial_mq_values",
    "test_start_change_and_duplicate_preserve_first_effect_receipt_and_wire",
    "test_failure_after_real_effect_rolls_back_business_inbox_and_state",
    "test_rejected_cas_rolls_back_local_command_reservation_but_commits_inbox",
    "test_evaluation_start_cancel_replay_and_final_projection",
    "test_participant_retry_retains_frozen_request_and_replay",
    "test_failed_evaluation_event_rolls_back_projection_and_sequence",
    "test_legacy_handoff_preserves_original_identity_and_never_marks_delivered",
    "test_real_nsq_bootstrap_original_admission_and_shutdown",
    "test_full_candidate_worker_commits_monotone_state_events_without_extra_calls",
    "test_physical_failure_never_admits_and_cannot_forge_logical_budget",
    "test_failed_ack_remains_unknown_and_storage_failure_propagates",
    "test_closed_local_savepoint_preserves_root_and_refusal_receipt",
    "test_oversized_body_authorization_and_exact_acknowledgement",
    "test_preprovisioned_failure_topology_with_real_nsq",
    "test_five_execution_writes_cannot_bypass_mq_admission",
    "test_mq_cutover_keeps_workload_authorization_before_mode_error",
    "test_queries_and_governance_preparation_stay_registered",
    "test_legacy_result_settles_only_with_atomic_original_business_ack",
    "test_legacy_handoff_and_ack_share_lock_order_without_deadlock",
}
legacy_cases = (
    {
        f"test_native_handoff_process_kill_preserves_atomic_ownership[{boundary}]"
        for boundary in ("before_commit", "after_commit")
    }
    | {
        f"test_handoff_invalid_review_refused_before_database[{boundary}]"
        for boundary in ("digest", "attestation", "keys", "empty_ids", "duplicate_ids")
    }
    | {
        f"test_handoff_database_diagnostics_are_redacted_and_owned_pool_closed[{boundary}]"
        for boundary in ("read", "close")
    }
    | {
        f"test_single_row_preserves_time_budget_and_reuses_first_wire[{unknown}-{attempts}]"
        for unknown in ("False", "True")
        for attempts in (0, 3, 8, 10)
    }
    | {
        f"test_mismatched_first_wire_or_body_never_resealed_or_repaired[{damage}]"
        for damage in ("wire", "wire_digest", "wire_identity", "body", "aggregate", "owned_missing")
    }
    | {
        f"test_unowned_first_wire_is_reused_only_with_matching_delivery_metadata[{change}]"
        for change in ("None", "attempts", "available_at", "exhausted_stage")
    }
)
# Retirement must refuse all five writes with either legacy option value and
# retain every query/governance method. Require exact cases rather than the old
# MQ-enabled-only count; no missing or skipped case can satisfy this gate.
write_methods = (
    "Commands.Start",
    "Commands.Change",
    "ParticipantManagement.Retry",
    "EvaluationManagement.Start",
    "EvaluationManagement.Cancel",
)
retained_methods = (
    "Commands.CheckEligibility",
    "ParticipantManagement.GetExecution",
    "ParticipantManagement.GetRetryReceipt",
    "EvaluationManagement.Get",
    "EvaluationManagement.Create",
    "EvaluationManagement.Prepare",
    "SolutionManagement.Get",
    "SolutionManagement.GetModels",
    "SolutionManagement.Prepare",
)
cutover_cases = (
    {
        f"test_five_execution_writes_cannot_bypass_mq_admission[{method}-{enabled}]"
        for method in write_methods
        for enabled in ("False", "True")
    }
    | {
        f"test_mq_cutover_keeps_workload_authorization_before_mode_error[{method}]"
        for method in write_methods
    }
    | {
        f"test_queries_and_governance_preparation_stay_registered[{method}-{enabled}]"
        for method in retained_methods
        for enabled in ("False", "True")
    }
)
root = ElementTree.parse(Path(sys.argv[1])).getroot()
cases = root.findall(".//testcase")
case_ids = [(c.get("classname", ""), c.get("name", "")) for c in cases]
if (
    not cases
    or len(case_ids) != len(set(case_ids))
    or any(c.find(tag) is not None for c in cases for tag in ("failure", "error", "skipped"))
    or not required.issubset({c.get("name", "").split("[")[0] for c in cases})
    or not legacy_cases.issubset({c.get("name", "") for c in cases})
    or not all(
        root.find(f".//testcase[@name='test_go_python_messaging_interop[{kind}]']") is not None
        for kind in range(1, 10)
    )
    or {c.get("name", "") for c in cases if c.get("classname", "").endswith("test_grpc_mq_cutover")}
    != cutover_cases
    or not all(
        root.find(
            ".//testcase[@name='test_legacy_result_settles_only_with_atomic_original_business_ack"
            f"[{scenario}]']"
        )
        is not None
        for scenario in (
            "stored",
            "wrong_hash",
            "held",
            "rollback",
            "source_conflict",
            "unowned",
            "storage_error",
        )
    )
):
    raise SystemExit("Required MQ report is empty, incomplete, failed or skipped")
print(json.dumps({"passed": len(cases), "failed": 0, "errors": 0, "skipped": 0}))
